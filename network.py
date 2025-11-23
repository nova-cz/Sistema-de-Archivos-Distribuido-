import socket
import threading
import json
import time
import base64
import copy
import struct
import logging
import atexit
from config import NODES, NODE_NAME, NETWORK_PORT, HEARTBEAT_INTERVAL, NODE_TIMEOUT

logger = logging.getLogger('sistema.network')

class NetworkManager:

    def __init__(self, file_manager, operation_log, sync_manager):
        self.nodes = NODES
        self.node_name = NODE_NAME
        self.port = NETWORK_PORT
        self.file_manager = file_manager
        self.operation_log = operation_log
        self.sync_manager = sync_manager
        self.pending_operations = None  # Se establecerá después
        
        # Estado de los nodos
        self.node_status = {node: {"alive": True, "last_seen": time.time()} 
                            for node in self.nodes if node != self.node_name}
        
        # Lock para acceso seguro al estado de los nodos
        self.status_lock = threading.Lock()
        
        # Iniciar servidor y mecanismos de heartbeat
        self.server_socket = None
        self.running = True
        self.active_connections = set()
        
        # Iniciar threads de servidor y heartbeat
        self.server_thread = threading.Thread(target=self._start_server)
        self.heartbeat_thread = threading.Thread(target=self._send_heartbeats)
        self.status_thread = threading.Thread(target=self._check_nodes_status)
        
        self.server_thread.daemon = True
        self.heartbeat_thread.daemon = True
        self.status_thread.daemon = True
        
        # Registrar limpieza al cerrar
        atexit.register(self.stop)
        
        logger.info(f"Inicializando NetworkManager para nodo {self.node_name}")
        logger.info(f"Puerto de red: {self.port}")
        logger.info(f"Nodos configurados: {list(self.nodes.keys())}")
    
    def set_pending_operations(self, pending_operations):
        self.pending_operations = pending_operations
    
    def start(self):
        """Inicia los threads de red"""
        logger.info("Iniciando threads de red...")
        self.server_thread.start()
        self.heartbeat_thread.start()
        self.status_thread.start()
        logger.info("Threads de red iniciados")
    
    def _start_server(self):
        """Inicia el servidor para escuchar mensajes de otros nodos"""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            
            max_attempts = 5
            current_port = self.port
            success = False
            
            for attempt in range(max_attempts):
                try:
                    logger.info(f"Intentando iniciar servidor en puerto {current_port}")
                    self.server_socket.bind(('0.0.0.0', current_port))
                    self.server_socket.listen(10)
                    success = True
                    break
                except OSError as e:
                    if e.errno == 48:  # Address already in use
                        logger.warning(f"Puerto {current_port} en uso. Intentando con {current_port+1}")
                        current_port += 1
                        if attempt == max_attempts - 1:
                            raise
                        # Crear un nuevo socket para el siguiente intento
                        self.server_socket.close()
                        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    else:
                        raise
            
            if not success:
                logger.error(f"No se pudo iniciar el servidor después de {max_attempts} intentos")
                return
            
            self.port = current_port  # Actualizar el puerto si cambió
            logger.info(f"Servidor iniciado en el puerto {self.port}")
            
            while self.running:
                try:
                    client_socket, address = self.server_socket.accept()
                    logger.debug(f"Conexión aceptada de {address}")
                    client_thread = threading.Thread(target=self._handle_client, args=(client_socket, address))
                    client_thread.daemon = True
                    client_thread.start()
                except Exception as e:
                    if self.running:
                        logger.error(f"Error al aceptar conexión: {e}")
        except Exception as e:
            logger.error(f"Error al iniciar servidor: {e}")
            raise
    
    def _cleanup_connection(self, sock):
        """Limpia una conexión de socket"""
        try:
            if sock in self.active_connections:
                self.active_connections.remove(sock)
            sock.shutdown(socket.SHUT_RDWR)
        except:
            pass
        finally:
            sock.close()
    
    def _send_message(self, node, message):
        """Envía un mensaje a otro nodo"""
        client_socket = None
        try:
            if node == self.node_name:
                logger.debug("Ignorando envío de mensaje a nosotros mismos")
                return True
            
            ip = self.nodes[node]["ip"]
            port = NETWORK_PORT
            
            logger.debug(f"Conectando a {node} ({ip}:{port})")
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            client_socket.settimeout(5)  # Timeout de 5 segundos
            client_socket.connect((ip, port))
            
            # Serializar el mensaje
            message_data = json.dumps(message).encode('utf-8')
            
            # Enviar longitud del mensaje primero (4 bytes)
            message_length = len(message_data)
            client_socket.sendall(struct.pack('!I', message_length))
            
            # Enviar el mensaje
            client_socket.sendall(message_data)
            
            # Recibir respuesta
            response_length = struct.unpack('!I', client_socket.recv(4))[0]
            response_data = client_socket.recv(response_length)
            response = json.loads(response_data.decode('utf-8'))
            
            logger.debug(f"Respuesta recibida de {node}: {response}")
            
            # Actualizar estado del nodo
            with self.status_lock:
                self.node_status[node]["alive"] = True
                self.node_status[node]["last_seen"] = time.time()
            
            return response
        except socket.timeout:
            logger.error(f"Timeout al conectar con {node}")
            with self.status_lock:
                self.node_status[node]["alive"] = False
            return None
        except ConnectionRefusedError:
            logger.error(f"Conexión rechazada por {node}")
            with self.status_lock:
                self.node_status[node]["alive"] = False
            return None
        except Exception as e:
            logger.error(f"Error al enviar mensaje a {node}: {e}")
            with self.status_lock:
                self.node_status[node]["alive"] = False
            return None
        finally:
            if client_socket:
                self._cleanup_connection(client_socket)
    
    def _handle_client(self, client_socket, address):
        """Maneja una conexión entrante de otro nodo"""
        self.active_connections.add(client_socket)
        try:
            logger.debug(f"Manejando conexión de {address}")
            
            # Recibir longitud del mensaje primero
            length_data = client_socket.recv(4)
            if not length_data:
                logger.warning(f"Conexión cerrada por {address} sin datos")
                return
            
            message_length = struct.unpack('!I', length_data)[0]
            logger.debug(f"Esperando mensaje de {message_length} bytes")
            
            # Recibir el mensaje completo
            chunks = []
            bytes_received = 0
            while bytes_received < message_length:
                chunk = client_socket.recv(min(message_length - bytes_received, 4096))
                if not chunk:
                    break
                chunks.append(chunk)
                bytes_received += len(chunk)
            
            message_data = b''.join(chunks)
            message = json.loads(message_data.decode('utf-8'))
            logger.debug(f"Mensaje recibido de {address}: {message}")
            
            # Procesar mensaje
            response = self._process_message(message)
            logger.debug(f"Enviando respuesta a {address}: {response}")
            
            # Enviar respuesta
            response_data = json.dumps(response).encode('utf-8')
            response_length = len(response_data)
            client_socket.sendall(struct.pack('!I', response_length))
            client_socket.sendall(response_data)
            
        except Exception as e:
            logger.error(f"Error al manejar cliente {address}: {e}")
        finally:
            self._cleanup_connection(client_socket)
    
    def _process_message(self, message):
        """Procesa un mensaje recibido de otro nodo"""
        message_type = message.get("type")
        source_node = message.get("source_node")
        
        logger.debug(f"Procesando mensaje tipo {message_type} de {source_node}")
        
        # Actualizar estado del nodo
        if source_node and source_node in self.node_status:
            with self.status_lock:
                self.node_status[source_node]["alive"] = True
                self.node_status[source_node]["last_seen"] = time.time()
                logger.debug(f"Estado actualizado para nodo {source_node}")
        
        if message_type == "heartbeat":
            return {"status": "ok"}
        
        elif message_type == "transfer_file":
            filename = message.get("filename")
            file_data = message.get("file_data")
            
            logger.info(f"Recibiendo archivo {filename} de {source_node}")
            if self.file_manager.save_file(filename, file_data):
                # Registrar operación en el log
                self.operation_log.add_operation(
                    "transfer_file",
                    source_node,
                    target_node=self.node_name,
                    filename=filename
                )
                logger.info(f"Archivo {filename} guardado exitosamente")
                
                return {"status": "ok"}
            else:
                logger.error(f"Error al guardar archivo {filename}")
                return {"status": "error", "message": "Error al guardar archivo"}
            
        elif message_type == "transfer_folder":
            folder_name = message.get("folder_name")
            folder_data = message.get("folder_data")

            logger.info(f"Recibiendo carpeta {folder_name} de {source_node}")
            if self.file_manager.save_folder(folder_data):
                # Registrar operación en el log
                self.operation_log.add_operation(
                    "transfer_folder",
                    source_node,
                    target_node=self.node_name,
                    filename=folder_name
                )
                logger.info(f"Carpeta {folder_name} guardada exitosamente")
                
                return {"status": "ok"}
            else:
                logger.error(f"Error al crear carpeta {folder_name}")
                return {"status": "error", "message": "Error al crear archivo"}
        
        elif message_type == "view_file":
            filename = message.get("filename")
            
            logger.info(f"Recibiendo solicitud para ver archivo {filename} de {source_node}")
            file_type, content, error_or_mime = self.file_manager.get_file_content_for_view(filename)
            
            if error_or_mime and file_type is None:
                logger.error(f"Error al obtener contenido del archivo {filename}: {error_or_mime}")
                return {"status": "error", "message": error_or_mime}
            
            logger.info(f"Enviando contenido del archivo {filename} a {source_node}")
            return {
                "status": "ok",
                "file_type": file_type,
                "content": content,
                "mime_type": error_or_mime if file_type == 'image' else None,
                "filename": filename
            }
        
        elif message_type == "get_pending_operations":
            pending_operations = self.pending_operations.get_pending_operations(source_node)
            logger.info(f"Enviando operaciones pendientes a {source_node}")
            return {"status": "ok", "pending_operations": pending_operations}
        
        elif message_type == "get_all_pendings":
            pending_operations = self.pending_operations.get_all_pendings()
            logger.info(f"Enviando operaciones pendientes a {source_node}")
            return {"status": "ok", "pending_operations": pending_operations}
        
        elif message_type == "list_files":
            files = self.file_manager.list_files(None if "folder_name" not in message else message.get("folder_name"))
            logger.debug(f"Enviando lista de {len(files)} archivos a {source_node}")
            return {"status": "ok", "files": files}
        
        else:
            logger.warning(f"Tipo de mensaje desconocido: {message_type}")
            return {"status": "error", "message": "Tipo de mensaje desconocido"}
    
    def _send_heartbeats(self):
        """Envía mensajes de heartbeat periódicamente a todos los nodos"""
        while self.running:
            for node in self.nodes:
                if node != self.node_name:
                    message = {
                        "type": "heartbeat",
                        "source_node": self.node_name,
                        "timestamp": time.time()
                    }
                    
                    logger.debug(f"Enviando heartbeat a {node}")
                    threading.Thread(target=self._send_message, args=(node, message)).start()
            
            time.sleep(HEARTBEAT_INTERVAL)
    
    def _check_nodes_status(self):
        """Verifica el estado de los nodos periódicamente"""
        while self.running:
            current_time = time.time()
            
            with self.status_lock:
                for node, status in self.node_status.items():
                    if status["alive"] and current_time - status["last_seen"] > NODE_TIMEOUT:
                        status["alive"] = False
                        logger.warning(f"Nodo {node} ha dejado de responder")
            
            time.sleep(HEARTBEAT_INTERVAL)
    
    def send_file(self, filename, target_node, file_data=None):
        """Envía un archivo a otro nodo"""
        logger.info(f"Preparando envío de archivo {filename} a {target_node}")
        
        if not file_data:
            file_data = self.file_manager.get_file_data(filename)
        
        timestamp = time.time()
        message = {
            "type": "transfer_file",
            "source_node": self.node_name,
            "filename": filename,
            "file_data": file_data,
            "timestamp": timestamp
        }
        
        logger.info(f"Enviando archivo {filename} a {target_node}")
        success = self._send_message(target_node, message)
        
        if success:
            logger.info(f"Archivo {filename} enviado exitosamente a {target_node}")
            # Guardar operación en el log
            self.operation_log.add_operation(
                "transfer_file",
                self.node_name,
                target_node=target_node,
                filename=filename
            )
        else:
            logger.error(f"Error al enviar archivo {filename} a {target_node}")
            self.pending_operations.add_operation(
                "transfer_file",
                self.node_name,
                target_node=target_node,
                filename=filename
            )
        
        return success
    
    def send_folder(self, folder_name, target_node, folder_data=None):
        """Envía una carpeta completa a otro nodo"""
        logger.info(f"Preparando envío de carpeta {folder_name} a {target_node}")
        
        if folder_data is None:
            logger.info(f"Obteniendo datos de la carpeta {folder_name}")
            folder_data = self.file_manager.get_folder_data(folder_name)
            if folder_data is None:
                logger.error(f"No se pudo obtener datos de la carpeta {folder_name}")
                return False
            logger.info(f"Datos de la carpeta {folder_name} obtenidos correctamente ({len(folder_data['files'])} archivos)")
        
        timestamp = time.time()
        message = {
            "type": "transfer_folder",
            "source_node": self.node_name,
            "folder_name": folder_name,
            "folder_data": folder_data,
            "timestamp": timestamp
        }
        
        logger.info(f"Enviando carpeta {folder_name} a {target_node}")
        response = self._send_message(target_node, message)
        print(response)
        success = response and response.get("status") == "ok"
        print(success)
        
        if success:
            logger.info(f"Carpeta {folder_name} enviada exitosamente a {target_node}")
            # Guardar operación en el log
            self.operation_log.add_operation(
                "transfer_folder",
                self.node_name,
                target_node=target_node,
                filename=folder_name
            )
        else:
            logger.error(f"Error al enviar carpeta {folder_name} a {target_node}. Respuesta: {response}")
            self.pending_operations.add_operation(
                "transfer_folder",
                self.node_name,
                target_node=target_node,
                filename=folder_name
            )
        
        return success
    
    def delete_file(self, filename):
        """Elimina un archivo localmente y notifica a otros nodos"""
        logger.info(f"Iniciando eliminación del archivo {filename}")
        
        # Primero eliminar localmente
        if not self.file_manager.delete_file(filename):
            logger.error(f"Error al eliminar archivo {filename} localmente")
            #return False
        
        # Registrar operación en el log
        self.operation_log.add_operation(
            "delete",
            self.node_name,
            filename=filename
        )
        logger.info(f"Archivo {filename} eliminado exitosamente")
        
        # Notificar a otros nodos
        timestamp = time.time()
        
        for node in self.nodes:
            if node != self.node_name:
                logger.info(f"Propagando eliminación de {filename} a {node}")
                self.pending_operations.add_operation(
                    "delete",
                    node,
                    filename=filename
                )
        
        return True
    
    def get_node_status(self):
        """Obtiene el estado de conexión de todos los nodos"""
        with self.status_lock:
            status = {node: info["alive"] for node, info in self.node_status.items()}
            status[self.node_name] = True  # Este nodo siempre está activo
            return status
    
    def stop(self):
        """Detiene todos los servicios de red"""
        logger.info("Deteniendo NetworkManager...")
        self.running = False
        
        # Limpiar todas las conexiones activas
        for sock in list(self.active_connections):
            try:
                if sock in self.active_connections:
                    self.active_connections.remove(sock)
                sock.close()
            except:
                pass
        
        if self.server_socket:
            try:
                self.server_socket.close()
            except:
                pass
        
        logger.info("NetworkManager detenido")