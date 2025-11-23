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
        self.pending_operations = None
        self.block_manager = None  # NUEVO: Referencia al block manager
        
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
    
    def set_block_manager(self, block_manager):
        """NUEVO: Establece el block manager"""
        self.block_manager = block_manager
    
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
                    if e.errno == 48 or e.errno == 10048:  # Address already in use (Unix/Windows)
                        logger.warning(f"Puerto {current_port} en uso. Intentando con {current_port+1}")
                        current_port += 1
                        if attempt == max_attempts - 1:
                            raise
                        self.server_socket.close()
                        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    else:
                        raise
            
            if not success:
                logger.error(f"No se pudo iniciar el servidor después de {max_attempts} intentos")
                return
            
            self.port = current_port
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
            client_socket.settimeout(10)  # Timeout aumentado para bloques grandes
            client_socket.connect((ip, port))
            
            # Serializar el mensaje
            message_data = json.dumps(message).encode('utf-8')
            
            # Enviar longitud del mensaje primero (4 bytes)
            message_length = len(message_data)
            client_socket.sendall(struct.pack('!I', message_length))
            
            # Enviar el mensaje
            client_socket.sendall(message_data)
            
            # Recibir respuesta
            response_length_data = client_socket.recv(4)
            if not response_length_data:
                return None
            response_length = struct.unpack('!I', response_length_data)[0]
            
            # Recibir respuesta completa
            chunks = []
            bytes_received = 0
            while bytes_received < response_length:
                chunk = client_socket.recv(min(response_length - bytes_received, 65536))
                if not chunk:
                    break
                chunks.append(chunk)
                bytes_received += len(chunk)
            
            response_data = b''.join(chunks)
            response = json.loads(response_data.decode('utf-8'))
            
            logger.debug(f"Respuesta recibida de {node}")
            
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
                chunk = client_socket.recv(min(message_length - bytes_received, 65536))
                if not chunk:
                    break
                chunks.append(chunk)
                bytes_received += len(chunk)
            
            message_data = b''.join(chunks)
            message = json.loads(message_data.decode('utf-8'))
            logger.debug(f"Mensaje recibido de {address}: tipo={message.get('type')}")
            
            # Procesar mensaje
            response = self._process_message(message)
            logger.debug(f"Enviando respuesta a {address}")
            
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
        
        # ==================== HANDLERS EXISTENTES ====================
        
        if message_type == "heartbeat":
            return {"status": "ok"}
        
        elif message_type == "transfer_file":
            filename = message.get("filename")
            file_data = message.get("file_data")
            
            logger.info(f"Recibiendo archivo {filename} de {source_node}")
            if self.file_manager.save_file(filename, file_data):
                self.operation_log.add_operation(
                    "transfer_file",
                    source_node,
                    target_node=self.node_name,
                    filename=filename
                )
                return {"status": "ok"}
            else:
                return {"status": "error", "message": "Error al guardar archivo"}
            
        elif message_type == "transfer_folder":
            folder_name = message.get("folder_name")
            folder_data = message.get("folder_data")

            logger.info(f"Recibiendo carpeta {folder_name} de {source_node}")
            if self.file_manager.save_folder(folder_data):
                self.operation_log.add_operation(
                    "transfer_folder",
                    source_node,
                    target_node=self.node_name,
                    filename=folder_name
                )
                return {"status": "ok"}
            else:
                return {"status": "error", "message": "Error al crear archivo"}
        
        elif message_type == "view_file":
            filename = message.get("filename")
            file_type, content, error_or_mime = self.file_manager.get_file_content_for_view(filename)
            
            if error_or_mime and file_type is None:
                return {"status": "error", "message": error_or_mime}
            
            return {
                "status": "ok",
                "file_type": file_type,
                "content": content,
                "mime_type": error_or_mime if file_type == 'image' else None,
                "filename": filename
            }
        
        elif message_type == "get_pending_operations":
            pending_operations = self.pending_operations.get_pending_operations(source_node)
            return {"status": "ok", "pending_operations": pending_operations}
        
        elif message_type == "get_all_pendings":
            pending_operations = self.pending_operations.get_all_pendings()
            return {"status": "ok", "pending_operations": pending_operations}
        
        elif message_type == "list_files":
            files = self.file_manager.list_files(None if "folder_name" not in message else message.get("folder_name"))
            return {"status": "ok", "files": files}
        
        # ==================== NUEVOS HANDLERS PARA BLOQUES ====================
        
        elif message_type == "store_block":
            """Almacena un bloque recibido de otro nodo"""
            if not self.block_manager:
                return {"status": "error", "message": "Block manager no disponible"}
            
            block_id = message.get("block_id")
            block_data = message.get("block_data")
            is_replica = message.get("is_replica", False)
            
            logger.info(f"Recibiendo bloque {block_id} (replica={is_replica}) de {source_node}")
            
            if self.block_manager.save_block_locally(block_id, block_data, is_replica):
                return {"status": "ok"}
            else:
                return {"status": "error", "message": "Error al guardar bloque"}
        
        elif message_type == "get_block":
            """Envía un bloque solicitado por otro nodo"""
            if not self.block_manager:
                return {"status": "error", "message": "Block manager no disponible"}
            
            block_id = message.get("block_id")
            logger.info(f"Nodo {source_node} solicita bloque {block_id}")
            
            block_data = self.block_manager.get_block_locally(block_id)
            if block_data:
                return {"status": "ok", "block_data": block_data}
            else:
                return {"status": "error", "message": "Bloque no encontrado"}
        
        elif message_type == "delete_block":
            """Elimina un bloque local"""
            if not self.block_manager:
                return {"status": "error", "message": "Block manager no disponible"}
            
            block_id = message.get("block_id")
            logger.info(f"Eliminando bloque {block_id} por solicitud de {source_node}")
            
            if self.block_manager.delete_block_locally(block_id):
                return {"status": "ok"}
            else:
                return {"status": "error", "message": "Error al eliminar bloque"}
        
        elif message_type == "get_block_table":
            """Envía la tabla de bloques para sincronización"""
            if not self.block_manager:
                return {"status": "error", "message": "Block manager no disponible"}
            
            return {
                "status": "ok",
                "block_table": self.block_manager.get_block_table(),
                "file_index": self.block_manager.get_file_index()
            }
        
        elif message_type == "sync_block_table":
            """Recibe y sincroniza tabla de bloques de otro nodo"""
            if not self.block_manager:
                return {"status": "error", "message": "Block manager no disponible"}
            
            remote_table = message.get("block_table", {})
            remote_index = message.get("file_index", {})
            
            self.block_manager.sync_block_table(remote_table)
            self.block_manager.sync_file_index(remote_index)
            
            return {"status": "ok"}
        
        elif message_type == "get_distributed_files":
            """Retorna lista de archivos distribuidos"""
            if not self.block_manager:
                return {"status": "error", "message": "Block manager no disponible"}
            
            files = self.block_manager.get_all_files()
            return {"status": "ok", "files": files}
        
        elif message_type == "get_system_stats":
            """Retorna estadísticas del sistema"""
            if not self.block_manager:
                return {"status": "error", "message": "Block manager no disponible"}
            
            stats = self.block_manager.get_system_stats()
            return {"status": "ok", "stats": stats}
        
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
        
        success = self._send_message(target_node, message)
        
        if success:
            logger.info(f"Archivo {filename} enviado exitosamente a {target_node}")
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
            folder_data = self.file_manager.get_folder_data(folder_name)
            if folder_data is None:
                return False
        
        message = {
            "type": "transfer_folder",
            "source_node": self.node_name,
            "folder_name": folder_name,
            "folder_data": folder_data,
            "timestamp": time.time()
        }
        
        response = self._send_message(target_node, message)
        success = response and response.get("status") == "ok"
        
        if success:
            self.operation_log.add_operation(
                "transfer_folder",
                self.node_name,
                target_node=target_node,
                filename=folder_name
            )
        else:
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
        
        if not self.file_manager.delete_file(filename):
            pass
        
        self.operation_log.add_operation(
            "delete",
            self.node_name,
            filename=filename
        )
        
        for node in self.nodes:
            if node != self.node_name:
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
            status[self.node_name] = True
            return status
    
    def stop(self):
        """Detiene todos los servicios de red"""
        logger.info("Deteniendo NetworkManager...")
        self.running = False
        
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