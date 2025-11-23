import threading
import time
import os
import base64
from file_manager import FileManager
from operation_log import OperationLog
from network import NetworkManager
from sync import SyncManager
from pending_operations import PendingOperations
from config import NODE_NAME, SHARED_DIR, NODES

class Node:

    def __init__(self):
        self.node_name = NODE_NAME

        # Inicializar componentes
        self.operation_log = OperationLog()
        self.file_manager = FileManager(self.operation_log)
        self.pending_operations = PendingOperations()
        self.sync_manager = SyncManager(self.file_manager, self.operation_log)
        self.network_manager = NetworkManager(self.file_manager, self.operation_log, self.sync_manager)
        
        # Establecer referencias circulares
        self.sync_manager.set_network_manager(self.network_manager)
        self.sync_manager.set_pending_operations(self.pending_operations)

        self.network_manager.set_pending_operations(self.pending_operations)
        
        # Cache de archivos remotos
        self.remote_files_cache = {}
        self.remote_files_timestamp = {}

        self.transparent_operations = []
        self.temp_files = []
        
        # Iniciar sincronización periódica
        self.sync_thread = threading.Thread(target=self._periodic_sync)
        self.sync_thread.daemon = True

        self.running = True
    
    def start(self):
        """Inicia todos los servicios del nodo"""
        print(f"Iniciando nodo: {self.node_name}")
        
        # Iniciar manager de red
        self.network_manager.start()
        
        # Iniciar sincronización periódica
        self.sync_thread.start()
        
        print(f"Nodo {self.node_name} iniciado correctamente")
    
    def _periodic_sync(self):
        """Realiza sincronización periódica con otros nodos"""
        while self.running:
            try:
                # Esperar un tiempo antes de sincronizar
                time.sleep(3)  # Sincronizar cada 5 segundos
                
                # Iniciar sincronización
                self.sync_manager.start_sync()
                
                # Actualizar caché de archivos remotos
                self._update_remote_files_cache()
            except Exception as e:
                print(f"Error durante la sincronización periódica: {e}")
    
    def _update_remote_files_cache(self):
        """Actualiza la caché de archivos remotos"""

        self.transparent_operations = self.pending_operations.get_all_pendings()
        
        for node in NODES:
            if node != self.node_name:
                # Intentar obtener archivos de todos los nodos, estén conectados o no
                try:
                    files = self.get_remote_files(node)
                    if files is not None:
                        self.remote_files_cache[node] = files
                        self.remote_files_timestamp[node] = time.time()

                    self.transparent_operations.extend(self.get_all_pendings(node))
                except Exception as e:
                    print(f"Error al actualizar caché para {node}: {e}")
                    # Usar caché existente si hay error
                    pass

        self.transparent_operations.sort(key=lambda op: op["timestamp"])
    
    def list_files(self):
        """Lista los archivos en el sistema local"""
        return self.file_manager.list_files()
    
    def transfer_file(self, filename, target_node, source_node, is_dir=False):
        if is_dir:
            success = self.transfer_folder(filename, target_node, source_node)
            return success
        
        if source_node != self.node_name:
            self.pending_operations.add_operation(
                "transfer_file",
                source_node,
                target_node=target_node,
                filename=filename
            )
            return True
        
        success = self.network_manager.send_file(filename, target_node)
        return success
    
    def transfer_folder(self, folder_name, target_node, source_node):
        """Transfiere una carpeta completa entre nodos"""
        if source_node != self.node_name:
            self.pending_operations.add_operation(
                "transfer_folder",
                source_node,
                target_node=target_node,
                filename=folder_name
            )
            return True
        
        success = self.network_manager.send_folder(folder_name, target_node)
        return success
    
    def delete_file(self, filename):
        """Elimina un archivo del sistema local y notifica a otros nodos"""
        success = self.network_manager.delete_file(filename)
        return success
    
    def get_node_status(self):
        """Obtiene el estado de conexión de todos los nodos"""
        status = self.network_manager.get_node_status()
        status[self.node_name] = True  # Este nodo siempre está activo
        return status
    
    def stop(self):
        """Detiene todos los servicios del nodo"""
        self.running = False
        self.network_manager.stop()
        print(f"Nodo {self.node_name} detenido")

    def get_files_list(self, target_node, folder_name=None):
        message = {
            "type": "list_files",
            "source_node": self.node_name,
            "timestamp": time.time()
        }

        if folder_name is not None:
            message["folder_name"] = folder_name
        
        response = self.network_manager._send_message(target_node, message)
        return response
    
    def get_remote_files(self, target_node, folder_name=None):
        """Obtiene la lista de archivos de un nodo remoto"""
        # Verificar si el nodo está en línea
        node_status = self.network_manager.get_node_status()
        
        if node_status.get(target_node, False):
            try:
                response = self.get_files_list(target_node, folder_name)
                
                if isinstance(response, dict) and response.get("status") == "ok":
                    files = response.get("files", [])
                    # Actualizar caché
                    self.remote_files_cache[target_node] = files
                    self.remote_files_timestamp[target_node] = time.time()
                    return files
                # Si hay error, usar caché
                if target_node in self.remote_files_cache:
                    return self.format_files(self.remote_files_cache[target_node], target_node)
                return []
            except Exception as e:
                print(f"Error al obtener archivos remotos: {e}")
                # Si hay error, usar caché
                if target_node in self.remote_files_cache:
                    return self.format_files(self.remote_files_cache[target_node], target_node)
                return []
        else:
            # Si está offline, devolver la caché si existe
            if target_node in self.remote_files_cache:
                return self.format_files(self.remote_files_cache[target_node], target_node)
            return []
        
    def get_all_pendings(self, node):
        message = {
            "type": "get_all_pendings",
            "source_node": self.node_name,
            "timestamp": time.time()
        }

        response = self.network_manager._send_message(node, message)

        if isinstance(response, dict) and response.get("status") == "ok":
            pendings = response.get("pending_operations", [])
            return pendings
        return []
    
    def format_files(self, files, target_node):
        if len(self.transparent_operations) <= 0:
            return files
        for op in self.transparent_operations:
            if op["type"] == "transfer_file":
                if target_node != op["target_node"]:
                    continue
                if not any(item["name"] == op["filename"] for item in files):
                    files.append({
                        "name": op["filename"],
                        "modified": op["timestamp"],
                        "is_dir": False
                    })
            elif op["type"] == "transfer_folder":
                if target_node != op["target_node"]:
                    continue
                if op["source_node"] == self.node_name:
                    new_files = self.file_manager.list_files(op["filename"])
                else:
                    response = self.get_files_list(target_node, op["filename"])
                    if isinstance(response, dict) and response.get("status") == "ok":
                        new_files = response.get("files", [])
                    else:
                        continue
                for f in new_files:
                    if not any(item["name"] == f["name"] for item in files):
                        files.append(f)
            elif op["type"] == "delete":
                for i in range(len(files) - 1, -1, -1):
                    #if files[i]["name"] == op["filename"]:
                    print(files[i]["name"], ",  ", op["filename"])
                    if self.is_in_path(op["filename"], files[i]["name"]):
                        del files[i]
        return files
    
    def is_in_path(self, path_1, path_2):
        path_1 = os.path.abspath(path_1)
        path_2 = os.path.abspath(path_2)
        return os.path.commonpath([path_1]) == os.path.commonpath([path_1, path_2])