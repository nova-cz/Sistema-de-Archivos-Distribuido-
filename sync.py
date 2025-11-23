import threading
import time
import logging
import os
import base64
from config import SHARED_DIR

logger = logging.getLogger('sistema.sync')

class SyncManager:

    def __init__(self, file_manager, operation_log):
        self.file_manager = file_manager
        self.operation_log = operation_log
        self.pending_operations = None  # Se establecerá después
        self.network_manager = None  # Se establecerá después
        self.lock = threading.Lock()
        self.syncing = False
        self.node_name = None
    
    def set_network_manager(self, network_manager):
        self.network_manager = network_manager
        self.node_name = network_manager.node_name
    
    def set_pending_operations(self, pending_operations):
        self.pending_operations = pending_operations
    
    def start_sync(self):
        """Inicia el proceso de sincronización con otros nodos"""
        with self.lock:
            if self.syncing:
                return
            self.syncing = True
        
        try:
            # Solicitar operaciones a todos los nodos activos
            node_status = self.network_manager.get_node_status()

            pending_operations = self.pending_operations.pending_operations
            
            for node, alive in node_status.items():
                # Procesar operaciones pendientes para este nodo
                if node != self.node_name and alive:
                    timestamp = time.time()
                    message = {
                        "type": "get_pending_operations",
                        "source_node": self.node_name,
                        "timestamp": timestamp
                    }
        
                    response = self.network_manager._send_message(node, message)
                    if isinstance(response, dict) and response.get("status") == "ok":
                        new_op = response.get("pending_operations", [])
                        pending_operations.extend(new_op)

            pending_operations.sort(key=lambda op: op["timestamp"])

            self.pending_operations.pending_operations = pending_operations
            self.pending_operations.save_pending()
                
            self._process_pending_operations(pending_operations)
            
        finally:
            with self.lock:
                self.syncing = False
    
    def _process_pending_operations(self, pending_ops):
        """Procesa operaciones pendientes para un nodo específico"""
        if not pending_ops:
            return
            
        logger.info(f"Procesando {len(pending_ops)} operaciones pendientes para nodo {self.node_name}")
        
        for op in pending_ops:
            if op["source_node"] != self.node_name:
                continue

            success = False

            if op["type"] == "transfer_file":

                file_path = os.path.join(SHARED_DIR, op["filename"])
                if not os.path.exists(file_path):
                    success = True
                else:
                    # Leer archivo y codificarlo en base64
                    with open(file_path, 'rb') as f:
                        file_data = base64.b64encode(f.read()).decode('utf-8')

                    target_node = op["target_node"]
                    
                    # Preparar mensaje
                    message = {
                        "type": "transfer_file",
                        "source_node": self.node_name,
                        "filename": op["filename"],
                        "file_data": file_data,
                        "timestamp": time.time()
                    }
                    
                    # Enviar archivo
                    response = self.network_manager._send_message(target_node, message)
                    success = response and response.get("status") == "ok"

            elif op["type"] == "transfer_folder":
                folder_name = op["filename"]
                folder_path = os.path.join(SHARED_DIR, folder_name)
                if not os.path.exists(folder_path):
                    logger.warning(f"La carpeta {folder_name} ya no existe en el origen")
                    success = True
                else:
                    logger.info(f"Obteniendo datos de la carpeta {folder_name}")
                    folder_data = self.file_manager.get_folder_data(folder_name)
                    if folder_data is None:
                        logger.error(f"No se pudo obtener datos de la carpeta {folder_name}")
                        success = False

                    else:
        
                        target_node = op["target_node"]
                        timestamp = time.time()
                        message = {
                            "type": "transfer_folder",
                            "source_node": self.node_name,
                            "folder_name": folder_name,
                            "folder_data": folder_data,
                            "timestamp": timestamp
                        }
                        
                        logger.info(f"Enviando carpeta {folder_name} a {target_node}")
                        response = self.network_manager._send_message(target_node, message)
                        success = response and response.get("status") == "ok"
                
            elif op["type"] == "delete":

                success = self.file_manager.delete_file(op["filename"])
            
            if success:
                logger.info(f"Operación pendiente procesada con éxito: {op}")
                self.pending_operations.remove_operation(op["id"])
            else:
                logger.warning(f"No se pudo procesar operación pendiente: {op}")