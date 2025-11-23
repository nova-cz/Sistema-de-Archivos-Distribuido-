import json
import os
import threading
import time
import copy
from config import PENDING_LOG_FILE

class PendingOperations:

    def __init__(self):
        self.pending_file = PENDING_LOG_FILE
        self.lock = threading.Lock()
        self.pending_operations = []
        self.load_pending()
    
    def load_pending(self):
        """Carga operaciones pendientes desde el archivo"""
        if os.path.exists(self.pending_file):
            try:
                with open(self.pending_file, 'r') as f:
                    self.pending_operations = json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                # Si el archivo está corrupto o no existe, lo reiniciamos
                self.pending_operations = []
        else:
            self.pending_operations = []
    
    def save_pending(self):
        """Guarda operaciones pendientes en el archivo"""
        with open(self.pending_file, 'w') as f:
            json.dump(self.pending_operations, f, indent=2)
    
    def add_operation(self, operation_type, source_node, target_node=None, filename=None, file_data=None):
        """Agrega una operación pendiente para ser procesada cuando el nodo vuelva a conectarse"""
        operation = {
            "type": operation_type,  # "transfer" o "delete"
            "source_node": source_node,
            "timestamp": time.time(),
            "id": f"{operation_type}_{target_node}_{time.time()}"
        }
        
        if filename:
            operation["filename"] = filename
            
        if file_data:
            operation["file_data"] = file_data
            
        if target_node:
            operation["target_node"] = target_node
        
        with self.lock:
            self.pending_operations.append(operation)
            self.save_pending()
        
        return operation
    
    def get_pending_operations(self, target_node):
        """Obtiene operaciones pendientes para un nodo específico o todas"""
        eliminated = [item for item in self.pending_operations if item["source_node"] == target_node]
        self.pending_operations[:] = [item for item in self.pending_operations if item["source_node"] != target_node]
        if len(eliminated) > 0:
            self.save_pending()
        return eliminated
    
    def get_all_pendings(self):
        return copy.deepcopy(self.pending_operations)
    
    def remove_operation(self, operation_id):
        """Elimina una operación pendiente después de procesarla"""
        with self.lock:
            self.pending_operations = [op for op in self.pending_operations if op["id"] != operation_id]
            self.save_pending()