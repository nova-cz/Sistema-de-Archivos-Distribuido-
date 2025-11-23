import json
import time
import os
import threading
from config import LOG_FILE

class OperationLog:

    def __init__(self):
        self.log_file = LOG_FILE
        self.lock = threading.Lock()
        self.operations = []
        self.load_log()
    
    def load_log(self):
        """Carga el registro de operaciones desde el archivo"""
        if os.path.exists(self.log_file):
            try:
                with open(self.log_file, 'r') as f:
                    self.operations = json.load(f)
            except json.JSONDecodeError:
                # Si el archivo está corrupto, lo reiniciamos
                self.operations = []
        else:
            self.operations = []
    
    def save_log(self):
        """Guarda el registro de operaciones en el archivo"""
        with open(self.log_file, 'w') as f:
            json.dump(self.operations, f, indent=2)

    def add_operation(self, operation_type, source_node, target_node=None, filename=None, timestamp=None):
        """Agrega una nueva operación al registro"""
        if timestamp is None:
            timestamp = time.time()
        
        operation = {
            "type": operation_type,  # "transfer" o "delete"
            "source_node": source_node,
            "timestamp": timestamp,
            "operation_id": f"{source_node}_{timestamp}"
        }
        
        if target_node:
            operation["target_node"] = target_node
        
        if filename:
            operation["filename"] = filename
        
        with self.lock:
            self.operations.append(operation)
            self.save_log()
        
        return operation
    
    def get_operations_since(self, timestamp):
        """Obtiene todas las operaciones desde un timestamp dado"""
        with self.lock:
            return [op for op in self.operations if op["timestamp"] > timestamp]
    
    def get_last_timestamp(self):
        """Obtiene el timestamp de la última operación"""
        with self.lock:
            if not self.operations:
                return 0
            return max(op["timestamp"] for op in self.operations)
    
    def operation_exists(self, operation_id):
        """Verifica si una operación ya existe en el registro"""
        with self.lock:
            return any(op["operation_id"] == operation_id for op in self.operations)