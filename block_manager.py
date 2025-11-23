"""
block_manager.py - Sistema de gestión de bloques para el sistema de archivos distribuido

Este módulo implementa:
1. División de archivos en bloques de 1 MB
2. Tabla de bloques (similar a tabla de páginas)
3. Replicación automática de bloques
4. Reconstrucción de archivos desde bloques
"""

import os
import json
import threading
import time
import hashlib
import base64
from config import SHARED_DIR, NODES, NODE_NAME, BLOCK_SIZE, NODE_CAPACITY

# Archivo donde se guarda la tabla de bloques global
BLOCK_TABLE_FILE = os.path.join(SHARED_DIR, "block_table.json")
# Archivo donde se guarda el índice de archivos
FILE_INDEX_FILE = os.path.join(SHARED_DIR, "file_index.json")
# Directorio donde se guardan los bloques
BLOCKS_DIR = os.path.join(SHARED_DIR, "blocks")


class BlockManager:
    """
    Gestor de bloques del sistema de archivos distribuido.
    
    Responsabilidades:
    - Dividir archivos en bloques de 1 MB
    - Mantener la tabla de bloques (qué bloque está dónde)
    - Gestionar la replicación de bloques
    - Reconstruir archivos desde sus bloques
    """
    
    def __init__(self, network_manager=None):
        self.block_size = BLOCK_SIZE  # 1 MB por defecto
        self.node_name = NODE_NAME
        self.node_capacity = NODE_CAPACITY  # Capacidad en MB de este nodo
        self.network_manager = network_manager
        self.lock = threading.Lock()
        
        # Crear directorio de bloques si no existe
        os.makedirs(BLOCKS_DIR, exist_ok=True)
        
        # Cargar tabla de bloques e índice de archivos
        self.block_table = self._load_block_table()
        self.file_index = self._load_file_index()
    
    def set_network_manager(self, network_manager):
        """Establece el network manager para comunicación entre nodos"""
        self.network_manager = network_manager
    
    # ==================== TABLA DE BLOQUES ====================
    
    def _load_block_table(self):
        """
        Carga la tabla de bloques desde el archivo.
        
        La tabla de bloques es un diccionario donde:
        - Clave: ID del bloque (ej: "archivo1_block_0")
        - Valor: Información del bloque (nodo, réplica, estado, etc.)
        """
        if os.path.exists(BLOCK_TABLE_FILE):
            try:
                with open(BLOCK_TABLE_FILE, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                return {"blocks": {}, "node_usage": {node: 0 for node in NODES}}
        return {"blocks": {}, "node_usage": {node: 0 for node in NODES}}
    
    def _save_block_table(self):
        """Guarda la tabla de bloques en el archivo"""
        with open(BLOCK_TABLE_FILE, 'w') as f:
            json.dump(self.block_table, f, indent=2)
    
    def _load_file_index(self):
        """
        Carga el índice de archivos.
        
        El índice mapea nombre de archivo -> lista de bloques que lo componen
        """
        if os.path.exists(FILE_INDEX_FILE):
            try:
                with open(FILE_INDEX_FILE, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                return {}
        return {}
    
    def _save_file_index(self):
        """Guarda el índice de archivos"""
        with open(FILE_INDEX_FILE, 'w') as f:
            json.dump(self.file_index, f, indent=2)
    
    def get_block_table(self):
        """Retorna la tabla de bloques completa"""
        with self.lock:
            return self.block_table.copy()
    
    def get_file_index(self):
        """Retorna el índice de archivos"""
        with self.lock:
            return self.file_index.copy()
    
    # ==================== DIVISIÓN EN BLOQUES ====================
    
    def split_file_into_blocks(self, file_path, original_filename):
        """
        Divide un archivo en bloques de 1 MB.
        
        Args:
            file_path: Ruta al archivo a dividir
            original_filename: Nombre original del archivo
            
        Returns:
            Lista de diccionarios con información de cada bloque
        """
        blocks = []
        file_size = os.path.getsize(file_path)
        total_blocks = (file_size + self.block_size - 1) // self.block_size  # Redondeo hacia arriba
        
        if total_blocks == 0:
            total_blocks = 1  # Archivo vacío = 1 bloque vacío
        
        # Generar ID único para el archivo
        file_id = self._generate_file_id(original_filename)
        
        with open(file_path, 'rb') as f:
            for block_num in range(total_blocks):
                # Leer bloque de 1 MB
                block_data = f.read(self.block_size)
                
                # Generar ID del bloque
                block_id = f"{file_id}_block_{block_num}"
                
                # Calcular hash para verificar integridad
                block_hash = hashlib.md5(block_data).hexdigest()
                
                blocks.append({
                    "block_id": block_id,
                    "block_num": block_num,
                    "file_id": file_id,
                    "original_filename": original_filename,
                    "size": len(block_data),
                    "hash": block_hash,
                    "data": base64.b64encode(block_data).decode('utf-8')
                })
        
        return blocks, file_id
    
    def _generate_file_id(self, filename):
        """Genera un ID único para un archivo basado en nombre y timestamp"""
        timestamp = str(time.time())
        unique_str = f"{filename}_{timestamp}_{self.node_name}"
        return hashlib.md5(unique_str.encode()).hexdigest()[:12]
    
    # ==================== DISTRIBUCIÓN Y REPLICACIÓN ====================
    
    def get_available_nodes(self, exclude_node=None):
        """
        Obtiene lista de nodos disponibles para almacenar bloques.
        
        Args:
            exclude_node: Nodo a excluir de la lista (para réplicas)
            
        Returns:
            Lista de nodos ordenados por espacio disponible
        """
        available = []
        
        for node in NODES:
            if node == exclude_node:
                continue
            
            # Calcular espacio usado en este nodo
            used = self.block_table.get("node_usage", {}).get(node, 0)
            capacity = NODE_CAPACITY.get(node, 50)  # 50 MB por defecto
            free_space = capacity - used
            
            if free_space > 0:
                available.append({
                    "node": node,
                    "free_space": free_space,
                    "used": used,
                    "capacity": capacity
                })
        
        # Ordenar por espacio libre (mayor primero) para balancear carga
        available.sort(key=lambda x: x["free_space"], reverse=True)
        
        return available
    
    def allocate_blocks(self, blocks, original_filename):
        """
        Asigna nodos para cada bloque y su réplica.
        
        Estrategia de distribución:
        - Distribuir bloques entre nodos de forma balanceada
        - La réplica SIEMPRE va en un nodo diferente al original
        
        Args:
            blocks: Lista de bloques a distribuir
            original_filename: Nombre del archivo original
            
        Returns:
            Lista de bloques con asignación de nodos
        """
        with self.lock:
            allocated_blocks = []
            nodes_list = list(NODES.keys())
            
            for i, block in enumerate(blocks):
                # Obtener nodos disponibles
                available = self.get_available_nodes()
                
                if len(available) < 2:
                    raise Exception("No hay suficientes nodos disponibles para replicación")
                
                # Seleccionar nodo primario (round-robin con balanceo)
                primary_node = available[0]["node"]
                
                # Seleccionar nodo para réplica (diferente al primario)
                replica_candidates = self.get_available_nodes(exclude_node=primary_node)
                if not replica_candidates:
                    raise Exception("No hay nodo disponible para réplica")
                replica_node = replica_candidates[0]["node"]
                
                # Actualizar información del bloque
                block["primary_node"] = primary_node
                block["replica_node"] = replica_node
                block["status"] = "allocated"
                block["created_at"] = time.time()
                
                # Actualizar uso de nodos en la tabla
                if "node_usage" not in self.block_table:
                    self.block_table["node_usage"] = {node: 0 for node in NODES}
                
                # Cada bloque ocupa 1 MB (redondeado hacia arriba)
                block_mb = max(1, (block["size"] + self.block_size - 1) // self.block_size)
                self.block_table["node_usage"][primary_node] = self.block_table["node_usage"].get(primary_node, 0) + block_mb
                self.block_table["node_usage"][replica_node] = self.block_table["node_usage"].get(replica_node, 0) + block_mb
                
                # Guardar en tabla de bloques
                self.block_table["blocks"][block["block_id"]] = {
                    "block_id": block["block_id"],
                    "block_num": block["block_num"],
                    "file_id": block["file_id"],
                    "original_filename": original_filename,
                    "size": block["size"],
                    "hash": block["hash"],
                    "primary_node": primary_node,
                    "replica_node": replica_node,
                    "status": "allocated",
                    "created_at": block["created_at"]
                }
                
                allocated_blocks.append(block)
            
            # Guardar tabla actualizada
            self._save_block_table()
            
            return allocated_blocks
    
    # ==================== ALMACENAMIENTO DE BLOQUES ====================
    
    def save_block_locally(self, block_id, block_data, is_replica=False):
        """
        Guarda un bloque en el almacenamiento local.
        
        Args:
            block_id: ID del bloque
            block_data: Datos del bloque en base64
            is_replica: Si es True, es una réplica
            
        Returns:
            True si se guardó correctamente
        """
        try:
            # Crear subdirectorio para réplicas si es necesario
            if is_replica:
                block_dir = os.path.join(BLOCKS_DIR, "replicas")
            else:
                block_dir = os.path.join(BLOCKS_DIR, "primary")
            
            os.makedirs(block_dir, exist_ok=True)
            
            # Guardar bloque
            block_path = os.path.join(block_dir, f"{block_id}.bin")
            
            # Decodificar de base64
            if isinstance(block_data, str):
                data = base64.b64decode(block_data)
            else:
                data = block_data
            
            with open(block_path, 'wb') as f:
                f.write(data)
            
            return True
        except Exception as e:
            print(f"Error al guardar bloque {block_id}: {e}")
            return False
    
    def get_block_locally(self, block_id, check_replica=True):
        """
        Obtiene un bloque del almacenamiento local.
        
        Args:
            block_id: ID del bloque
            check_replica: Si buscar también en réplicas
            
        Returns:
            Datos del bloque en base64 o None si no existe
        """
        # Primero buscar en bloques primarios
        primary_path = os.path.join(BLOCKS_DIR, "primary", f"{block_id}.bin")
        if os.path.exists(primary_path):
            with open(primary_path, 'rb') as f:
                return base64.b64encode(f.read()).decode('utf-8')
        
        # Si no está, buscar en réplicas
        if check_replica:
            replica_path = os.path.join(BLOCKS_DIR, "replicas", f"{block_id}.bin")
            if os.path.exists(replica_path):
                with open(replica_path, 'rb') as f:
                    return base64.b64encode(f.read()).decode('utf-8')
        
        return None
    
    def delete_block_locally(self, block_id):
        """Elimina un bloque del almacenamiento local"""
        deleted = False
        
        # Eliminar de primarios
        primary_path = os.path.join(BLOCKS_DIR, "primary", f"{block_id}.bin")
        if os.path.exists(primary_path):
            os.remove(primary_path)
            deleted = True
        
        # Eliminar de réplicas
        replica_path = os.path.join(BLOCKS_DIR, "replicas", f"{block_id}.bin")
        if os.path.exists(replica_path):
            os.remove(replica_path)
            deleted = True
        
        return deleted
    
    # ==================== DISTRIBUCIÓN A NODOS ====================
    
    def distribute_blocks(self, allocated_blocks, file_id, original_filename):
        """
        Distribuye los bloques a sus nodos asignados.
        
        Args:
            allocated_blocks: Lista de bloques con nodos asignados
            file_id: ID del archivo
            original_filename: Nombre original
            
        Returns:
            True si todos los bloques se distribuyeron correctamente
        """
        success = True
        block_ids = []
        
        for block in allocated_blocks:
            block_id = block["block_id"]
            block_ids.append(block_id)
            primary_node = block["primary_node"]
            replica_node = block["replica_node"]
            block_data = block["data"]
            
            # Guardar en nodo primario
            if primary_node == self.node_name:
                # Guardar localmente
                if not self.save_block_locally(block_id, block_data, is_replica=False):
                    success = False
            else:
                # Enviar a nodo remoto
                if not self._send_block_to_node(block_id, block_data, primary_node, is_replica=False):
                    success = False
            
            # Guardar réplica
            if replica_node == self.node_name:
                # Guardar localmente como réplica
                if not self.save_block_locally(block_id, block_data, is_replica=True):
                    success = False
            else:
                # Enviar réplica a nodo remoto
                if not self._send_block_to_node(block_id, block_data, replica_node, is_replica=True):
                    success = False
        
        # Actualizar índice de archivos
        with self.lock:
            self.file_index[file_id] = {
                "original_filename": original_filename,
                "block_ids": block_ids,
                "total_blocks": len(block_ids),
                "created_at": time.time(),
                "size": sum(b["size"] for b in allocated_blocks)
            }
            self._save_file_index()
        
        return success
    
    def _send_block_to_node(self, block_id, block_data, target_node, is_replica=False):
        """Envía un bloque a otro nodo"""
        if not self.network_manager:
            print(f"Error: No hay network manager configurado")
            return False
        
        message = {
            "type": "store_block",
            "source_node": self.node_name,
            "block_id": block_id,
            "block_data": block_data,
            "is_replica": is_replica,
            "timestamp": time.time()
        }
        
        response = self.network_manager._send_message(target_node, message)
        return response and response.get("status") == "ok"
    
    # ==================== RECONSTRUCCIÓN DE ARCHIVOS ====================
    
    def reconstruct_file(self, file_id):
        """
        Reconstruye un archivo a partir de sus bloques.
        
        Args:
            file_id: ID del archivo a reconstruir
            
        Returns:
            Tupla (datos_del_archivo, nombre_original) o (None, None) si falla
        """
        with self.lock:
            if file_id not in self.file_index:
                print(f"Archivo {file_id} no encontrado en índice")
                return None, None
            
            file_info = self.file_index[file_id]
            block_ids = file_info["block_ids"]
            original_filename = file_info["original_filename"]
        
        # Obtener todos los bloques en orden
        file_data = b""
        
        for block_id in block_ids:
            block_data = self._get_block(block_id)
            if block_data is None:
                print(f"No se pudo obtener bloque {block_id}")
                return None, None
            
            # Decodificar de base64
            file_data += base64.b64decode(block_data)
        
        return file_data, original_filename
    
    def _get_block(self, block_id):
        """
        Obtiene un bloque, buscando primero localmente, luego en nodos remotos.
        Implementa tolerancia a fallas usando réplicas.
        """
        # Primero intentar obtener localmente
        local_data = self.get_block_locally(block_id)
        if local_data:
            return local_data
        
        # Si no está local, buscar en la tabla de bloques
        block_info = self.block_table.get("blocks", {}).get(block_id)
        if not block_info:
            print(f"Bloque {block_id} no encontrado en tabla")
            return None
        
        # Intentar obtener del nodo primario
        primary_node = block_info.get("primary_node")
        if primary_node and primary_node != self.node_name:
            data = self._request_block_from_node(block_id, primary_node)
            if data:
                return data
        
        # Si falla, intentar con la réplica (TOLERANCIA A FALLAS)
        replica_node = block_info.get("replica_node")
        if replica_node and replica_node != self.node_name:
            print(f"Nodo primario falló, intentando con réplica en {replica_node}")
            data = self._request_block_from_node(block_id, replica_node)
            if data:
                return data
        
        return None
    
    def _request_block_from_node(self, block_id, node):
        """Solicita un bloque a un nodo remoto"""
        if not self.network_manager:
            return None
        
        message = {
            "type": "get_block",
            "source_node": self.node_name,
            "block_id": block_id,
            "timestamp": time.time()
        }
        
        response = self.network_manager._send_message(node, message)
        if response and response.get("status") == "ok":
            return response.get("block_data")
        
        return None
    
    # ==================== ELIMINACIÓN DE ARCHIVOS ====================
    
    def delete_file(self, file_id):
        """
        Elimina un archivo y todos sus bloques del sistema.
        
        Args:
            file_id: ID del archivo a eliminar
            
        Returns:
            True si se eliminó correctamente
        """
        with self.lock:
            if file_id not in self.file_index:
                return False
            
            file_info = self.file_index[file_id]
            block_ids = file_info["block_ids"]
            
            # Eliminar cada bloque
            for block_id in block_ids:
                block_info = self.block_table.get("blocks", {}).get(block_id, {})
                
                # Eliminar del nodo primario
                primary_node = block_info.get("primary_node")
                if primary_node == self.node_name:
                    self.delete_block_locally(block_id)
                elif primary_node and self.network_manager:
                    self._delete_block_from_node(block_id, primary_node)
                
                # Eliminar réplica
                replica_node = block_info.get("replica_node")
                if replica_node == self.node_name:
                    self.delete_block_locally(block_id)
                elif replica_node and self.network_manager:
                    self._delete_block_from_node(block_id, replica_node)
                
                # Actualizar uso de nodos
                if primary_node and primary_node in self.block_table.get("node_usage", {}):
                    self.block_table["node_usage"][primary_node] = max(0, self.block_table["node_usage"][primary_node] - 1)
                if replica_node and replica_node in self.block_table.get("node_usage", {}):
                    self.block_table["node_usage"][replica_node] = max(0, self.block_table["node_usage"][replica_node] - 1)
                
                # Eliminar de tabla de bloques
                if block_id in self.block_table.get("blocks", {}):
                    del self.block_table["blocks"][block_id]
            
            # Eliminar del índice de archivos
            del self.file_index[file_id]
            
            # Guardar cambios
            self._save_block_table()
            self._save_file_index()
            
            return True
    
    def _delete_block_from_node(self, block_id, node):
        """Solicita eliminación de un bloque en un nodo remoto"""
        if not self.network_manager:
            return False
        
        message = {
            "type": "delete_block",
            "source_node": self.node_name,
            "block_id": block_id,
            "timestamp": time.time()
        }
        
        response = self.network_manager._send_message(node, message)
        return response and response.get("status") == "ok"
    
    # ==================== UTILIDADES ====================
    
    def get_file_attributes(self, file_id):
        """
        Obtiene los atributos detallados de un archivo.
        
        Returns:
            Diccionario con información del archivo y sus bloques
        """
        with self.lock:
            if file_id not in self.file_index:
                return None
            
            file_info = self.file_index[file_id].copy()
            file_info["blocks_detail"] = []
            
            for block_id in file_info["block_ids"]:
                block_info = self.block_table.get("blocks", {}).get(block_id, {})
                file_info["blocks_detail"].append({
                    "block_id": block_id,
                    "block_num": block_info.get("block_num", 0),
                    "size": block_info.get("size", 0),
                    "primary_node": block_info.get("primary_node", "unknown"),
                    "replica_node": block_info.get("replica_node", "unknown"),
                    "hash": block_info.get("hash", "")
                })
            
            return file_info
    
    def get_system_stats(self):
        """Obtiene estadísticas del sistema de bloques"""
        with self.lock:
            stats = {
                "total_files": len(self.file_index),
                "total_blocks": len(self.block_table.get("blocks", {})),
                "node_usage": self.block_table.get("node_usage", {}),
                "node_capacity": NODE_CAPACITY,
                "node_free_space": {}
            }
            
            for node, capacity in NODE_CAPACITY.items():
                used = stats["node_usage"].get(node, 0)
                stats["node_free_space"][node] = capacity - used
            
            return stats
    
    def get_all_files(self):
        """Retorna lista de todos los archivos en el sistema"""
        with self.lock:
            files = []
            for file_id, info in self.file_index.items():
                files.append({
                    "file_id": file_id,
                    "filename": info["original_filename"],
                    "size": info.get("size", 0),
                    "total_blocks": info.get("total_blocks", 0),
                    "created_at": info.get("created_at", 0)
                })
            return files
    
    def sync_block_table(self, remote_table):
        """
        Sincroniza la tabla de bloques con datos de otro nodo.
        Útil para mantener consistencia en el sistema distribuido.
        """
        with self.lock:
            # Merge de bloques
            for block_id, block_info in remote_table.get("blocks", {}).items():
                if block_id not in self.block_table.get("blocks", {}):
                    self.block_table["blocks"][block_id] = block_info
            
            self._save_block_table()
    
    def sync_file_index(self, remote_index):
        """Sincroniza el índice de archivos con datos de otro nodo"""
        with self.lock:
            for file_id, file_info in remote_index.items():
                if file_id not in self.file_index:
                    self.file_index[file_id] = file_info
            
            self._save_file_index()