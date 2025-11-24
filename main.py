from flask import Flask, render_template, request, jsonify, send_file
import os
import threading
import logging
import time
import tempfile
import io
import base64
from node import Node
from config import WEB_PORT, NODES, NODE_CAPACITY

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("sistema.log"),
        logging.StreamHandler()
    ]
)

app = Flask(__name__)
node = Node()

# ==================== RUTAS EXISTENTES ====================

@app.route('/')
def index():
    """Página principal de la interfaz web"""
    return render_template('index.html', node_name=node.node_name, nodes=NODES, node_capacity=NODE_CAPACITY)

@app.route('/api/node_files/<node_name>', methods=['GET'])
def get_node_files(node_name):
    """API para obtener archivos de un nodo específico"""
    if node_name == node.node_name:
        files = node.list_files()
        return jsonify(files)
    else:
        files = node.get_remote_files(node_name)
        if files is None:
            return jsonify([])
        return jsonify(files)

@app.route('/api/files', methods=['GET'])
def list_files():
    """API para listar archivos"""
    files = node.list_files()
    return jsonify(files)

@app.route('/api/view_file', methods=['POST'])
def view_file():
    """API para ver el contenido de un archivo"""
    data = request.get_json()
    filename = data.get('filename')
    source_node = data.get('source_node')
    
    if not filename:
        return jsonify({"status": "error", "message": "Falta nombre de archivo"})
    
    try:
        if source_node == node.node_name:
            file_type, content, error_or_mime = node.file_manager.get_file_content_for_view(filename)
            
            if error_or_mime and file_type is None:
                return jsonify({"status": "error", "message": error_or_mime})
            
            return jsonify({
                "status": "ok",
                "file_type": file_type,
                "content": content,
                "mime_type": error_or_mime if file_type == 'image' else None,
                "filename": filename
            })
        else:
            message = {
                "type": "view_file",
                "source_node": node.node_name,
                "filename": filename,
                "timestamp": time.time()
            }
            
            response = node.network_manager._send_message(source_node, message)
            
            if response and response.get("status") == "ok":
                return jsonify(response)
            else:
                return jsonify({"status": "error", "message": "Error al obtener archivo del nodo remoto"})
                
    except Exception as e:
        return jsonify({"status": "error", "message": f"Error interno: {str(e)}"})

@app.route('/api/transfer', methods=['POST'])
def transfer_file():
    """API para transferir un archivo"""
    data = request.get_json()
    filename = data.get('filename')
    target_node = data.get('target_node')
    source_node = data.get('source_node')
    is_dir = data.get('is_dir')
    
    if not filename or not target_node:
        return jsonify({"status": "error", "message": "Faltan parámetros"})
    
    success = node.transfer_file(filename, target_node, source_node, is_dir)
    
    if success:
        return jsonify({"status": "ok"})
    else:
        return jsonify({"status": "error", "message": "Error al transferir archivo"})

@app.route('/api/delete', methods=['POST'])
def delete_file():
    """API para eliminar un archivo"""
    data = request.get_json()
    filename = data.get('filename')
    source_node = data.get('source_node')
    
    if not filename:
        return jsonify({"status": "error", "message": "Falta nombre de archivo"})
    
    success = node.delete_file(filename)
    
    if success:
        return jsonify({"status": "ok"})
    else:
        return jsonify({"status": "error", "message": "Error al eliminar archivo"})

@app.route('/api/status', methods=['GET'])
def get_status():
    """API para obtener el estado de los nodos"""
    status = node.get_node_status()
    return jsonify(status)

# ==================== NUEVAS RUTAS PARA SISTEMA DE BLOQUES ====================

@app.route('/api/upload', methods=['POST'])
def upload_file():
    """
    API para subir un archivo al sistema distribuido.
    
    El archivo se divide en bloques de 1 MB, se distribuye entre nodos
    y se replica automáticamente.
    """
    if 'file' not in request.files:
        return jsonify({"status": "error", "message": "No se envió ningún archivo"})
    
    file = request.files['file']
    
    if file.filename == '':
        return jsonify({"status": "error", "message": "Nombre de archivo vacío"})
    
    try:
        # Guardar archivo temporalmente
        temp_dir = tempfile.gettempdir()
        temp_path = os.path.join(temp_dir, file.filename)
        file.save(temp_path)
        
        # Subir al sistema distribuido
        result = node.upload_file(temp_path, file.filename)
        
        # Limpiar archivo temporal
        if os.path.exists(temp_path):
            os.remove(temp_path)
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({"status": "error", "message": f"Error al subir archivo: {str(e)}"})

@app.route('/api/download/<file_id>', methods=['GET'])
def download_file(file_id):
    """
    API para descargar un archivo del sistema distribuido.
    
    Reconstruye el archivo desde sus bloques distribuidos.
    """
    try:
        file_data, original_filename = node.download_file(file_id)
        
        if file_data is None:
            return jsonify({"status": "error", "message": "No se pudo reconstruir el archivo"})
        
        # Crear archivo en memoria para enviar
        file_stream = io.BytesIO(file_data)
        file_stream.seek(0)
        
        return send_file(
            file_stream,
            as_attachment=True,
            download_name=original_filename,
            mimetype='application/octet-stream'
        )
        
    except Exception as e:
        return jsonify({"status": "error", "message": f"Error al descargar: {str(e)}"})

@app.route('/api/delete_distributed/<file_id>', methods=['DELETE'])
def delete_distributed_file(file_id):
    """
    API para eliminar un archivo distribuido y todos sus bloques.
    Tolerante a fallos: elimina el archivo aunque algunos nodos estén desconectados.
    Maneja archivos huérfanos (bloques sin entrada en file_index).
    """
    try:
        result = node.delete_distributed_file(file_id)
        
        if isinstance(result, dict):
            # Nueva respuesta detallada
            if result.get("success"):
                message = f"Archivo eliminado: {result.get('blocks_deleted', 0)} bloques eliminados"
                if result.get("blocks_failed", 0) > 0:
                    message += f", {result['blocks_failed']} bloques no disponibles (nodos offline: {', '.join(result.get('failed_nodes', []))})"
                
                return jsonify({
                    "status": "ok", 
                    "message": message,
                    "details": result
                })
            elif result.get("error") == "File not found":
                # Archivo no existe en file_index, pero podría tener bloques huérfanos
                # Intentar eliminar bloques huérfanos con este file_id
                logging.info(f"Archivo {file_id} no en índice, buscando bloques huérfanos...")
                
                block_table = node.get_block_table()
                orphan_blocks = {}
                
                # Buscar bloques de este file_id
                for block_id, block_info in block_table.get("blocks", {}).items():
                    if block_info.get("file_id") == file_id:
                        orphan_blocks[block_id] = block_info
                
                if not orphan_blocks:
                    return jsonify({
                        "status": "error",
                        "message": "Archivo no encontrado y no tiene bloques"
                    })
                
                # Eliminar bloques huérfanos
                deleted = 0
                with node.block_manager.lock:
                    for block_id, block_info in orphan_blocks.items():
                        try:
                            # Eliminar localmente
                            primary_node = block_info.get("primary_node")
                            if primary_node == node.node_name:
                                node.block_manager.delete_block_locally(block_id)
                            elif primary_node:
                                node.block_manager._delete_block_from_node(block_id, primary_node)
                            
                            replica_node = block_info.get("replica_node")
                            if replica_node == node.node_name:
                                node.block_manager.delete_block_locally(block_id)
                            elif replica_node:
                                node.block_manager._delete_block_from_node(block_id, replica_node)
                            
                            # Eliminar de tabla
                            if block_id in block_table.get("blocks", {}):
                                del block_table["blocks"][block_id]
                            
                            deleted += 1
                        except:
                            pass
                    
                    node.block_manager.block_table = block_table
                    node.block_manager._save_block_table()
                
                # Propagar a otros nodos
                node_status = node.network_manager.get_node_status()
                for other_node, is_alive in node_status.items():
                    if other_node != node.node_name and is_alive:
                        try:
                            message_data = {
                                "type": "cleanup_orphan_blocks",
                                "source_node": node.node_name,
                                "orphan_file_ids": [file_id],
                                "timestamp": time.time()
                            }
                            node.network_manager._send_message(other_node, message_data)
                        except:
                            pass
                
                return jsonify({
                    "status": "ok",
                    "message": f"Bloques huérfanos eliminados: {deleted}",
                    "details": {"blocks_deleted": deleted, "was_orphan": True}
                })
            else:
                return jsonify({
                    "status": "error", 
                    "message": result.get("error", "Error al eliminar archivo")
                })
        else:
            # Respuesta legacy (bool)
            if result:
                return jsonify({"status": "ok", "message": "Archivo eliminado correctamente"})
            else:
                return jsonify({"status": "error", "message": "Error al eliminar archivo"})
            
    except Exception as e:
        return jsonify({"status": "error", "message": f"Error: {str(e)}"})

@app.route('/api/distributed_files', methods=['GET'])
def get_distributed_files():
    """
    API para obtener la lista de archivos distribuidos en el sistema.
    """
    try:
        files = node.get_distributed_files()
        return jsonify({"status": "ok", "files": files})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

@app.route('/api/view_distributed/<file_id>', methods=['GET'])
def view_distributed_file(file_id):
    """
    API para ver el contenido de un archivo distribuido.
    Reconstruye el archivo y lo muestra (texto o imagen).
    """
    try:
        # Reconstruir el archivo desde sus bloques
        file_data, original_filename = node.download_file(file_id)
        
        if file_data is None:
            return jsonify({"status": "error", "message": "No se pudo reconstruir el archivo"})
        
        # Determinar el tipo de archivo por extensión
        import os
        file_extension = os.path.splitext(original_filename)[1].lower()
        
        # Tipos de archivo soportados
        text_extensions = ['.txt', '.py', '.js', '.html', '.css', '.json', '.xml', '.md', '.yml', '.yaml', '.log']
        image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.webp']
        
        # Archivo de texto
        if file_extension in text_extensions or len(file_data) == 0:
            try:
                content = file_data.decode('utf-8')
                return jsonify({
                    "status": "ok",
                    "file_type": "text",
                    "content": content,
                    "filename": original_filename
                })
            except UnicodeDecodeError:
                # Si no se puede decodificar como texto, tratarlo como binario
                return jsonify({
                    "status": "ok",
                    "file_type": "binary",
                    "content": base64.b64encode(file_data).decode('utf-8'),
                    "filename": original_filename
                })
        
        # Archivo de imagen
        elif file_extension in image_extensions:
            mime_types = {
                '.jpg': 'image/jpeg',
                '.jpeg': 'image/jpeg',
                '.png': 'image/png',
                '.gif': 'image/gif',
                '.bmp': 'image/bmp',
                '.svg': 'image/svg+xml',
                '.webp': 'image/webp'
            }
            mime_type = mime_types.get(file_extension, 'application/octet-stream')
            
            return jsonify({
                "status": "ok",
                "file_type": "image",
                "content": base64.b64encode(file_data).decode('utf-8'),
                "mime_type": mime_type,
                "filename": original_filename
            })
        
        # Archivo no soportado para visualización
        else:
            return jsonify({
                "status": "error",
                "message": f"Tipo de archivo no soportado para visualización: {file_extension}",
                "file_type": "unsupported"
            })
            
    except Exception as e:
        return jsonify({"status": "error", "message": f"Error al ver archivo: {str(e)}"})

@app.route('/api/file_attributes/<file_id>', methods=['GET'])
def get_file_attributes(file_id):
    """
    API para obtener los atributos detallados de un archivo.
    
    Muestra en qué nodos está cada bloque.
    """
    try:
        attributes = node.get_file_attributes(file_id)
        
        if attributes:
            return jsonify({"status": "ok", "attributes": attributes})
        else:
            return jsonify({"status": "error", "message": "Archivo no encontrado"})
            
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

@app.route('/api/block_table', methods=['GET'])
def get_block_table():
    """
    API para obtener la tabla de bloques completa.
    """
    try:
        block_table = node.get_block_table()
        return jsonify({"status": "ok", "block_table": block_table})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

@app.route('/api/system_stats', methods=['GET'])
def get_system_stats():
    """
    API para obtener estadísticas del sistema.
    
    Incluye: archivos totales, bloques, uso por nodo, espacio libre.
    """
    try:
        stats = node.get_system_stats()
        return jsonify({"status": "ok", "stats": stats})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

@app.route('/api/cleanup_orphan_blocks', methods=['POST'])
def cleanup_orphan_blocks():
    """
    API para limpiar bloques huérfanos (bloques sin archivo en file_index).
    
    Útil para limpiar bloques de archivos que se subieron parcialmente.
    """
    try:
        # Obtener tablas
        block_table = node.get_block_table()
        file_index = node.block_manager.get_file_index()
        
        # Encontrar file_ids válidos
        valid_file_ids = set(file_index.keys())
        
        # Encontrar bloques huérfanos
        all_blocks = block_table.get("blocks", {})
        orphan_blocks = {}
        orphan_file_ids = set()
        
        for block_id, block_info in all_blocks.items():
            file_id = block_info.get("file_id")
            if file_id and file_id not in valid_file_ids:
                orphan_blocks[block_id] = block_info
                orphan_file_ids.add(file_id)
        
        if not orphan_blocks:
            return jsonify({
                "status": "ok",
                "message": "No hay bloques huérfanos",
                "orphan_count": 0
            })
        
        # Eliminar bloques huérfanos
        deleted_count = 0
        failed_count = 0
        
        with node.block_manager.lock:
            for block_id in orphan_blocks.keys():
                try:
                    block_info = orphan_blocks[block_id]
                    
                    # Eliminar del nodo primario
                    primary_node = block_info.get("primary_node")
                    if primary_node == node.node_name:
                        node.block_manager.delete_block_locally(block_id)
                    elif primary_node and node.network_manager:
                        node.block_manager._delete_block_from_node(block_id, primary_node)
                    
                    # Eliminar réplica
                    replica_node = block_info.get("replica_node")
                    if replica_node == node.node_name:
                        node.block_manager.delete_block_locally(block_id)
                    elif replica_node and node.network_manager:
                        node.block_manager._delete_block_from_node(block_id, replica_node)
                    
                    # Actualizar uso de nodos
                    if primary_node and primary_node in block_table.get("node_usage", {}):
                        block_table["node_usage"][primary_node] = max(0, block_table["node_usage"][primary_node] - 1)
                    if replica_node and replica_node in block_table.get("node_usage", {}):
                        block_table["node_usage"][replica_node] = max(0, block_table["node_usage"][replica_node] - 1)
                    
                    # Eliminar de tabla de bloques
                    if block_id in block_table.get("blocks", {}):
                        del block_table["blocks"][block_id]
                    
                    deleted_count += 1
                except Exception as e:
                    logging.error(f"Error al eliminar bloque huérfano {block_id}: {e}")
                    failed_count += 1
            
            # Guardar cambios
            node.block_manager.block_table = block_table
            node.block_manager._save_block_table()
        
        # Propagar limpieza a otros nodos
        node_status = node.network_manager.get_node_status()
        for other_node, is_alive in node_status.items():
            if other_node != node.node_name and is_alive:
                try:
                    message = {
                        "type": "cleanup_orphan_blocks",
                        "source_node": node.node_name,
                        "orphan_file_ids": list(orphan_file_ids),
                        "timestamp": time.time()
                    }
                    node.network_manager._send_message(other_node, message)
                except:
                    pass
        
        return jsonify({
            "status": "ok",
            "message": f"Bloques huérfanos eliminados: {deleted_count}",
            "deleted": deleted_count,
            "failed": failed_count,
            "orphan_file_ids": list(orphan_file_ids)
        })
        
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})


# ==================== INICIO DE LA APLICACIÓN ====================

def start_node():
    node.start()

if __name__ == "__main__":
    # Configurar carpeta static para CSS
    app.static_folder = 'static'
    
    # Asegurar que existe la carpeta static
    os.makedirs(app.static_folder, exist_ok=True)
    
    # Guardar CSS en static
    from config import SHARED_DIR
    css_path = os.path.join(app.static_folder, 'styles.css')
    if os.path.exists('styles.css') and not os.path.exists(css_path):
        with open('styles.css', 'r') as f:
            css_content = f.read()
        with open(css_path, 'w') as f:
            f.write(css_content)
    
    # Iniciar el nodo en un thread separado
    node_thread = threading.Thread(target=start_node)
    node_thread.daemon = True
    node_thread.start()
    
    print(f"\n{'='*60}")
    print(f"  SISTEMA DE ARCHIVOS DISTRIBUIDO TOLERANTE A FALLAS")
    print(f"{'='*60}")
    print(f"  Nodo: {node.node_name}")
    print(f"  URL: http://0.0.0.0:{WEB_PORT}")
    print(f"{'='*60}")
    print(f"  Capacidad de este nodo: {NODE_CAPACITY.get(node.node_name, 50)} MB")
    print(f"  Tamaño de bloque: 1 MB")
    print(f"{'='*60}")
    print("  Presiona CTRL+C para detener la aplicación")
    print(f"{'='*60}\n")
    
    # Iniciar la aplicación web
    app.run(host='0.0.0.0', port=WEB_PORT, debug=False)