from flask import Flask, render_template, request, jsonify
import os
import threading
import logging
import time
from node import Node
from config import WEB_PORT, NODES

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
    
@app.route('/')
def index():
    """Página principal de la interfaz web"""
    return render_template('index.html', node_name=node.node_name, nodes=NODES)

@app.route('/api/node_files/<node_name>', methods=['GET'])
def get_node_files(node_name):
    """API para obtener archivos de un nodo específico"""
    if node_name == node.node_name:
        # Si es el nodo local, usar la función existente
        files = node.list_files()
        return jsonify(files)
    else:
        # Si es otro nodo, solicitar los archivos a través de la red
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
            # Archivo local
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
            # Archivo remoto - necesitamos obtenerlo primero
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
    print("IS DIIIIIIIIIIR-------------", data.get('is_dir'))
    
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
    # Siempre reportar que todos los nodos están conectados
    status = {node: True for node in NODES}
    return jsonify(status)

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
    
    print(f"Iniciando nodo {node.node_name} en http://0.0.0.0:{WEB_PORT}")
    print("Presiona CTRL+C para detener la app")
    
    # Iniciar la aplicación web
    app.run(host='0.0.0.0', port=WEB_PORT, debug=False)