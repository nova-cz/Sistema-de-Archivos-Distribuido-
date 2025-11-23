import os
import shutil
import threading
import base64
from config import SHARED_DIR

class FileManager:

    def __init__(self, operation_log):
        self.shared_dir = SHARED_DIR
        self.lock = threading.Lock()
        self.operation_log = operation_log
        
        # Asegurar que el directorio compartido existe
        os.makedirs(self.shared_dir, exist_ok=True)

    def list_files(self, path1=None):
        print(path1)
        if path1 is None:
            folder_path = self.shared_dir
            prefix = ''
        else:
            folder_path = os.path.join(self.shared_dir, path1)
            prefix = path1 + '/' if not path1.endswith('/') else path1

            if not os.path.exists(folder_path):
                print(f"ERROR: Carpeta {folder_path} no existe")
                return None

            if not os.path.isdir(folder_path):
                print(f"ERROR: {folder_path} no es una carpeta")
                return None

        files = []

        with self.lock:
            # Agrega path1 como carpeta si fue especificado
            if path1 is not None:
                stat = os.stat(folder_path)
                files.append({
                    'name': path1,
                    'path': folder_path,
                    'size': 0,
                    'modified': stat.st_mtime,
                    'is_dir': True
                })

            for root, dirs, filenames in os.walk(folder_path):
                relative_root = os.path.relpath(root, folder_path)
                if relative_root == '.':
                    relative_root = ''
                
                for filename in filenames:
                    if filename in ('operations.json', 'pending_operations.json'):
                        continue

                    full_path = os.path.join(root, filename)
                    relative_path = os.path.join(relative_root, filename)
                    relative_path = os.path.normpath(relative_path)

                    # Prefijar con path1 si aplica
                    final_name = os.path.join(prefix, relative_path) if prefix else relative_path

                    stat = os.stat(full_path)
                    files.append({
                        'name': final_name,
                        'path': full_path,
                        'size': stat.st_size,
                        'modified': stat.st_mtime,
                        'is_dir': False
                    })

                for dirname in dirs:
                    full_path = os.path.join(root, dirname)
                    relative_path = os.path.join(relative_root, dirname)
                    relative_path = os.path.normpath(relative_path)

                    final_name = os.path.join(prefix, relative_path) if prefix else relative_path

                    stat = os.stat(full_path)
                    files.append({
                        'name': final_name,
                        'path': full_path,
                        'size': 0,
                        'modified': stat.st_mtime,
                        'is_dir': True
                    })

        files.sort(key=lambda op: op["path"])
        return files
    
    def get_file_data(self, filename):
        """Obtiene los datos de un archivo"""
        file_path = os.path.join(self.shared_dir, filename)
        
        if not os.path.exists(file_path):
            return None
        
        if os.path.isdir(file_path):
            return None  # No se pueden transferir directorios directamente
        
        with open(file_path, 'rb') as f:
            file_data = f.read()
        
        return base64.b64encode(file_data).decode('utf-8')
    
    def get_folder_data(self, folder_name):
        """Obtiene todos los archivos de una carpeta y subcarpetas"""
        folder_path = os.path.join(self.shared_dir, folder_name)
        
        if not os.path.exists(folder_path):
            print(f"ERROR: Carpeta {folder_path} no existe")
            return None
        
        if not os.path.isdir(folder_path):
            print(f"ERROR: {folder_path} no es una carpeta")
            return None
        
        try:
            folder_data = {
                'folder_name': folder_name,
                'files': {}
            }
            
            # Recorrer todos los archivos y subcarpetas
            for root, dirs, files in os.walk(folder_path):
                # Calcular la ruta relativa desde la carpeta base
                rel_dir = os.path.relpath(root, folder_path)
                if rel_dir == '.':
                    rel_dir = ''
                
                # Procesar archivos
                for file in files:
                    file_path = os.path.join(root, file)
                    relative_file_path = os.path.join(rel_dir, file) if rel_dir else file
                    
                    # Leer el archivo
                    with open(file_path, 'rb') as f:
                        file_content = f.read()
                    
                    # Convertir a base64
                    file_data_b64 = base64.b64encode(file_content).decode('utf-8')
                    
                    folder_data['files'][relative_file_path] = file_data_b64
                    print(f"Agregado archivo: {relative_file_path} ({len(file_content)} bytes)")
            
            print(f"Carpeta {folder_name} procesada. Total archivos: {len(folder_data['files'])}")
            return folder_data
            
        except Exception as e:
            print(f"ERROR al leer carpeta {folder_name}: {e}")
            return None
    
    def save_file(self, filename, file_data, is_base64=True):
        """Guarda un archivo en el sistema"""
        file_path = os.path.join(self.shared_dir, filename)
        
        # Crear los directorios necesarios
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        try:
            with self.lock:
                if is_base64:
                    # Decodificar base64, incluso si está vacío
                    if file_data == '':
                        # Archivo vacío
                        decoded_data = b''
                    else:
                        decoded_data = base64.b64decode(file_data)
                else:
                    decoded_data = file_data
                
                with open(file_path, 'wb') as f:
                    f.write(decoded_data)
            
            return True
            
        except Exception as e:
            print(f"ERROR al guardar archivo {filename}: {e}")
            return False
    
    def save_folder(self, folder_data):
        """Guarda una carpeta completa con todos sus archivos"""
        if not folder_data or 'folder_name' not in folder_data or 'files' not in folder_data:
            return False
        
        folder_name = folder_data['folder_name']
        files = folder_data['files']
        
        try:
            with self.lock:
                # Crear la carpeta base
                folder_path = os.path.join(self.shared_dir, folder_name)
                os.makedirs(folder_path, exist_ok=True)
                
                # Crear todos los archivos
                for relative_file_path, file_data_b64 in files.items():
                    full_file_path = os.path.join(folder_path, relative_file_path)
                    
                    # Crear directorios intermedios si es necesario
                    os.makedirs(os.path.dirname(full_file_path), exist_ok=True)
                    
                    # Decodificar y guardar archivo
                    if file_data_b64 == '':
                        file_content = b''
                    else:
                        file_content = base64.b64decode(file_data_b64)
                    
                    with open(full_file_path, 'wb') as f:
                        f.write(file_content)
                        
                return True
                
        except Exception as e:
            return False
    
    def create_folder(self, filename):
        file_path_temp = os.path.join(self.shared_dir, filename)
        file_path = os.path.join(file_path_temp, "temp")
        
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        return True
    
    def delete_file(self, filename):
        """Elimina un archivo o directorio del sistema"""
        file_path = os.path.join(self.shared_dir, filename)
        
        if not os.path.exists(file_path):
            return True
        
        with self.lock:
            if os.path.isdir(file_path):
                shutil.rmtree(file_path)
            else:
                os.remove(file_path)
        
        return True
    
    def get_file_content_for_view(self, filename):
        """Obtiene el contenido de un archivo para visualización en la web"""
        file_path = os.path.join(self.shared_dir, filename)
        
        if not os.path.exists(file_path):
            return None, None, "Archivo no encontrado"
        
        if os.path.isdir(file_path):
            return None, None, "Es una carpeta, no un archivo"
        
        try:
            # Obtener información del archivo
            stat = os.stat(file_path)
            file_size = stat.st_size
            
            # Determinar el tipo de archivo basado en la extensión
            file_extension = os.path.splitext(filename)[1].lower()
            
            # Definir tipos de archivos soportados
            text_extensions = ['.txt', '.py', '.js', '.html', '.css', '.json', '.xml', '.md', '.yml', '.yaml', '.ini', '.cfg', '.log']
            image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.webp']
            
            if file_extension in text_extensions or file_size == 0:
                # Archivo de texto o archivo vacío
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    return 'text', content, None
                except UnicodeDecodeError:
                    # Si no se puede leer como UTF-8, intentar como binario
                    with open(file_path, 'rb') as f:
                        content = f.read()
                    return 'binary', base64.b64encode(content).decode('utf-8'), None
            
            elif file_extension in image_extensions:
                # Archivo de imagen
                with open(file_path, 'rb') as f:
                    content = f.read()
                mime_type = self._get_mime_type(file_extension)
                return 'image', base64.b64encode(content).decode('utf-8'), mime_type
            
            else:
                # Archivo binario o no soportado para visualización
                return 'unsupported', None, f"Tipo de archivo no soportado para visualización: {file_extension}"
                
        except Exception as e:
            return None, None, f"Error al leer archivo: {str(e)}"
    
    def _get_mime_type(self, extension):
        """Obtiene el tipo MIME basado en la extensión del archivo"""
        mime_types = {
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.png': 'image/png',
            '.gif': 'image/gif',
            '.bmp': 'image/bmp',
            '.svg': 'image/svg+xml',
            '.webp': 'image/webp'
        }
        return mime_types.get(extension, 'application/octet-stream')