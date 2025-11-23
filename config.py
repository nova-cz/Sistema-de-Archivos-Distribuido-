import socket
import os
import netifaces
import logging
import subprocess

IP_1 = "172.26.164.205"
IP_2 = "172.26.161.192"
IP_3 = "172.26.160.112"
IP_4 = "172.26.164.208"

# Configurar logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sistema.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('sistema')

THIS_NODE = "Maq1"  # Opciones: "Maq1", "Maq2", "Maq3", "Maq4"

def get_ip_address():
    """Obtiene la IP basada en el nodo configurado"""
    # Definimos las IPs de los nodos
    node_ips = {
        "Maq1": IP_1,
        "Maq2": IP_2,
        "Maq3": IP_3,
        "Maq4": IP_4
    }
    
    # Devuelve la IP correspondiente al nodo configurado
    if THIS_NODE in node_ips:
        ip = node_ips[THIS_NODE]
        logger.info(f"Usando IP para {THIS_NODE}: {ip}")
        return ip
    else:
        # Si hay un error en la configuración, intentamos detectar automáticamente
        logger.warning(f"Nodo '{THIS_NODE}' no reconocido, intentando detección automática...")
        return detect_ip_automatically()

def detect_ip_automatically():
    """Función original para detectar IP automáticamente"""
    try:
        logger.info("Buscando interfaces de red...")
        
        # Primero intentar con ifconfig/ip
        try:
            if os.name == 'posix':  # Linux/Unix
                if os.path.exists('/sbin/ip'):
                    result = subprocess.run(['ip', 'addr', 'show'], capture_output=True, text=True)
                    for line in result.stdout.split('\n'):
                        if 'inet ' in line and '127.0.0.1' not in line:
                            ip = line.split('inet ')[1].split('/')[0].strip()
                            logger.info(f"IP encontrada con ip addr: {ip}")
                            return ip
                else:
                    result = subprocess.run(['ifconfig'], capture_output=True, text=True)
                    for line in result.stdout.split('\n'):
                        if 'inet ' in line and '127.0.0.1' not in line:
                            ip = line.split('inet ')[1].split(' ')[0].strip()
                            logger.info(f"IP encontrada con ifconfig: {ip}")
                            return ip
        except Exception as e:
            logger.warning(f"Error al usar ifconfig/ip: {e}")
        
        # Si no funciona, usar netifaces
        interfaces = netifaces.interfaces()
        for interface in interfaces:
            logger.debug(f"Revisando interfaz: {interface}")
            addresses = netifaces.ifaddresses(interface)
            if netifaces.AF_INET in addresses:
                for link in addresses[netifaces.AF_INET]:
                    ip = link['addr']
                    logger.debug(f"  IP encontrada: {ip}")
                    # Solo evitar localhost
                    if not ip.startswith('127.'):
                        logger.info(f"IP seleccionada: {ip}")
                        return ip
        
        logger.warning("No se encontró una IP adecuada. Usando IP por defecto.")
        return "192.168.1.101"
    except Exception as e:
        logger.error(f"Error al obtener IP: {e}")
        return "192.168.1.101"

# Obtiene el nombre de host de la máquina actual
HOSTNAME = socket.gethostname()
logger.info(f"Nombre del host: {HOSTNAME}")

# Obtiene la dirección IP de la máquina actual basada en el nodo configurado
IP_ADDRESS = get_ip_address()
logger.info(f"IP seleccionada: {IP_ADDRESS}")

NODES = {
    "Maq1": {"ip": IP_1, "port": 8080},
    "Maq2": {"ip": IP_2, "port": 8080},
    "Maq3": {"ip": IP_3, "port": 8080},
}
    
# Asignamos directamente el nombre del nodo
NODE_NAME = THIS_NODE
logger.info(f"Este nodo se identificó como: {NODE_NAME}")
    
# Verificamos que el nodo exista en la configuración
if NODE_NAME not in NODES:
    logger.error(f"El nodo '{NODE_NAME}' no existe en la configuración NODES.")
    logger.info("Nodos disponibles:")
    for name, info in NODES.items():
        logger.info(f"  - {name}: {info['ip']}")
    logger.error("Por favor, corrige la variable THIS_NODE.")
    exit(1)

# Verificamos que la IP coincida
if NODES[NODE_NAME]["ip"] != IP_ADDRESS:
    logger.error(f"La IP configurada para {NODE_NAME} ({NODES[NODE_NAME]['ip']}) no coincide con la IP seleccionada ({IP_ADDRESS}).")
    logger.error("Por favor, verifica la configuración de nodos y la variable THIS_NODE.")
    exit(1)

# Puerto para la interfaz web
WEB_PORT = NODES[NODE_NAME]["port"]
logger.info(f"Puerto web: {WEB_PORT}")

# Puerto para la comunicación entre nodos
NETWORK_PORT = 8081
logger.info(f"Puerto de red: {NETWORK_PORT}")

# Directorio para archivos compartidos
SHARED_DIR = os.path.join(os.path.expanduser("."), "shared_dir")
os.makedirs(SHARED_DIR, exist_ok=True)
logger.info(f"Directorio compartido: {SHARED_DIR}")

# Archivo de registro de operaciones
LOG_FILE = os.path.join(SHARED_DIR, "operations.json")
logger.info(f"Archivo de log: {LOG_FILE}")

# Archivo de registro de operaciones pendientes
PENDING_LOG_FILE = os.path.join(SHARED_DIR, "pending_operations.json")
logger.info(f"Archivo de log pendiente: {PENDING_LOG_FILE}")

# Intervalo de heartbeat en segundos
HEARTBEAT_INTERVAL = 3
logger.info(f"Intervalo de heartbeat: {HEARTBEAT_INTERVAL} segundos")

# Tiempo máximo sin recibir heartbeat antes de considerar un nodo caído (segundos)
NODE_TIMEOUT = 8
logger.info(f"Timeout de nodo: {NODE_TIMEOUT} segundos")