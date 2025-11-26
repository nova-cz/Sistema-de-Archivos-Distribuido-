import socket
import os
import netifaces
import logging
import subprocess

IP_1 = "172.31.3.27"      # Tu IP (Maq1)
IP_2 = "172.31.14.108"      # IP de tu amigo (Maq2)
IP_3 = "172.31.3.141"       # IP tercera compu (Maq3)

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

THIS_NODE = "Maq1"  # TÚ: "Maq1", AMIGO 1: "Maq2", AMIGO 2: "Maq3"

# ==================== NUEVA CONFIGURACIÓN DE BLOQUES ====================

BLOCK_SIZE = 1024 * 1024  # 1 MB

NODE_CAPACITY = {
    "Maq1": 70,   # 70 MB para Maq1
    "Maq2": 50,   # 50 MB para Maq2
    "Maq3": 60,   # 60 MB para Maq3
}

# ========================================================================

def get_ip_address():
    """Obtiene la IP basada en el nodo configurado"""
    node_ips = {
        "Maq1": IP_1,
        "Maq2": IP_2,
        "Maq3": IP_3,
    }
    
    if THIS_NODE in node_ips:
        ip = node_ips[THIS_NODE]
        logger.info(f"Usando IP para {THIS_NODE}: {ip}")
        return ip
    else:
        logger.warning(f"Nodo '{THIS_NODE}' no reconocido, intentando detección automática...")
        return detect_ip_automatically()

def detect_ip_automatically():
    """Función original para detectar IP automáticamente"""
    try:
        logger.info("Buscando interfaces de red...")
        
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
            else:  # Windows
                result = subprocess.run(['ipconfig'], capture_output=True, text=True)
                for line in result.stdout.split('\n'):
                    if 'IPv4' in line:
                        ip = line.split(':')[-1].strip()
                        if not ip.startswith('127.'):
                            logger.info(f"IP encontrada con ipconfig: {ip}")
                            return ip
        except Exception as e:
            logger.warning(f"Error al usar ifconfig/ip/ipconfig: {e}")
        
        interfaces = netifaces.interfaces()
        for interface in interfaces:
            logger.debug(f"Revisando interfaz: {interface}")
            addresses = netifaces.ifaddresses(interface)
            if netifaces.AF_INET in addresses:
                for link in addresses[netifaces.AF_INET]:
                    ip = link['addr']
                    logger.debug(f"  IP encontrada: {ip}")
                    if not ip.startswith('127.'):
                        logger.info(f"IP seleccionada: {ip}")
                        return ip
        
        logger.warning("No se encontró una IP adecuada. Usando IP por defecto.")
        return "192.168.1.101"
    except Exception as e:
        logger.error(f"Error al obtener IP: {e}")
        return "192.168.1.101"

HOSTNAME = socket.gethostname()
logger.info(f"Nombre del host: {HOSTNAME}")

IP_ADDRESS = get_ip_address()
logger.info(f"IP seleccionada: {IP_ADDRESS}")

# ========== 3 NODOS ==========
NODES = {
    "Maq1": {"ip": IP_1, "port": 8080},
    "Maq2": {"ip": IP_2, "port": 8080},
    "Maq3": {"ip": IP_3, "port": 8080},
}
    
NODE_NAME = THIS_NODE
logger.info(f"Este nodo se identificó como: {NODE_NAME}")
    
if NODE_NAME not in NODES:
    logger.error(f"El nodo '{NODE_NAME}' no existe en la configuración NODES.")
    logger.info("Nodos disponibles:")
    for name, info in NODES.items():
        logger.info(f"  - {name}: {info['ip']}")
    logger.error("Por favor, corrige la variable THIS_NODE.")
    exit(1)

if NODES[NODE_NAME]["ip"] != IP_ADDRESS:
    logger.error(f"La IP configurada para {NODE_NAME} ({NODES[NODE_NAME]['ip']}) no coincide con la IP seleccionada ({IP_ADDRESS}).")
    logger.error("Por favor, verifica la configuración de nodos y la variable THIS_NODE.")
    exit(1)

WEB_PORT = NODES[NODE_NAME]["port"]
logger.info(f"Puerto web: {WEB_PORT}")

NETWORK_PORT = 8081
logger.info(f"Puerto de red: {NETWORK_PORT}")

SHARED_DIR = os.path.join(os.path.expanduser("."), "shared_dir")
os.makedirs(SHARED_DIR, exist_ok=True)
logger.info(f"Directorio compartido: {SHARED_DIR}")

LOG_FILE = os.path.join(SHARED_DIR, "operations.json")
logger.info(f"Archivo de log: {LOG_FILE}")

PENDING_LOG_FILE = os.path.join(SHARED_DIR, "pending_operations.json")
logger.info(f"Archivo de log pendiente: {PENDING_LOG_FILE}")

HEARTBEAT_INTERVAL = 3
logger.info(f"Intervalo de heartbeat: {HEARTBEAT_INTERVAL} segundos")

NODE_TIMEOUT = 8
logger.info(f"Timeout de nodo: {NODE_TIMEOUT} segundos")

logger.info("=== Configuración de capacidad de nodos ===")
for node, capacity in NODE_CAPACITY.items():
    logger.info(f"  {node}: {capacity} MB")
total_capacity = sum(NODE_CAPACITY.get(n, 0) for n in NODES.keys())
logger.info(f"  Capacidad total del sistema: {total_capacity} MB")
logger.info(f"  Capacidad útil (con réplicas): ~{total_capacity // 2} MB")