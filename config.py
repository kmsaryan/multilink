CHUNK_SIZE = 1200  # Adjusted to fit within 1500-byte MTU, accounting for headers
DB_PATH = "/usr/local/bin/multilink/sender_coord.db"  # VM1 coordination database
RECEIVER_IP = "90.27.22.100"
DATA_PORT = 9000
HEALTH_PORT = 9001
PAYLOAD_DIR = "/usr/local/bin/multilink/payloads"
RECEIVED_DIR = "/usr/local/bin/multilink/received"