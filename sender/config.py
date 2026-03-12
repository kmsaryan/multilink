import os

CHUNK_SIZE = 1200  # Adjusted to fit within 1500-byte MTU, accounting for headers

# Determine if the script is running in sender or receiver mode
IS_RECEIVER = os.path.basename(os.getcwd()) == "Receiver"

if IS_RECEIVER:
    DB_PATH = "/usr/local/bin/multilink/Receiver/receiver_state.db"  # VM2 coordination database
    PAYLOAD_DIR = "/usr/local/bin/multilink/Receiver/payloads"
    RECEIVED_DIR = "/usr/local/bin/multilink/Receiver/received"
    RESULTS_DIR = "/usr/local/bin/multilink/Receiver/results"
else:
    DB_PATH = "/usr/local/bin/multilink/sender/sender_coord.db"  # VM1 coordination database
    PAYLOAD_DIR = "/usr/local/bin/multilink/sender/payloads"
    RECEIVED_DIR = "/usr/local/bin/multilink/sender/received"

RECEIVER_IP = "90.27.22.100"
DATA_PORT = 9000
HEALTH_PORT = 9001