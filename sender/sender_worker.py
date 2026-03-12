#!/usr/bin/python3
import socket as socklib
import struct
import uuid
import time
import sys
import os
import sqlite3
import threading
import logging
from db_utils import get_conn
from config import DB_PATH, RECEIVER_IP, DATA_PORT

# Setup logging
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

def setup_logger(interface_ip):
    logger = logging.getLogger(f'sender_worker_{interface_ip}')
    logger.setLevel(logging.DEBUG)
    
    # File handler - captures everything
    log_file = os.path.join(LOG_DIR, f'sender_worker_{interface_ip.replace(".", "_")}.log')
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    
    # Console handler - shows INFO and above
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    
    # Formatter with timestamp
    formatter = logging.Formatter('[%(asctime)s] [%(levelname)-8s] %(message)s', 
                                  datefmt='%Y-%m-%d %H:%M:%S')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

def make_packet(payload_id, idx, data, packet_type=0):
    """Header: [Type:1][UUID:16][Index:4]. Total header = 21 bytes."""
    pid = uuid.UUID(payload_id).bytes
    header = struct.pack("!B", packet_type) + pid + struct.pack("!I", idx)
    return header + data

def forward_ack_to_orchestrator(ack):
    """Forward ACK via UNIX socket."""
    path = "/tmp/orchestrator.sock"
    if os.path.exists(path):
        try:
            with socklib.socket(socklib.AF_UNIX, socklib.SOCK_DGRAM) as unix_sock:
                unix_sock.sendto(ack, path)
        except Exception: pass

def receive_acks(sock, logger):
    """Background thread to handle incoming UDP ACKs."""
    sock.setsockopt(socklib.SOL_SOCKET, socklib.SO_RCVBUF, 1024 * 1024)
    ack_count = 0
    error_count = 0
    while True:
        try:
            ack, _ = sock.recvfrom(1024)
            # Fail Fast Check: Ensure it is a valid 21-byte ACK
            if ack and len(ack) == 21 and ack[0] == 0: 
                forward_ack_to_orchestrator(ack)
                ack_count += 1
                if ack_count % 500 == 0:
                    logger.info(f"ACK receiver: Processed {ack_count} acknowledgments")
            else:
                error_count += 1
                if error_count % 100 == 0:
                    logger.debug(f"ACK receiver: {error_count} malformed ACKs received")
        except Exception as e: 
            logger.error(f"ACK receiver exception: {e}")
            break

def send_metadata_packet(sock, payload_id, target_addr, logger):
    conn = get_conn(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT filename, total_chunks FROM payloads WHERE payload_id = ?", (payload_id,))
    row = cur.fetchone()
    conn.close()

    if row:
        fname, total = row
        pid_bytes = uuid.UUID(payload_id).bytes
        header = struct.pack("!B", 4) + pid_bytes + struct.pack("!I", total)
        payload = header + fname.encode('utf-8')
        sock.sendto(payload, target_addr)
        logger.info(f"Metadata sent for {fname} ({total} chunks)")

def run_worker(local_ip):
    logger = setup_logger(local_ip)
    logger.info(f"Sender worker started for {local_ip}")
    
    sock = socklib.socket(socklib.AF_INET, socklib.SOCK_DGRAM)
    
    sock.bind((local_ip, 0))
    target_addr = (RECEIVER_IP, DATA_PORT)
    
    logger.info(f"Worker {local_ip} active. Target: {RECEIVER_IP}:{DATA_PORT}")
    threading.Thread(target=receive_acks, args=(sock, logger), daemon=True).start()

    sent_metadata_cache = set()
    local_sent_cache = {}
    send_count = 0

    while True:
        conn = None
        try:
            conn = get_conn(DB_PATH)
            cur = conn.cursor()
            
            cur.execute(
                """
                SELECT payload_id, idx, data FROM chunks
                WHERE state='sending' AND assigned_interface=?
                ORDER BY payload_id, idx LIMIT 50
                """, (local_ip,)
            )
            chunks = cur.fetchall()

            if chunks:
                current_pid = chunks[0][0]
                if current_pid not in sent_metadata_cache:
                    send_metadata_packet(sock, current_pid, target_addr, logger)
                    sent_metadata_cache.add(current_pid)

                sent_list = []
                now = time.time()
                
                for p_id, c_idx, c_data in chunks:
                    chunk_key = (p_id, c_idx)
                    
                    if chunk_key in local_sent_cache and (now - local_sent_cache[chunk_key]) < 25:
                        continue
                        
                    local_sent_cache[chunk_key] = now
                    
                    packet = make_packet(p_id, c_idx, c_data)
                    sock.sendto(packet, target_addr)
                    sent_list.append((now, p_id, c_idx))
                
                if sent_list:
                    cur.executemany(
                        "UPDATE chunks SET last_sent=?, attempts=COALESCE(attempts,0)+1 WHERE payload_id=? AND idx=?",
                        sent_list
                    )
                    conn.commit()
                    send_count += len(sent_list)
                    logger.debug(f"Sent {len(sent_list)} chunks via {local_ip} (total: {send_count})")
                    
                if len(local_sent_cache) > 5000:
                    local_sent_cache = {k: v for k, v in local_sent_cache.items() if now - v < 25}

        except sqlite3.OperationalError as db_err:
            logger.error(f"Database operational error: {db_err}")
        except Exception as e:
            logger.error(f"Worker error: {e}", exc_info=True)
        finally:
            if conn: conn.close()
        time.sleep(0.05)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 sender_worker.py <interface_ip>")
        sys.exit(1)
    
    try:
        run_worker(sys.argv[1])
    except KeyboardInterrupt:
        logging.info("Sender worker stopping via Ctrl+C...")
        sys.exit(0)
    except Exception as e:
        logging.critical(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)