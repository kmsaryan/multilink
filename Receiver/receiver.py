#!/usr/bin/env python3
import socket
import struct
import uuid
import os
import select
import sqlite3
from config import CHUNK_SIZE, RECEIVED_DIR, DATA_PORT, HEALTH_PORT, DB_PATH
from db_utils import register_metadata, register_arrival, init_receiver_db

def run_receiver():
    db_dir = os.path.dirname(DB_PATH)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)
    init_receiver_db()

    sockets = []
    
    try:
        data_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        data_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  
        data_sock.bind(("0.0.0.0", DATA_PORT))
        data_sock.setblocking(False)
        sockets.append(data_sock)
        print(f" Receiver listening for DATA on port: {DATA_PORT}")
        
        health_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        health_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  
        health_sock.bind(("0.0.0.0", HEALTH_PORT))
        health_sock.setblocking(False)
        sockets.append(health_sock)
        print(f" Receiver listening for PROBES on port: {HEALTH_PORT}")
    except Exception as e:
        print(f"Could not bind to ports: {e}")
        return

    os.makedirs(RECEIVED_DIR, exist_ok=True)    
    stats = {}             # Tracks unique chunks received: { 'uuid': set(1, 2, 5...) }
    expected_chunks = {}   # Tracks total needed: { 'uuid': 874 }
    file_names = {}        # Tracks original filename: { 'uuid': 'testfile.data' }
    
    print(" Receiver is active. Waiting for multi-path traffic...")

    while True:
        ready_socks, _, _ = select.select(sockets, [], [], 1.0)

        for s in ready_socks:
            try:
                pkt, addr = s.recvfrom(2048)

                if len(pkt) < 21:
                    continue

                packet_type = pkt[0]

                # --- Handle Health Probes (Type 2) ---
                if packet_type == 2:
                    probe_ack = b'\x02' + b'\x00' * 16 + struct.pack("!I", 0)
                    s.sendto(probe_ack, addr)
                    continue

                # --- Handle Data and Metadata ---
                pid_bytes = pkt[1:17]
                payload_uuid = uuid.UUID(bytes=pid_bytes)
                pid_str = str(payload_uuid)

                # Metadata (Type 4)
                if packet_type == 4:
                    total_chunks = struct.unpack("!I", pkt[17:21])[0]
                    filename = pkt[21:].decode('utf-8')
                    
                    # Store in fast memory cache
                    expected_chunks[pid_str] = total_chunks
                    file_names[pid_str] = filename
                    
                    register_metadata(pid_str, filename, total_chunks)
                    print(f"Metadata Received: {filename} ({total_chunks} chunks) via {addr[0]}")
                    continue

                # Data (Type 0) or Retransmissions (Type 3)
                if packet_type in [0, 3]:
                    chunk_idx = struct.unpack("!I", pkt[17:21])[0]
                    payload_data = pkt[21:]

                    if pid_str not in stats:
                        stats[pid_str] = set()
                        print(f"New payload detected: {pid_str}")

                    file_path = os.path.join(RECEIVED_DIR, f"{pid_str}.bin")
                    mode = "r+b" if os.path.exists(file_path) else "wb"
                    
                    with open(file_path, mode) as f:
                        f.seek(chunk_idx * CHUNK_SIZE)
                        f.write(payload_data)

                    stats[pid_str].add(chunk_idx)
                    current_received = len(stats[pid_str])

                    # --- SEND ACK ---
                    ack_packet = b'\x00' + pid_bytes + struct.pack("!I", chunk_idx)
                    s.sendto(ack_packet, addr)

                    if current_received % 50 == 0:
                        target = expected_chunks.get(pid_str, "?")
                        percent = (current_received / target * 100) if isinstance(target, int) else 0
                        print(f" [{pid_str[:8]}] Progress: {current_received}/{target} chunks ({percent:.1f}%)")

                    register_arrival(pid_str, chunk_idx, addr[0], len(pkt))

                    if pid_str in expected_chunks:
                        if current_received == expected_chunks[pid_str]:
                            original_name = file_names[pid_str]
                            bin_path = os.path.join(RECEIVED_DIR, f"{pid_str}.bin")
                            final_path = os.path.join(RECEIVED_DIR, original_name)

                            if os.path.exists(bin_path):
                                os.rename(bin_path, final_path)
                                print(f" SUCCESS: {original_name} fully reassembled from {current_received} chunks!")

                                try:
                                    conn = sqlite3.connect(DB_PATH)
                                    cur = conn.cursor()
                                    cur.execute("UPDATE file_map SET status='completed' WHERE payload_id=?", (pid_str,))
                                    conn.commit()
                                    conn.close()
                                except Exception as e:
                                    print(f"Failed to update final status in DB: {e}")                                
                                del expected_chunks[pid_str]

            except Exception as e:
                pass

if __name__ == "__main__":
    try:
        run_receiver()
    except KeyboardInterrupt:
        print("\nStopping receiver...")