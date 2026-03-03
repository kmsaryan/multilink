#!/usr/bin/env python3
import socket
import struct
import uuid
import os
import time
import select
import sqlite3
from config import CHUNK_SIZE, RECEIVED_DIR, WIFI_PORT, FIVEG_PORT, SATELLITE_PORT
from db_utils import register_metadata, register_arrival, init_receiver_db, DB_PATH


def run_receiver():
    # Ensure the directory for the DB exists
    db_dir = os.path.dirname(DB_PATH)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)

    # Initialize the database schema (creates tables if they don't exist)
    init_receiver_db()

    # 1. Setup Sockets for all interfaces
    listen_ports = [WIFI_PORT, FIVEG_PORT, SATELLITE_PORT]
    sockets = []

    for port in listen_ports:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Good practice for restarts
            s.bind(("0.0.0.0", port))
            s.setblocking(False)
            sockets.append(s)
            print(f"Receiver listening on UDP port: {port}")
        except Exception as e:
            print(f"Could not bind to port {port}: {e}")

    os.makedirs(RECEIVED_DIR, exist_ok=True)

    stats = {}
    print("Receiver is active. Waiting for multi-path data...")

    while True:
        ready_socks, _, _ = select.select(sockets, [], [], 1.0)

        for s in ready_socks:
            try:
                pkt, addr = s.recvfrom(2048)

                if len(pkt) < 21:
                    continue

                packet_type = pkt[0]
                pid_bytes = pkt[1:17]
                payload_uuid = uuid.UUID(bytes=pid_bytes)
                pid_str = str(payload_uuid)

                # --- Handle Metadata (Type 4) ---
                if packet_type == 4:
                    total_chunks = struct.unpack("!I", pkt[17:21])[0]
                    filename = pkt[21:].decode('utf-8')
                    register_metadata(pid_str, filename, total_chunks)
                    print(f"📄 Metadata Received: {filename} ({total_chunks} chunks)")
                    continue

                # --- Handle Data (0) or Retransmissions (3) ---
                if packet_type in [0, 3]:
                    chunk_idx = struct.unpack("!I", pkt[17:21])[0]
                    payload_data = pkt[21:]

                    if pid_str not in stats:
                        stats[pid_str] = set()
                        print(f"New incoming payload detected: {pid_str}")

                    file_path = os.path.join(RECEIVED_DIR, f"{pid_str}.bin")

                    mode = "r+b" if os.path.exists(file_path) else "wb"
                    with open(file_path, mode) as f:
                        f.seek(chunk_idx * CHUNK_SIZE)
                        f.write(payload_data)

                    stats[pid_str].add(chunk_idx)

                    # --- SEND ACK ---
                    ack_packet = b'\x00' + pid_bytes + struct.pack("!I", chunk_idx)
                    s.sendto(ack_packet, addr)

                    if len(stats[pid_str]) % 20 == 0:
                        print(f"[{pid_str[:8]}] Received chunk {chunk_idx}. Total unique: {len(stats[pid_str])}")

                    # Log Arrival for Modeling
                    register_arrival(pid_str, chunk_idx, addr[0], len(pkt))

                    # --- Check Completion ---
                    try:
                        conn = sqlite3.connect(DB_PATH)
                        cur = conn.cursor()
                        # Use a query that won't fail if the table is busy
                        cur.execute("SELECT filename, total_chunks, received_chunks, status FROM file_map WHERE payload_id=?", (pid_str,))
                        row = cur.fetchone()

                        if row and row[3] != 'completed':
                            original_name, total, received, status = row
                            if received >= total:
                                bin_path = os.path.join(RECEIVED_DIR, f"{pid_str}.bin")
                                final_path = os.path.join(RECEIVED_DIR, original_name)

                                if os.path.exists(bin_path):
                                    os.rename(bin_path, final_path)
                                    print(f"🎉 SUCCESS: {original_name} reassembled.")

                                    cur.execute("UPDATE file_map SET status='completed' WHERE payload_id=?", (pid_str,))
                                    conn.commit()
                        conn.close()
                    except sqlite3.OperationalError:
                        pass  # Database locked, will check on next packet

                elif packet_type == 2:
                    probe_ack = b'\x02' + b'\x00' * 16 + struct.pack("!I", 0)
                    s.sendto(probe_ack, addr)

            except Exception as e:
                print(f"Error processing packet: {e}")


if __name__ == "__main__":
    try:
        run_receiver()
    except KeyboardInterrupt:
        print("\nStopping receiver...")