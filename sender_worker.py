#!/usr/bin/python3
import socket as socklib
import struct
import uuid
import time
import sys
import os
import sqlite3
import threading
from db_utils import get_conn
from config import DB_PATH, RECEIVER_IP, RECEIVER_PORT

def get_port_for_interface(interface_ip):
    """Map interface IP to specific port on VM2."""
    ports = {"10.0.1.1": 5001, "10.0.2.1": 5002, "10.0.3.1": 5003}
    return ports.get(interface_ip, RECEIVER_PORT)

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

def receive_acks(sock):
    """Background thread to handle incoming UDP ACKs."""
    # Give the socket a large buffer so it doesn't drop ACKs during bursts
    sock.setsockopt(socklib.SOL_SOCKET, socklib.SO_RCVBUF, 1024 * 1024)
    while True:
        try:
            ack, _ = sock.recvfrom(1024)
            if ack: forward_ack_to_orchestrator(ack)
        except: break

def send_metadata_packet(sock, payload_id, target_addr):
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
        print(f"📄 Metadata Sent for {fname} ({total} chunks)")

def run_worker(local_ip, fixed_port):
    sock = socklib.socket(socklib.AF_INET, socklib.SOCK_DGRAM)
    sock.bind((local_ip, fixed_port))
    receiver_port = get_port_for_interface(local_ip)
    target_addr = (RECEIVER_IP, receiver_port)
    
    print(f"🚀 Worker {local_ip} active. Target: {RECEIVER_IP}:{receiver_port}")
    threading.Thread(target=receive_acks, args=(sock,), daemon=True).start()

    sent_metadata_cache = set()
    
    # NEW: Short-term memory to prevent the worker from spamming the network
    local_sent_cache = {} 

    while True:
        conn = None
        try:
            conn = get_conn(DB_PATH)
            cur = conn.cursor()
            
            # 1. Fetch chunks. Removed "last_sent IS NULL" so we can actually see Orchestrator assignments
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
                    send_metadata_packet(sock, current_pid, target_addr)
                    sent_metadata_cache.add(current_pid)

                sent_list = []
                now = time.time()
                
                for p_id, c_idx, c_data in chunks:
                    chunk_key = (p_id, c_idx)
                    
                    # 2. Prevent Retransmission Storm:
                    # If we sent this exact chunk less than 25 seconds ago, skip it.
                    if chunk_key in local_sent_cache and (now - local_sent_cache[chunk_key]) < 25:
                        continue
                        
                    # Mark it as sent in our local memory
                    local_sent_cache[chunk_key] = now
                    
                    packet = make_packet(p_id, c_idx, c_data)
                    sock.sendto(packet, target_addr)
                    sent_list.append((now, p_id, c_idx))
                
                # 3. Only update DB if we actually sent something
                if sent_list:
                    cur.executemany(
                        "UPDATE chunks SET last_sent=?, attempts=COALESCE(attempts,0)+1 WHERE payload_id=? AND idx=?",
                        sent_list
                    )
                    conn.commit()
                    print(f"📤 Sent {len(sent_list)} chunks via {local_ip}")
                    
                # 4. Memory management: clean up old cache entries so we don't run out of RAM
                if len(local_sent_cache) > 5000:
                    local_sent_cache = {k: v for k, v in local_sent_cache.items() if now - v < 25}

        except sqlite3.OperationalError:
            pass 
        except Exception as e:
            print(f"Worker Error: {e}")
        finally:
            if conn: conn.close()
        time.sleep(0.05)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(1)
    run_worker(sys.argv[1], int(sys.argv[2]))