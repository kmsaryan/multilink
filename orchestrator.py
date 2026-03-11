#!/usr/bin/env python3
import time
import sqlite3
import sys
import threading
import socket as socklib
import os
import uuid
import struct
from config import DB_PATH
from db_utils import get_conn, mark_acked

def handle_retransmissions():
    """Handle retransmissions for chunks that timed out."""
    conn = get_conn(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE chunks 
        SET state='pending', last_sent=NULL 
        WHERE state='sending' AND (last_sent IS NULL OR last_sent < ?)
        """,
        (time.time() - 30,),
    )
    if cur.rowcount > 0:
        print(f" Orchestrator: Reset {cur.rowcount} timed-out chunks to pending.")
    conn.commit()
    conn.close()

class Orchestrator:
    def __init__(self, db_path, specific_interface=None):
        self.db_path = db_path
        self.specific_interface = specific_interface

    def read_interface_scores(self):
        """
        Reads the pre-calculated predictive scores from the database.
        Falls back to the raw health score if the predictor daemon is offline or catching up.
        """
        conn = get_conn(self.db_path)
        cur = conn.cursor()
        
        query = """
            SELECT 
                s.interface_ip, 
                COALESCE(p.blended_score, s.performance_score) as final_score
            FROM interface_stats s
            LEFT JOIN interface_predictions p 
                ON s.interface_ip = p.interface_ip AND (? - p.timestamp) < 5
            WHERE s.performance_score > 0
            ORDER BY final_score DESC
        """
        cur.execute(query, (time.time(),))
        interfaces = cur.fetchall()
        conn.close()
        return interfaces

    def pick_next_chunks(self, limit=50):
        """Pick the next chunks to send."""
        conn = get_conn(self.db_path)
        cur = conn.cursor()
        cur.execute("""
            SELECT payload_id, idx, data FROM chunks
            WHERE state='pending'
            ORDER BY payload_id, idx
            LIMIT ?
        """, (limit,))
        chunks = cur.fetchall()
        conn.close()
        return chunks

    def assign_chunks_to_interfaces(self, chunks, interfaces):
        """Assign chunks purely based on the database-provided scores."""
        if not chunks or not interfaces:
            return

        total_score = sum(score for ip, score in interfaces)
        if total_score <= 0:
            interface_weights = [(ip, 1/len(interfaces)) for ip, _ in interfaces]
        else:
            interface_weights = [(ip, score / total_score) for ip, score in interfaces]

        conn = get_conn(self.db_path)
        cur = conn.cursor()
        chunk_idx = 0
        
        for iface_ip, weight in interface_weights:
            num_to_assign = max(1, int(weight * len(chunks)))
            if num_to_assign > 0:
                print(f"   -> Assigning {num_to_assign} chunks to {iface_ip} (Weight: {weight*100:.1f}%)")
            
            for _ in range(num_to_assign):
                if chunk_idx >= len(chunks):
                    break
                p_id, c_idx, _ = chunks[chunk_idx]
                cur.execute(
                    "UPDATE chunks SET state='sending', assigned_interface=?, last_sent=? WHERE payload_id=? AND idx=?",
                    (iface_ip, time.time(), p_id, c_idx)
                )
                chunk_idx += 1

        conn.commit()
        try:
            cur.execute("DELETE FROM interface_metrics_history WHERE timestamp < ?", (time.time() - 300,))
            conn.commit()
        except:
            pass
        conn.close()

    def run(self):
        print("Orchestrator main loop started (Lightweight Mode).")
        while True:
            interfaces = self.read_interface_scores()

            if not interfaces:
                print("No healthy interfaces found in DB!")
                time.sleep(0.5)
                continue

            conn = get_conn(self.db_path)
            cur = conn.cursor()
            cur.execute("SELECT state, COUNT(*) FROM chunks GROUP BY state")
            stats = cur.fetchall()
            if stats:
                print(f" Queue Status: {dict(stats)}")
            conn.close()

            chunks = self.pick_next_chunks()
            if chunks:
                print(f"Orchestrator processing {len(chunks)} chunks...")
                self.assign_chunks_to_interfaces(chunks, interfaces)
            
            handle_retransmissions()
            time.sleep(0.5)

def setup_unix_socket():
    socket_path = "/tmp/orchestrator.sock"
    if os.path.exists(socket_path):
        os.remove(socket_path)
    sock = socklib.socket(socklib.AF_UNIX, socklib.SOCK_DGRAM)
    sock.bind(socket_path)
    os.chmod(socket_path, 0o666)
    return sock

def parse_ack(ack):
    """
    A valid ACK must be exactly 21 bytes (1 Type + 16 UUID + 4 Index).
    """
    if len(ack) != 21:
        return None, None
        
    try:
        if ack[0] != 0:
            return None, None
            
        pid_bytes = ack[1:17]
        payload_id = str(uuid.UUID(bytes=pid_bytes))
        idx = struct.unpack("!I", ack[17:21])[0]
        return payload_id, idx
    except Exception:
        return None, None

def handle_acks(unix_sock):
    print("ACK Handler thread started.")
    while True:
        try:
            ack_data, _ = unix_sock.recvfrom(1024)
            p_id, c_idx = parse_ack(ack_data)
            if p_id and c_idx is not None:
                mark_acked(DB_PATH, p_id, c_idx)
        except Exception as e:
            pass

if __name__ == "__main__":
    u_sock = setup_unix_socket()
    threading.Thread(target=handle_acks, args=(u_sock,), daemon=True).start()
    orch = Orchestrator(DB_PATH)
    try:
        orch.run()
    except KeyboardInterrupt:
        print("\n Stopping Orchestrator...")