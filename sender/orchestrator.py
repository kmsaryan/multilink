#!/usr/bin/env python3
import time
import sqlite3
import sys
import threading
import socket as socklib
import os
import uuid
import struct
import logging
from config import DB_PATH
from db_utils import get_conn, mark_acked

# Setup logging
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

def setup_orchestrator_logger():
    logger = logging.getLogger('orchestrator')
    logger.setLevel(logging.DEBUG)
    
    log_file = os.path.join(LOG_DIR, 'orchestrator.log')
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    
    formatter = logging.Formatter('[%(asctime)s] [%(levelname)-8s] %(message)s', 
                                  datefmt='%Y-%m-%d %H:%M:%S')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

orch_logger = setup_orchestrator_logger()

def handle_retransmissions():
    conn = get_conn(DB_PATH)
    cur = conn.cursor()

    # Derive timeout from live interface RTT stored by health_checker
    cur.execute("SELECT MAX(avg_rtt) FROM interface_stats WHERE performance_score > 0")
    row = cur.fetchone()
    max_rtt_ms = row[0] if row and row[0] else 500.0  # fallback 500ms if no data yet

    # 10x worst-case RTT, converted ms -> seconds, clamped to [3s, 15s]
    adaptive_timeout = max(3.0, min(15.0, (max_rtt_ms / 1000.0) * 10))

    cur.execute(
        """
        UPDATE chunks 
        SET state='pending', last_sent=NULL 
        WHERE state='sending' AND (last_sent IS NULL OR last_sent < ?)
        """,
        (time.time() - adaptive_timeout,),
    )
    if cur.rowcount > 0:
        orch_logger.warning(
            f"NETWORK TIMEOUT: Reset {cur.rowcount} timed-out chunks to pending "
            f"(adaptive timeout={adaptive_timeout:.1f}s from max_rtt={max_rtt_ms:.0f}ms)"
        )
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
                ON s.interface_ip = p.interface_ip AND (? - p.timestamp) < 15
            WHERE s.performance_score > 0
            ORDER BY final_score DESC
        """
        cur.execute(query, (time.time(),))
        interfaces = cur.fetchall()
        conn.close()
        return interfaces

    def pick_next_chunks(self, limit=300):
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
        update_batch = []
        t_now = time.time()

        for iface_ip, weight in interface_weights:
            num_to_assign = max(1, int(weight * len(chunks)))
            if num_to_assign > 0:
                print(f"   -> Assigning {num_to_assign} chunks to {iface_ip} (Weight: {weight*100:.1f}%)")

            for _ in range(num_to_assign):
                if chunk_idx >= len(chunks):
                    break
                p_id, c_idx, _ = chunks[chunk_idx]
                update_batch.append((iface_ip, t_now, p_id, c_idx))
                chunk_idx += 1

        if update_batch:
            cur.executemany(
                "UPDATE chunks SET state='sending', assigned_interface=?, last_sent=? WHERE payload_id=? AND idx=?",
                update_batch
            )

        conn.commit()
        
        try:
            cur.execute("DELETE FROM interface_metrics_history WHERE timestamp < ?", (time.time() - 300,))
            conn.commit()
        except:
            pass
        conn.close()

    def run(self):
        orch_logger.info("Orchestrator main loop started (Lightweight Mode).")
        loop_count = 0
        while True:
            loop_count += 1
            try:
                interfaces = self.read_interface_scores()

                if not interfaces:
                    orch_logger.warning("No healthy interfaces found in DB!")
                    time.sleep(0.5)
                    continue

                conn = get_conn(self.db_path)
                cur = conn.cursor()
                cur.execute("SELECT state, COUNT(*) FROM chunks GROUP BY state")
                stats = cur.fetchall()
                if stats:
                    stats_dict = dict(stats)
                    orch_logger.debug(f"Queue Status: {stats_dict}")
                    print(f" Queue Status: {stats_dict}")
                conn.close()

                chunks = self.pick_next_chunks()
                if chunks:
                    orch_logger.debug(f"Orchestrator processing {len(chunks)} chunks...")
                    print(f"Orchestrator processing {len(chunks)} chunks...")
                    self.assign_chunks_to_interfaces(chunks, interfaces)
                
                handle_retransmissions()
                time.sleep(0.1 if chunks else 0.5)
            except Exception as e:
                orch_logger.error(f"Orchestrator loop error: {e}", exc_info=True)
                time.sleep(1)

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
    orch_logger.info("ACK Handler thread started.")
    ack_count = 0
    error_count = 0
    ack_buffer = []          # (payload_id, idx) pairs pending DB write
    last_flush = time.time()
    FLUSH_SIZE = 50          # write when buffer reaches this many ACKs
    FLUSH_INTERVAL = 0.1     # or at least every 100ms

    while True:
        try:
            unix_sock.settimeout(0.05)
            try:
                ack_data, _ = unix_sock.recvfrom(1024)
                p_id, c_idx = parse_ack(ack_data)
                if p_id and c_idx is not None:
                    ack_buffer.append((p_id, c_idx))
                    ack_count += 1
                else:
                    error_count += 1
                    if error_count % 100 == 0:
                        orch_logger.debug(f"ACK Handler: {error_count} malformed ACKs received")
            except (BlockingIOError, TimeoutError):
                pass  # no ACK available right now, fall through to flush check

            now = time.time()
            if ack_buffer and (len(ack_buffer) >= FLUSH_SIZE or (now - last_flush) >= FLUSH_INTERVAL):
                try:
                    conn = get_conn(DB_PATH)
                    cur = conn.cursor()
                    cur.executemany(
                        "UPDATE chunks SET state='acked' WHERE payload_id=? AND idx=?",
                        ack_buffer
                    )
                    conn.commit()
                    conn.close()
                    if ack_count % 1000 == 0:
                        orch_logger.info(f"ACK Handler: Processed {ack_count} acknowledgments")
                except Exception as db_err:
                    orch_logger.error(f"ACK batch flush error: {db_err}")
                ack_buffer.clear()
                last_flush = now

        except Exception as e:
            orch_logger.debug(f"ACK handler exception (non-critical): {type(e).__name__}")
            
if __name__ == "__main__":
    u_sock = setup_unix_socket()
    orch_logger.info("Unix socket established for ACK communication")
    threading.Thread(target=handle_acks, args=(u_sock,), daemon=True).start()
    orch = Orchestrator(DB_PATH)
    try:
        orch.run()
    except KeyboardInterrupt:
        orch_logger.info("Orchestrator stopping via Ctrl+C...")
        sys.exit(0)
    except Exception as e:
        orch_logger.critical(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)