import sqlite3, time, os
import numpy as np

def get_conn(db_path):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30) # Increase timeout to 30s
    # Enable WAL mode for every connection to be safe
    conn.execute("PRAGMA journal_mode=WAL;")
    # Ensure foreign keys are on if you use them
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn
    
def get_conn_with_lock(db_path):
    """Get a database connection with locking to prevent race conditions."""
    conn = sqlite3.connect(db_path, timeout=10)
    conn.execute("BEGIN IMMEDIATE")  # Lock the database for atomic operations
    return conn

def init_sender_db(db_path):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = get_conn(db_path)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS payloads (
            payload_id TEXT PRIMARY KEY,
            filename TEXT,
            total_chunks INT,
            status TEXT,
            next_seq INT DEFAULT 0
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS chunks (
            payload_id TEXT,
            idx INT,
            state TEXT,
            last_sent REAL,
            assigned_interface TEXT,
            data BLOB,
            hash TEXT,
            attempts INT DEFAULT 0,
            PRIMARY KEY (payload_id, idx)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS interface_stats (
            interface_ip TEXT PRIMARY KEY,
            success_rate REAL,
            avg_rtt REAL,
            last_check REAL,
            performance_score REAL DEFAULT 1.0,
            jitter REAL DEFAULT 0.0,
            loss_rate REAL DEFAULT 0.0
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS interface_metrics_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            interface_ip TEXT,
            timestamp REAL,
            uplink_rtt REAL,
            throughput REAL,
            jitter REAL,
            loss_rate REAL
        )
    """)
    conn.commit()
    conn.close()

def mark_acked(db_path, payload_id, idx):
    """Mark chunk as acknowledged"""
    conn = get_conn(db_path)
    cur = conn.cursor()
    cur.execute("UPDATE chunks SET state='acked' WHERE payload_id=? AND idx=?", (payload_id, idx))
    conn.commit(); conn.close()

def update_interface_health(db_path, interface_ip, rtt, throughput, jitter, loss_rate):
    """
    Update interface health metrics in the database.

    Args:
        db_path (str): Path to the SQLite database.
        interface_ip (str): IP address of the interface.
        rtt (float): Round-trip time in milliseconds.
        throughput (float): Throughput in Mbps.
        jitter (float): Jitter in milliseconds.
        loss_rate (float): Packet loss rate (0.0 to 1.0).
    """
    conn = get_conn(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO interface_stats (
            interface_ip, success_rate, avg_rtt, last_check, performance_score, jitter, loss_rate
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            interface_ip,
            throughput,
            rtt,
            time.time(),
            throughput / (rtt + 0.001),
            jitter,
            loss_rate,
        ),
    )
    conn.commit()
    conn.close()
