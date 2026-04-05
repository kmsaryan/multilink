import sqlite3, time, os
import numpy as np


def infer_scenario_from_filename(filename):
    """Infer experiment scenario from a payload filename."""
    name = (filename or "").lower()
    is_link_failure = "link" in name and "failure" in name

    if "nlos" in name and is_link_failure:
        return "nlos_link_failure"
    if "los" in name and is_link_failure:
        return "los_link_failure"
    if "nlos" in name:
        return "nlos"
    if "los" in name:
        return "los"
    if is_link_failure:
        return "link_failure"
    return "unknown"

def get_conn(db_path):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30) # Increase timeout to 30s
    # Enable WAL mode for every connection to prevent locking issues
    conn.execute("PRAGMA journal_mode=WAL;")
    # Ensure foreign keys are on
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
    # --- ADDED: instant_bitrate ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS interface_stats (
            interface_ip TEXT PRIMARY KEY,
            success_rate REAL,
            avg_rtt REAL,
            last_check REAL,
            performance_score REAL DEFAULT 1.0,
            jitter REAL DEFAULT 0.0,
            loss_rate REAL DEFAULT 0.0,
            instant_bitrate REAL DEFAULT 0.0 
        )
    """)
    # --- ADDED: instant_bitrate ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS interface_metrics_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            interface_ip TEXT,
            timestamp REAL,
            uplink_rtt REAL,
            throughput REAL,
            jitter REAL,
            loss_rate REAL,
            instant_bitrate REAL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS run_statistics (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            payload_id TEXT UNIQUE,
            filename TEXT,
            scenario TEXT,
            total_chunks_declared INT,
            chunks_rows INT,
            acked_rows INT,
            pending_rows INT,
            sending_rows INT,
            acked_ratio_pct REAL,
            avg_attempts REAL,
            max_attempts INT,
            transfer_time_s REAL,
            first_last_sent REAL,
            last_last_sent REAL,
            updated_at REAL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS scenario_statistics (
            scenario_run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            scenario TEXT,
            sample_count INT,
            mean_transfer_time_s REAL,
            variance_transfer_time_s REAL,
            std_transfer_time_s REAL,
            min_transfer_time_s REAL,
            max_transfer_time_s REAL,
            ci95_half_width_s REAL,
            source_payload_count INT,
            created_at REAL
        )
    """)
    conn.commit()
    conn.close()


def store_run_statistics(
    db_path,
    payload_id,
    filename,
    scenario,
    total_chunks_declared,
    chunks_rows,
    acked_rows,
    pending_rows,
    sending_rows,
    acked_ratio_pct,
    avg_attempts,
    max_attempts,
    transfer_time_s,
    first_last_sent,
    last_last_sent,
):
    """Insert or update the per-payload run statistics row."""
    conn = get_conn(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO run_statistics (
            payload_id, filename, scenario, total_chunks_declared,
            chunks_rows, acked_rows, pending_rows, sending_rows,
            acked_ratio_pct, avg_attempts, max_attempts,
            transfer_time_s, first_last_sent, last_last_sent, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            payload_id,
            filename,
            scenario,
            total_chunks_declared,
            chunks_rows,
            acked_rows,
            pending_rows,
            sending_rows,
            acked_ratio_pct,
            avg_attempts,
            max_attempts,
            transfer_time_s,
            first_last_sent,
            last_last_sent,
            time.time(),
        ),
    )
    conn.commit()
    conn.close()


def store_scenario_statistics(db_path, scenario, transfer_times, source_payload_count):
    """Append a scenario-level snapshot computed from stored run values."""
    if not transfer_times:
        return

    mean_value = float(np.mean(transfer_times))
    variance_value = float(np.var(transfer_times, ddof=1)) if len(transfer_times) >= 2 else None
    std_value = float(np.std(transfer_times, ddof=1)) if len(transfer_times) >= 2 else None
    min_value = float(np.min(transfer_times))
    max_value = float(np.max(transfer_times))
    ci95 = 1.96 * std_value / np.sqrt(len(transfer_times)) if std_value is not None else None

    conn = get_conn(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO scenario_statistics (
            scenario, sample_count, mean_transfer_time_s,
            variance_transfer_time_s, std_transfer_time_s,
            min_transfer_time_s, max_transfer_time_s,
            ci95_half_width_s, source_payload_count, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            scenario,
            len(transfer_times),
            mean_value,
            variance_value,
            std_value,
            min_value,
            max_value,
            ci95,
            source_payload_count,
            time.time(),
        ),
    )
    conn.commit()
    conn.close()

def mark_acked(db_path, payload_id, idx):
    """Mark chunk as acknowledged"""
    conn = get_conn(db_path)
    cur = conn.cursor()
    cur.execute("UPDATE chunks SET state='acked' WHERE payload_id=? AND idx=?", (payload_id, idx))
    conn.commit()
    conn.close()

def update_interface_health(db_path, interface_ip, rtt, throughput, jitter, loss_rate, instant_bitrate=0.0):
    """
    Update interface health metrics in the database.
    """
    conn = get_conn(db_path)
    cur = conn.cursor()
    perf_score = instant_bitrate / (rtt + 0.001)

    cur.execute(
        """
        INSERT OR REPLACE INTO interface_stats (
            interface_ip, success_rate, avg_rtt, last_check, performance_score, jitter, loss_rate, instant_bitrate
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            interface_ip,
            throughput,       # This is now raw bits per second (bps)
            rtt,
            time.time(),
            perf_score,
            jitter,
            loss_rate,
            instant_bitrate   # The new metric
        ),
    )
    conn.commit()
    conn.close()