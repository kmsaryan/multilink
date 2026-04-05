import sqlite3
import os
import time
import config   
DB_PATH = config.DB_PATH

def init_receiver_db():
    """Initializes the receiver database schema."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    # Table to map UUIDs to real filenames
    cur.execute("""
        CREATE TABLE IF NOT EXISTS file_map (
            payload_id TEXT PRIMARY KEY,
            filename TEXT,
            total_chunks INT,
            received_chunks INT DEFAULT 0,
            status TEXT DEFAULT 'receiving',
            metadata_arrived_time REAL,
            completion_time REAL
        )
    """)
    # Table to log every packet arrival for plotting
    cur.execute("""
        CREATE TABLE IF NOT EXISTS arrival_logs (
            payload_id TEXT,
            chunk_idx INT,
            arrival_time REAL,
            source_ip TEXT,
            size INT
        )
    """)

    # Persistent per-run experiment history
    cur.execute("""
        CREATE TABLE IF NOT EXISTS run_statistics (
            payload_id TEXT PRIMARY KEY,
            report_id TEXT,
            filename TEXT,
            scenario TEXT,
            status TEXT,
            total_chunks INT,
            received_chunks INT,
            completion_ratio REAL,
            chunk_to_chunk_time_s REAL,
            file_to_file_time_s REAL,
            goodput_mbps REAL,
            metadata_arrived_time REAL,
            first_arrival_time REAL,
            last_arrival_time REAL,
            completion_time REAL,
            receiver_sha256 TEXT,
            file_present INT,
            updated_at REAL
        )
    """)

    # Append-only scenario summary snapshots for each report execution
    cur.execute("""
        CREATE TABLE IF NOT EXISTS scenario_statistics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_id TEXT,
            scenario TEXT,
            n_runs INT,
            completion_rate_pct REAL,
            file_present_rate_pct REAL,
            chunk_to_chunk_time_mean_s REAL,
            chunk_to_chunk_time_std_s REAL,
            chunk_to_chunk_time_min_s REAL,
            chunk_to_chunk_time_max_s REAL,
            file_to_file_time_mean_s REAL,
            file_to_file_time_std_s REAL,
            file_to_file_time_min_s REAL,
            file_to_file_time_max_s REAL,
            goodput_mean_mbps REAL,
            goodput_std_mbps REAL,
            goodput_min_mbps REAL,
            goodput_max_mbps REAL,
            created_at REAL
        )
    """)
    conn.commit()
    conn.close()


def infer_scenario_from_filename(filename):
    """Infers a scenario label from the payload filename."""
    lowered = (filename or "").lower()
    if "los_link_failure" in lowered:
        return "LOS_LINK_FAILURE"
    if "nlos_link_failure" in lowered:
        return "NLOS_LINK_FAILURE"
    if "los" in lowered and "nlos" not in lowered:
        return "LOS"
    if "nlos" in lowered:
        return "NLOS"
    if "link_failure" in lowered:
        return "LINK_FAILURE"
    return "UNKNOWN"

def register_metadata(pid, filename, total_chunks):
    """
    Saves the original filename and total chunks for a payload.
    This is triggered by Type 4 packets.
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO file_map (payload_id, filename, total_chunks, metadata_arrived_time)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(payload_id) DO UPDATE SET
                filename = excluded.filename,
                total_chunks = excluded.total_chunks,
                metadata_arrived_time = COALESCE(file_map.metadata_arrived_time, excluded.metadata_arrived_time)
        """, (pid, filename, total_chunks, time.time()))
        conn.commit()
    except sqlite3.Error as e:
        print(f"DB Error in register_metadata: {e}")
    finally:
        conn.close()

def register_arrival(pid, idx, ip, size):
    """Logs the arrival of a specific data chunk."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        # Log the timestamped arrival
        cur.execute("INSERT INTO arrival_logs VALUES (?, ?, ?, ?, ?)", 
                    (pid, idx, time.time(), ip, size))
        
        # Keep received_chunks as UNIQUE chunk count (retransmissions are logged
        # in arrival_logs but do not inflate completion progress).
        cur.execute("""
            UPDATE file_map 
            SET received_chunks = (
                SELECT COUNT(DISTINCT chunk_idx)
                FROM arrival_logs
                WHERE payload_id = ?
            )
            WHERE payload_id = ?
        """, (pid, pid))
        
        conn.commit()
    except sqlite3.Error as e:
        print(f"DB Error in register_arrival: {e}")
    finally:
        conn.close()

def mark_transfer_complete(pid):
    """Records completion timestamp when all chunks are received."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE file_map 
            SET completion_time = ?, status = 'completed'
            WHERE payload_id = ? AND status = 'receiving'
        """, (time.time(), pid))
        conn.commit()
    except sqlite3.Error as e:
        print(f"DB Error in mark_transfer_complete: {e}")
    finally:
        conn.close()


def store_run_statistics(
    payload_id,
    report_id,
    filename,
    scenario,
    status,
    total_chunks,
    received_chunks,
    completion_ratio,
    chunk_to_chunk_time_s,
    file_to_file_time_s,
    goodput_mbps,
    metadata_arrived_time,
    first_arrival_time,
    last_arrival_time,
    completion_time,
    receiver_sha256,
    file_present,
):
    """Upserts one persistent per-run row in run_statistics."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO run_statistics (
                payload_id, report_id, filename, scenario, status,
                total_chunks, received_chunks, completion_ratio,
                chunk_to_chunk_time_s, file_to_file_time_s, goodput_mbps,
                metadata_arrived_time, first_arrival_time, last_arrival_time,
                completion_time, receiver_sha256, file_present, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(payload_id) DO UPDATE SET
                report_id = excluded.report_id,
                filename = excluded.filename,
                scenario = excluded.scenario,
                status = excluded.status,
                total_chunks = excluded.total_chunks,
                received_chunks = excluded.received_chunks,
                completion_ratio = excluded.completion_ratio,
                chunk_to_chunk_time_s = excluded.chunk_to_chunk_time_s,
                file_to_file_time_s = excluded.file_to_file_time_s,
                goodput_mbps = excluded.goodput_mbps,
                metadata_arrived_time = excluded.metadata_arrived_time,
                first_arrival_time = excluded.first_arrival_time,
                last_arrival_time = excluded.last_arrival_time,
                completion_time = excluded.completion_time,
                receiver_sha256 = excluded.receiver_sha256,
                file_present = excluded.file_present,
                updated_at = excluded.updated_at
            """,
            (
                payload_id,
                report_id,
                filename,
                scenario,
                status,
                total_chunks,
                received_chunks,
                completion_ratio,
                chunk_to_chunk_time_s,
                file_to_file_time_s,
                goodput_mbps,
                metadata_arrived_time,
                first_arrival_time,
                last_arrival_time,
                completion_time,
                receiver_sha256,
                file_present,
                time.time(),
            ),
        )
        conn.commit()
    except sqlite3.Error as e:
        print(f"DB Error in store_run_statistics: {e}")
    finally:
        conn.close()


def store_scenario_statistics(report_id, rows):
    """Appends scenario-level snapshots into scenario_statistics."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        for row in rows:
            cur.execute(
                """
                INSERT INTO scenario_statistics (
                    report_id, scenario, n_runs,
                    completion_rate_pct, file_present_rate_pct,
                    chunk_to_chunk_time_mean_s, chunk_to_chunk_time_std_s,
                    chunk_to_chunk_time_min_s, chunk_to_chunk_time_max_s,
                    file_to_file_time_mean_s, file_to_file_time_std_s,
                    file_to_file_time_min_s, file_to_file_time_max_s,
                    goodput_mean_mbps, goodput_std_mbps,
                    goodput_min_mbps, goodput_max_mbps,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    report_id,
                    row.get("scenario"),
                    row.get("n_runs"),
                    row.get("completion_rate_pct"),
                    row.get("file_present_rate_pct"),
                    row.get("chunk_to_chunk_time_mean_s"),
                    row.get("chunk_to_chunk_time_std_s"),
                    row.get("chunk_to_chunk_time_min_s"),
                    row.get("chunk_to_chunk_time_max_s"),
                    row.get("file_to_file_time_mean_s"),
                    row.get("file_to_file_time_std_s"),
                    row.get("file_to_file_time_min_s"),
                    row.get("file_to_file_time_max_s"),
                    row.get("goodput_mean_mbps"),
                    row.get("goodput_std_mbps"),
                    row.get("goodput_min_mbps"),
                    row.get("goodput_max_mbps"),
                    time.time(),
                ),
            )
        conn.commit()
    except sqlite3.Error as e:
        print(f"DB Error in store_scenario_statistics: {e}")
    finally:
        conn.close()