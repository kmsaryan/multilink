import sqlite3
import os
import time
import config   
DB_PATH = config.DB_PATH
SQLITE_TIMEOUT_SECONDS = 30


def get_db_connection(db_path=None, timeout=SQLITE_TIMEOUT_SECONDS):
    """Create a SQLite connection configured for better concurrent access."""
    target_path = db_path or DB_PATH
    db_dir = os.path.dirname(target_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    conn = sqlite3.connect(target_path, timeout=timeout)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute(f"PRAGMA busy_timeout={int(timeout * 1000)};")
    return conn


def ensure_wal_mode(db_path=None):
    """Ensure database is initialized in WAL mode."""
    conn = get_db_connection(db_path=db_path)
    conn.close()


def _ensure_column(conn, table_name, column_def):
    """Add a missing column to an existing table without disturbing old rows."""
    column_name = column_def.split()[0]
    existing_columns = {
        row[1] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    }
    if column_name not in existing_columns:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_def}")

def init_receiver_db():
    """Initializes the receiver database schema."""
    conn = get_db_connection()
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
            sample_count INT,
            completion_rate_pct REAL,
            file_present_rate_pct REAL,
            chunk_to_chunk_time_mean_s REAL,
            chunk_to_chunk_time_variance_s REAL,
            chunk_to_chunk_time_std_s REAL,
            chunk_to_chunk_time_min_s REAL,
            chunk_to_chunk_time_max_s REAL,
            chunk_to_chunk_time_ci95_s REAL,
            file_to_file_time_mean_s REAL,
            file_to_file_time_variance_s REAL,
            file_to_file_time_std_s REAL,
            file_to_file_time_min_s REAL,
            file_to_file_time_max_s REAL,
            file_to_file_time_ci95_s REAL,
            goodput_mean_mbps REAL,
            goodput_variance_mbps REAL,
            goodput_std_mbps REAL,
            goodput_min_mbps REAL,
            goodput_max_mbps REAL,
            goodput_ci95_mbps REAL,
            created_at REAL
        )
    """)

    # Make sure existing databases pick up newly added statistical columns.
    _ensure_column(conn, "scenario_statistics", "sample_count INT")
    _ensure_column(conn, "scenario_statistics", "chunk_to_chunk_time_variance_s REAL")
    _ensure_column(conn, "scenario_statistics", "chunk_to_chunk_time_ci95_s REAL")
    _ensure_column(conn, "scenario_statistics", "file_to_file_time_variance_s REAL")
    _ensure_column(conn, "scenario_statistics", "file_to_file_time_ci95_s REAL")
    _ensure_column(conn, "scenario_statistics", "goodput_variance_mbps REAL")
    _ensure_column(conn, "scenario_statistics", "goodput_ci95_mbps REAL")
    conn.commit()
    conn.close()


def infer_scenario_from_filename(filename):
    """Infers a scenario label from the payload filename."""
    lowered = (filename or "").lower()
    is_link_failure = "link_failure" in lowered or ("link" in lowered and "failure" in lowered)

    if "nlos" in lowered and is_link_failure:
        return "nlos_link_failure"
    if "los" in lowered and is_link_failure:
        return "los_link_failure"
    if "nlos" in lowered:
        return "nlos"
    if "los" in lowered:
        return "los"
    return "unknown"

def register_metadata(pid, filename, total_chunks):
    """
    Saves the original filename and total chunks for a payload.
    This is triggered by Type 4 packets.
    """
    conn = get_db_connection()
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
    conn = get_db_connection()
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
    conn = get_db_connection()
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
    conn = get_db_connection()
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
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        for row in rows:
            cur.execute(
                """
                INSERT INTO scenario_statistics (
                    report_id, scenario, n_runs, sample_count,
                    completion_rate_pct, file_present_rate_pct,
                    chunk_to_chunk_time_mean_s, chunk_to_chunk_time_std_s,
                    chunk_to_chunk_time_variance_s,
                    chunk_to_chunk_time_min_s, chunk_to_chunk_time_max_s,
                    chunk_to_chunk_time_ci95_s,
                    file_to_file_time_mean_s, file_to_file_time_std_s,
                    file_to_file_time_variance_s,
                    file_to_file_time_min_s, file_to_file_time_max_s,
                    file_to_file_time_ci95_s,
                    goodput_mean_mbps, goodput_std_mbps,
                    goodput_variance_mbps,
                    goodput_min_mbps, goodput_max_mbps,
                    goodput_ci95_mbps,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    report_id,
                    row.get("scenario"),
                    row.get("n_runs"),
                    row.get("sample_count", row.get("n_runs")),
                    row.get("completion_rate_pct"),
                    row.get("file_present_rate_pct"),
                    row.get("chunk_to_chunk_time_mean_s"),
                    row.get("chunk_to_chunk_time_std_s"),
                    row.get("chunk_to_chunk_time_variance_s"),
                    row.get("chunk_to_chunk_time_min_s"),
                    row.get("chunk_to_chunk_time_max_s"),
                    row.get("chunk_to_chunk_time_ci95_s"),
                    row.get("file_to_file_time_mean_s"),
                    row.get("file_to_file_time_std_s"),
                    row.get("file_to_file_time_variance_s"),
                    row.get("file_to_file_time_min_s"),
                    row.get("file_to_file_time_max_s"),
                    row.get("file_to_file_time_ci95_s"),
                    row.get("goodput_mean_mbps"),
                    row.get("goodput_std_mbps"),
                    row.get("goodput_variance_mbps"),
                    row.get("goodput_min_mbps"),
                    row.get("goodput_max_mbps"),
                    row.get("goodput_ci95_mbps"),
                    time.time(),
                ),
            )
        conn.commit()
    except sqlite3.Error as e:
        print(f"DB Error in store_scenario_statistics: {e}")
    finally:
        conn.close()


def fetch_scenario_statistics_history(db_path=None, scenario=None):
    conn = get_db_connection(db_path=db_path)
    cur = conn.cursor()

    base_query = """
        SELECT
            scenario,
            n_runs,
            COUNT(*)                                    AS report_count,
            AVG(chunk_to_chunk_time_mean_s)             AS ctc_mean,
            AVG(chunk_to_chunk_time_variance_s)         AS ctc_variance,
            AVG(chunk_to_chunk_time_std_s)              AS ctc_std,
            AVG(chunk_to_chunk_time_ci95_s)             AS ctc_ci95,
            AVG(file_to_file_time_mean_s)               AS ftf_mean,
            AVG(file_to_file_time_variance_s)           AS ftf_variance,
            AVG(file_to_file_time_std_s)                AS ftf_std,
            AVG(file_to_file_time_ci95_s)               AS ftf_ci95,
            AVG(goodput_mean_mbps)                      AS gp_mean,
            AVG(goodput_variance_mbps)                  AS gp_variance,
            AVG(goodput_std_mbps)                       AS gp_std,
            AVG(goodput_ci95_mbps)                      AS gp_ci95,
            AVG(completion_rate_pct)                    AS completion_rate,
            AVG(file_present_rate_pct)                  AS file_present_rate
        FROM scenario_statistics
        WHERE chunk_to_chunk_time_mean_s IS NOT NULL
    """

    if scenario:
        base_query += " AND scenario = ?"
        base_query += " GROUP BY scenario, n_runs ORDER BY scenario ASC, n_runs ASC"
        rows = cur.execute(base_query, (scenario,)).fetchall()
    else:
        base_query += " GROUP BY scenario, n_runs ORDER BY scenario ASC, n_runs ASC"
        rows = cur.execute(base_query).fetchall()

    conn.close()

    return [
        {
            "scenario": row[0],
            "n_runs": int(row[1]),
            "report_count": int(row[2]),
            "ctc_mean": row[3],
            "ctc_variance": row[4],
            "ctc_std": row[5],
            "ctc_ci95": row[6],
            "ftf_mean": row[7],
            "ftf_variance": row[8],
            "ftf_std": row[9],
            "ftf_ci95": row[10],
            "gp_mean": row[11],
            "gp_variance": row[12],
            "gp_std": row[13],
            "gp_ci95": row[14],
            "completion_rate": row[15],
            "file_present_rate": row[16],
        }
        for row in rows
    ]