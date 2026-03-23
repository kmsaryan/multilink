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
    conn.commit()
    conn.close()

def register_metadata(pid, filename, total_chunks):
    """
    Saves the original filename and total chunks for a payload.
    This is triggered by Type 4 packets.
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT OR REPLACE INTO file_map (payload_id, filename, total_chunks, metadata_arrived_time) 
            VALUES (?, ?, ?, ?)
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
        
        # Increment the count of received chunks for this file
        cur.execute("""
            UPDATE file_map 
            SET received_chunks = received_chunks + 1 
            WHERE payload_id = ?
        """, (pid,))
        
        conn.commit()
    except sqlite3.Error as e:
        print(f"DB Error in register_arrival: {e}")

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
    finally:
        conn.close()