#!/usr/bin/python3
import os
import uuid
import shutil
import time
import hashlib
import sqlite3
from db_utils import init_sender_db, get_conn
from config import DB_PATH, CHUNK_SIZE, PAYLOAD_DIR

def register_payload(filepath):
    """
    Reads a file, splits it into chunks, and stores them as BLOBs 
    using a single batch transaction for maximum speed.
    """
    if not os.path.exists(filepath):
        print(f"❌ Error: File {filepath} not found.")
        return None

    fname = os.path.basename(filepath)
    size = os.path.getsize(filepath)
    total_chunks = (size + CHUNK_SIZE - 1) // CHUNK_SIZE
    
    # Generate Payload UUID
    payload_id = str(uuid.uuid4())

    print(f"📦 Processing {fname} ({size} bytes)...")

    # Supervisor Question: Why an intermediary file?
    # Answer: Fault Tolerance. If the system loses power during transmission, 
    # the original data is preserved locally and mapped to the UUID.
    new_filename = os.path.join(PAYLOAD_DIR, f"{payload_id}.bin")
    shutil.copy2(filepath, new_filename)

    init_sender_db(DB_PATH)

    chunk_list = []
    start_time = time.perf_counter()

    with open(filepath, "rb") as f:
        for i in range(total_chunks):
            chunk_data = f.read(CHUNK_SIZE)
            chunk_hash = hashlib.sha256(chunk_data).hexdigest()
            
            # (payload_id, idx, state, last_sent, assigned_interface, data, hash)
            chunk_list.append((
                payload_id, 
                i, 
                'pending', 
                None, 
                None, 
                chunk_data, 
                chunk_hash
            ))

    # Single Database Transaction (ACID Compliance)
    conn = get_conn(DB_PATH)
    cur = conn.cursor()
    try:
        # 1. Insert Metadata
        cur.execute("INSERT INTO payloads VALUES (?,?,?,?,?)", 
                    (payload_id, fname, total_chunks, 'queued', 0))
        
        # 2. Batch Insert Blobs (Fastest method)
        cur.executemany(
            """INSERT INTO chunks (payload_id, idx, state, last_sent, assigned_interface, data, hash) 
               VALUES (?, ?, ?, ?, ?, ?, ?)""", 
            chunk_list
        )
        
        conn.commit()
        end_time = time.perf_counter()
        
        print(f"✅ Registered {total_chunks} chunks in {end_time - start_time:.4f} seconds.")
        print(f"🆔 Payload ID: {payload_id}")
        
    except sqlite3.Error as e:
        print(f"❌ Database Error: {e}")
        conn.rollback()
    finally:
        conn.close()

    return payload_id

def monitor_folder(folder_path):
    """Continuously monitor folder for new files"""
    print(f"👀 Monitoring {folder_path} for new files...")
    processed_files = set()
    
    while True:
        try:
            # Look for new files. 
            # We explicitly ignore '.bin' files because those are our fault-tolerance backups.
            files = [f for f in os.listdir(folder_path) if not f.endswith('.bin')]
            
            for file in files:
                filepath = os.path.join(folder_path, file)
                if file not in processed_files:
                    print(f"🆕 New file detected: {file}")
                    register_payload(filepath)
                    processed_files.add(file)
        except Exception as e:
            print(f"Monitor error: {e}")
        
        time.sleep(2)

if __name__ == "__main__":
    os.makedirs(PAYLOAD_DIR, exist_ok=True)
    
    mode = input("Enter 'single' for one file or 'monitor' for folder mode: ").strip().lower()
    
    if mode == 'single':
        test_file = input("Enter file path: ").strip()
        register_payload(test_file)
    else:
        monitor_folder(PAYLOAD_DIR)