#!/usr/bin/env python3
import sqlite3
import pandas as pd
import os
import matplotlib.pyplot as plt
import config

DB_PATH = config.DB_PATH
RESULTS_DIR = config.RESULTS_DIR

def generate_report():
    if not os.path.exists(DB_PATH):
        print(f"Database not found at {DB_PATH}")
        return

    os.makedirs(RESULTS_DIR, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    
    # 1. Get the latest file info
    file_info = conn.execute("""
        SELECT payload_id, filename, total_chunks, received_chunks, metadata_arrived_time, completion_time 
        FROM file_map 
        ORDER BY rowid DESC LIMIT 1
    """).fetchone()

    if not file_info:
        print(" No data found in file_map table.")
        conn.close()
        return

    pid, fname, total, received, metadata_time, completion_time = file_info
    print(f"\n{'-'*60}")
    print(f"REPORT FOR: {fname}")
    print(f" UUID: {pid}")
    print(f" Progress: {received}/{total} chunks ({(received/total)*100:.1f}%)")
    print(f"{'-'*60}")

    # --- TOTAL TRANSFER DURATION ---
    if metadata_time and completion_time:
        total_duration = completion_time - metadata_time
        print(f"\nTRANSFER TIMELINE")
        print(f" Total transfer time (metadata → completion): {total_duration:.3f} seconds")
        print(f" File size: {received * 1200 / (1024*1024):.2f} MB")
        overall_mbps = (received * 1200 * 8) / (total_duration * 1000000) if total_duration > 0 else 0
        print(f" Overall throughput: {overall_mbps:.2f} Mbps")
        print("")

    # 2. Detailed Interface Analysis
    query = """
        SELECT 
            source_ip, 
            COUNT(chunk_idx) as chunks, 
            MIN(arrival_time) as start_t, 
            MAX(arrival_time) as end_t
        FROM arrival_logs 
        WHERE payload_id = ? 
        GROUP BY source_ip
    """
    df = pd.read_sql_query(query, conn, params=(pid,))
        print(f"PER-INTERFACE DATA TRANSFER ANALYSIS")
    
    if df.empty:
        print(" No arrival logs found for this UUID.")
    else:
        for _, row in df.iterrows():
            duration = row['end_t'] - row['start_t']
            # Avoid division by zero for very fast transfers
            duration = max(duration, 0.001) 
            
            # Math: (Chunks * 1200 bytes * 8 bits) / (duration * 1,000,000)
            mbps = (row['chunks'] * 1200 * 8) / (duration * 1000000)
            
            # Map IPs to readable names for your report
            ip = row['source_ip']
            iface_map = {
                "90.27.22.2": "Wi-Fi",
                "90.27.22.3": "5G",
                "90.27.22.4": "Satellite",
            }
            iface_name = iface_map.get(ip, "Unknown")
            
            print(f"Interface: {row['source_ip']} ({iface_name})")
            print(f"   - Chunks Received: {row['chunks']}")
            print(f"   - Time Window:     {duration:.3f} seconds")
            print(f"   - Avg Goodput:     {mbps:.2f} Mbps")
            print("")

    # 3. Plot Arrival Jitter
    plot_arrival_jitter(pid, conn)

    conn.close()

def plot_arrival_jitter(pid, conn):
    query = f"SELECT chunk_idx, arrival_time, source_ip FROM arrival_logs WHERE payload_id = '{pid}' ORDER BY arrival_time ASC"
    df = pd.read_sql_query(query, conn)
    
    if df.empty:
        return

    plt.figure(figsize=(12, 6))
    for ip in df['source_ip'].unique():
        subset = df[df['source_ip'] == ip]
        # Normalize time to start at 0
        relative_time = subset['arrival_time'] - df['arrival_time'].min()
        plt.scatter(relative_time, subset['chunk_idx'], label=f"Arrival via {ip}", s=10)

    plt.title(f"Packet Arrival Sequence (Goodput Analysis) - {pid[:8]}")
    plt.xlabel("Time since first packet (s)")
    plt.ylabel("Chunk Index")
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.7)
    output_path = os.path.join(RESULTS_DIR, f"arrival_plot_{pid[:8]}.png")
    plt.savefig(output_path)
    print(f" Arrival graph saved as {output_path}")

if __name__ == "__main__":
    generate_report()