import sqlite3
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import os

DB_PATH = "/usr/local/bin/multilink/sender_coord.db"
REPORTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "modeling_reports")

def get_latest_payload_id():
    """Fetches the most recently registered payload ID from the database."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT payload_id FROM payloads ORDER BY rowid DESC LIMIT 1")
        result = cur.fetchone()
        conn.close()
        return result[0] if result else None
    except Exception as e:
        print(f"Error fetching latest ID: {e}")
        return None

def generate_predictive_report(payload_id):
    if not payload_id:
        print(" No Payload ID provided or found.")
        return

    conn = sqlite3.connect(DB_PATH)
    os.makedirs(REPORTS_DIR, exist_ok=True)
    
    # 1. Fetch the actual history WITH the new Jitter and Loss columns
    query = "SELECT timestamp, uplink_rtt, throughput, jitter, loss_rate, interface_ip FROM interface_metrics_history"
    try:
        df = pd.read_sql_query(query, conn)
    except Exception as e:
        print(f"Database error (Have you deleted the old DB yet?): {e}")
        conn.close()
        return
        
    if df.empty:
        print(" No history data found in interface_metrics_history.")
        conn.close()
        return

    plt.figure(figsize=(12, 7))
    
    print(f"\n{'-'*50}")
    print(f" MODELING ANALYSIS: {payload_id}")
    print(f"{'-'*50}")

    for ip in df['interface_ip'].unique():
        subset = df[df['interface_ip'] == ip].tail(50) 
        
        if len(subset) < 2:
            continue

        # --- MATHEMATICAL MODELING (Linear Regression) ---
        x = subset['timestamp'] - subset['timestamp'].min()
        y_rtt = subset['uplink_rtt']
        
        # RTT Trend
        m, b = np.polyfit(x, y_rtt, 1)
        latest_actual_rtt = y_rtt.iloc[-1]
        predicted_30s_rtt = max(0.1, (m * (x.max() + 30)) + b)
        trend = "UP (Degrading)" if m > 0.01 else "DOWN (Improving)" if m < -0.01 else "STABLE"

        # Calculate Averages for the hidden metrics
        avg_jitter = subset['jitter'].mean()
        avg_loss = subset['loss_rate'].mean()
        avg_tput = subset['throughput'].mean()

        # Interface Name Mapping for cleaner output
        iface_name = "Wi-Fi" if ip == "10.0.1.1" else "5G" if ip == "10.0.2.1" else "Satellite"

        print(f" 🌐 Interface {ip} ({iface_name}):")
        print(f"   - Current RTT:   {latest_actual_rtt:.2f}ms")
        print(f"   - 30s Forecast:  {predicted_30s_rtt:.2f}ms | Trend: {trend} (Slope: {m:.4f})")
        print(f"   - Avg Jitter:    {avg_jitter:.2f}ms")
        print(f"   - Avg Loss:      {avg_loss:.1f}%")
        print(f"   - Avg Throughput:{avg_tput:.2f} Mbps")
        print(f"   --------------------------------------------------")
        
        # Plot Actual Data
        plt.scatter(subset['timestamp'], y_rtt, label=f"Actual: {iface_name}", alpha=0.5, s=15)
        
        # Plot the "Model" (Trendline)
        line_x = np.linspace(x.min(), x.max() + 30, 100)
        plt.plot(line_x + subset['timestamp'].min(), m*line_x + b, '--', label=f"30s Model: {iface_name}")

    plt.title(f"Short-Term Predictive Modeling (30s Window)\nPayload: {payload_id}")
    plt.xlabel("Unix Timestamp (s)")
    plt.ylabel("RTT (ms)")
    plt.legend(loc='upper left', bbox_to_anchor=(1, 1))
    plt.grid(True, linestyle=':', alpha=0.6)
    plt.tight_layout()
    
    output_file = os.path.join(REPORTS_DIR, f"modeling_report_{payload_id[:8]}.png")
    plt.savefig(output_file)
    print(f"\nModeling graph saved as: {output_file}")
    conn.close()

if __name__ == "__main__":
    print("1. Generate report for LATEST payload from database")
    print("2. Enter a specific Payload ID manually")
    choice = input("Select an option (1/2): ").strip()

    if choice == "1":
        target_id = get_latest_payload_id()
        if target_id:
            print(f" Found latest ID: {target_id}")
            generate_predictive_report(target_id)
        else:
            print("No payloads found in database.")
    else:
        target_id = input("Enter Payload UUID: ").strip()
        generate_predictive_report(target_id)