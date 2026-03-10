#!/usr/bin/python3
import time
import sqlite3
import numpy as np
from db_utils import get_conn
from config import DB_PATH

def init_prediction_db():
    """Creates a dedicated table for the active predictions."""
    conn = get_conn(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS interface_predictions (
            interface_ip TEXT PRIMARY KEY,
            predicted_rtt REAL,
            predicted_bitrate REAL,
            avg_jitter REAL,
            avg_loss REAL,
            blended_score REAL,
            timestamp REAL
        )
    """)
    conn.commit()
    conn.close()

def get_historical_metrics(interface_ip, window_seconds=60):
    conn = get_conn(DB_PATH)
    cur = conn.cursor()
    # Instant_bitrate metric
    cur.execute("""
        SELECT timestamp, uplink_rtt, instant_bitrate, jitter, loss_rate 
        FROM interface_metrics_history 
        WHERE interface_ip = ? AND timestamp > ?
        ORDER BY timestamp ASC
    """, (interface_ip, time.time() - window_seconds))
    results = cur.fetchall()
    conn.close()
    return results

def update_prediction(ip, p_rtt, p_bitrate, a_jitter, a_loss, score):
    conn = get_conn(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO interface_predictions 
        (interface_ip, predicted_rtt, predicted_bitrate, avg_jitter, avg_loss, blended_score, timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (ip, p_rtt, p_bitrate, a_jitter, a_loss, score, time.time()))
    conn.commit()
    conn.close()

def monitor_predictions():
    init_prediction_db()
    print("Predictor Daemon Started. ")
    interfaces = ['10.0.1.1', '10.0.2.1', '10.0.3.1']
    
    while True:
        for ip in interfaces:
            try:
                history = get_historical_metrics(ip)
                if len(history) < 10: 
                    continue                
                # Prepare arrays
                times = np.array([row[0] for row in history])
                rtts = np.array([row[1] for row in history])
                bitrates = np.array([row[2] for row in history])
                jitters = [row[3] for row in history if row[3] is not None]
                losses = [row[4] for row in history if row[4] is not None]
                t0 = times[0]
                x = times - t0
                target_x = (time.time() - t0) + 10 
                
                # 1. Linear Regression (10s Future)
                rtt_slope, rtt_intercept = np.polyfit(x, rtts, 1)
                pred_rtt = max(0.1, (rtt_slope * target_x) + rtt_intercept)                
                bitrate_slope, br_intercept = np.polyfit(x, bitrates, 1)
                pred_bitrate = max(0.0, (bitrate_slope * target_x) + br_intercept)                
                avg_jitter = sum(jitters) / len(jitters) if jitters else 0.0
                avg_loss = sum(losses) / len(losses) if losses else 0.0
                
                # 2. Score Calculation & Penalties
                base_score = pred_bitrate / (pred_rtt + 0.001)
                jitter_penalty = max(0.5, 1.0 - (avg_jitter / 40.0))
                loss_penalty = max(0.1, 1.0 - (avg_loss / 100.0))
                future_score = base_score * jitter_penalty * loss_penalty
                current_rtt = rtts[-1]
                current_bitrate = bitrates[-1]
                current_score = current_bitrate / (current_rtt + 0.001)
                
                # 3. Blended Score
                blended_score = (current_score * 0.4) + (future_score * 0.6)
                update_prediction(ip, pred_rtt, pred_bitrate, avg_jitter, avg_loss, blended_score)
                print(f" [PREDICT] {ip} | F-RTT: {pred_rtt:.1f}ms | Loss: {avg_loss:.1f}% | Score: {blended_score:.2f}")
            except Exception as e:
                pass
        time.sleep(0.5)

if __name__ == "__main__":
    try:
        monitor_predictions()
    except KeyboardInterrupt:
        print("\nStopping Predictor Daemon...")
