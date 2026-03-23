#!/usr/bin/python3
import time
import sqlite3
import numpy as np
import logging
import os
import sys
from db_utils import get_conn
from config import DB_PATH

# Setup logging
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

def setup_prediction_logger():
    logger = logging.getLogger('prediction_monitor')
    logger.setLevel(logging.DEBUG)
    
    log_file = os.path.join(LOG_DIR, 'prediction_monitor.log')
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

pred_logger = setup_prediction_logger()

HISTORY_WINDOW_SECONDS = 120
MIN_SAMPLES_REQUIRED = 20
FORECAST_HORIZON_SECONDS = 10
ENABLE_MONITOR_PLOTTING = True
PLOT_INTERVAL_SECONDS = 60
PLOT_WINDOW_SECONDS = 300
PREDICTION_REPORTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'modeling_reports')

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
    cur.execute("""
        CREATE TABLE IF NOT EXISTS interface_prediction_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            interface_ip TEXT,
            timestamp REAL,
            current_rtt REAL,
            predicted_rtt REAL,
            current_score REAL,
            future_score REAL,
            blended_score REAL
        )
    """)
    conn.commit()
    conn.close()

def get_historical_metrics(interface_ip, window_seconds=HISTORY_WINDOW_SECONDS):
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

def append_prediction_history(interface_ip, current_rtt, predicted_rtt, current_score, future_score, blended_score):
    conn = get_conn(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO interface_prediction_history (
            interface_ip, timestamp, current_rtt, predicted_rtt, current_score, future_score, blended_score
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (interface_ip, time.time(), current_rtt, predicted_rtt, current_score, future_score, blended_score)
    )
    conn.commit()
    conn.close()

def generate_monitor_plot(window_seconds=PLOT_WINDOW_SECONDS):
    if not ENABLE_MONITOR_PLOTTING:
        return

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import pandas as pd
    except Exception as import_err:
        pred_logger.error(f"Plotting disabled: {import_err}")
        return

    conn = get_conn(DB_PATH)
    try:
        df = pd.read_sql_query(
            """
            SELECT interface_ip, timestamp, current_rtt, predicted_rtt, current_score, future_score, blended_score
            FROM interface_prediction_history
            WHERE timestamp > ?
            ORDER BY timestamp ASC
            """,
            conn,
            params=(time.time() - window_seconds,)
        )
    except Exception as query_err:
        pred_logger.error(f"Failed to build monitor plot (query): {query_err}")
        conn.close()
        return
    conn.close()

    if df.empty:
        return

    os.makedirs(PREDICTION_REPORTS_DIR, exist_ok=True)

    # Align all interface series to 1-second bins so cross-interface comparisons are meaningful.
    df['t_bin'] = df['timestamp'].astype(int)
    agg = df.sort_values('timestamp').groupby(['t_bin', 'interface_ip'], as_index=False).last()
    if agg.empty:
        return

    interface_order = sorted(agg['interface_ip'].unique())
    color_map = {
        interface_order[i]: f"C{i % 10}" for i in range(len(interface_order))
    }

    fig, axes = plt.subplots(3, 1, figsize=(14, 11), sharex=True)

    # 1) What LR is doing: current RTT vs predicted RTT
    for ip in interface_order:
        subset = agg[agg['interface_ip'] == ip]
        clr = color_map[ip]
        axes[0].plot(subset['t_bin'], subset['current_rtt'], color=clr, alpha=0.75, linewidth=1.6, label=f"{ip} Current RTT")
        axes[0].plot(subset['t_bin'], subset['predicted_rtt'], '--', color=clr, alpha=0.95, linewidth=2.0, label=f"{ip} Pred RTT (+{FORECAST_HORIZON_SECONDS}s)")

    axes[0].set_title(f"Why Linear Regression: Current RTT vs Forecasted RTT (last {window_seconds}s)")
    axes[0].set_ylabel("RTT (ms)")
    axes[0].grid(True, linestyle=':', alpha=0.6)
    axes[0].legend(loc='upper left', bbox_to_anchor=(1, 1), fontsize=9)

    # 2) What orchestrator consumes: blended score trajectories
    for ip in interface_order:
        subset = agg[agg['interface_ip'] == ip]
        axes[1].plot(subset['t_bin'], subset['blended_score'], color=color_map[ip], linewidth=2.2, label=f"{ip} Blended Score")

    axes[1].set_title("Blended Score Over Time (Input to Interface Selection)")
    axes[1].set_ylabel("Blended Score")
    axes[1].grid(True, linestyle=':', alpha=0.6)
    axes[1].legend(loc='upper left', bbox_to_anchor=(1, 1), fontsize=9)

    # 3) Final decision trace: chosen interface and confidence margin.
    score_pivot = agg.pivot(index='t_bin', columns='interface_ip', values='blended_score').sort_index()
    score_pivot = score_pivot.dropna(how='all')

    if not score_pivot.empty:
        winner_ip = score_pivot.idxmax(axis=1)
        winner_score = score_pivot.max(axis=1)

        def second_best(row):
            vals = row.dropna().sort_values(ascending=False)
            return vals.iloc[1] if len(vals) > 1 else np.nan

        second_score = score_pivot.apply(second_best, axis=1)
        margin = winner_score - second_score

        ip_to_num = {ip: idx for idx, ip in enumerate(interface_order)}
        winner_num = winner_ip.map(ip_to_num)

        axes[2].step(winner_num.index, winner_num.values, where='post', color='black', linewidth=2.0, label='Chosen Interface')
        axes[2].set_yticks(list(ip_to_num.values()))
        axes[2].set_yticklabels(list(ip_to_num.keys()))
        axes[2].set_title("Selected Interface Timeline (argmax Blended Score)")
        axes[2].set_ylabel("Chosen Interface")
        axes[2].set_xlabel("Unix Timestamp (s)")
        axes[2].grid(True, linestyle=':', alpha=0.6)

        margin_axis = axes[2].twinx()
        margin_axis.plot(margin.index, margin.values, color='tab:purple', alpha=0.65, linewidth=1.8, label='Top-vs-Second Score Margin')
        margin_axis.set_ylabel("Decision Margin")

        lines_l, labels_l = axes[2].get_legend_handles_labels()
        lines_r, labels_r = margin_axis.get_legend_handles_labels()
        axes[2].legend(lines_l + lines_r, labels_l + labels_r, loc='upper left', bbox_to_anchor=(1, 1), fontsize=9)
    else:
        axes[2].text(0.5, 0.5, 'Insufficient synchronized score data', transform=axes[2].transAxes,
                     ha='center', va='center')
        axes[2].set_title("Selected Interface Timeline")
        axes[2].set_xlabel("Unix Timestamp (s)")
        axes[2].set_ylabel("Chosen Interface")
        axes[2].grid(True, linestyle=':', alpha=0.6)

    plt.tight_layout()
    output_path = os.path.join(PREDICTION_REPORTS_DIR, f"prediction_monitor_{int(time.time())}.png")
    plt.savefig(output_path)
    plt.close(fig)
    pred_logger.info(f"Prediction monitor plot saved: {output_path}")

def monitor_predictions():
    init_prediction_db()
    pred_logger.info("Predictor Daemon Started.")
    interfaces = ['10.0.1.1', '10.0.2.1', '10.0.3.1']
    prediction_count = 0
    last_plot_time = 0
    
    while True:
        try:
            for ip in interfaces:
                try:
                    history = get_historical_metrics(ip, HISTORY_WINDOW_SECONDS)
                    if len(history) < MIN_SAMPLES_REQUIRED:
                        continue                
                    # Prepare arrays
                    times = np.array([row[0] for row in history])
                    rtts = np.array([row[1] for row in history])
                    bitrates = np.array([row[2] for row in history])
                    jitters = [row[3] for row in history if row[3] is not None]
                    losses = [row[4] for row in history if row[4] is not None]
                    t0 = times[0]
                    x = times - t0
                    target_x = (time.time() - t0) + FORECAST_HORIZON_SECONDS
                    
                    # 1. Linear Regression (short-horizon future)
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
                    append_prediction_history(ip, current_rtt, pred_rtt, current_score, future_score, blended_score)
                    
                    prediction_count += 1
                    
                    # Log with appropriate level
                    if current_rtt > 500:
                        pred_logger.warning(f"[{ip}] HIGH LATENCY: RTT={current_rtt:.1f}ms | F-RTT: {pred_rtt:.1f}ms | Loss: {avg_loss:.1f}% | Score: {blended_score:.2f}")
                    elif avg_loss > 10:
                        pred_logger.warning(f"[{ip}] HIGH LOSS: RTT={current_rtt:.1f}ms | Loss: {avg_loss:.1f}% | Score: {blended_score:.2f}")
                    else:
                        pred_logger.debug(f"[{ip}] RTT: {current_rtt:.1f}ms | F-RTT: {pred_rtt:.1f}ms | Loss: {avg_loss:.1f}% | Score: {blended_score:.2f}")
                        
                except Exception as e:
                    pred_logger.error(f"Error predicting for {ip}: {e}", exc_info=False)

            now = time.time()
            if ENABLE_MONITOR_PLOTTING and (now - last_plot_time) >= PLOT_INTERVAL_SECONDS:
                generate_monitor_plot(PLOT_WINDOW_SECONDS)
                last_plot_time = now
                    
            time.sleep(0.5)
        except Exception as e:
            pred_logger.error(f"Monitor loop error: {e}", exc_info=True)
            time.sleep(1)

if __name__ == "__main__":
    try:
        monitor_predictions()
    except KeyboardInterrupt:
        pred_logger.info("\nStopping Predictor Daemon...")
        sys.exit(0)
