#!/usr/bin/env python3
import time
import numpy as np
from db_utils import get_conn
from config import DB_PATH

class CapacityPredictor:
    def __init__(self):
        pass

    def get_history(self, interface_ip, window_seconds=60):
        conn = get_conn(DB_PATH)
        cur = conn.cursor()
        # NEW: Fetching jitter and loss_rate from history
        cur.execute("""SELECT timestamp, uplink_rtt, throughput, jitter, loss_rate 
                       FROM interface_metrics_history 
                       WHERE interface_ip = ? AND timestamp > ?
                       ORDER BY timestamp ASC""",
                    (interface_ip, time.time() - window_seconds))
        results = cur.fetchall()
        conn.close()
        return results

    def predict_next_30s(self, interface_ip):
        history = self.get_history(interface_ip)
        if len(history) < 5:  
            return None, None, None, None

        # Prepare data arrays
        times = np.array([row[0] for row in history])
        rtts = np.array([row[1] for row in history])
        throughputs = np.array([row[2] for row in history])
        
        # We don't predict jitter/loss 30s out; we just use the current recent average to penalize
        jitters = [row[3] for row in history if row[3] is not None]
        losses = [row[4] for row in history if row[4] is not None]

        t0 = times[0]
        x = times - t0
        target_x = (time.time() - t0) + 30 

        # Predict RTT
        rtt_slope, rtt_intercept = np.polyfit(x, rtts, 1)
        predicted_rtt = max(0.1, (rtt_slope * target_x) + rtt_intercept)

        # Predict Throughput
        tput_slope, tput_intercept = np.polyfit(x, throughputs, 1)
        predicted_tput = max(0.01, (tput_slope * target_x) + tput_intercept)
        
        # Averages for penalties
        avg_jitter = sum(jitters) / len(jitters) if jitters else 0.0
        avg_loss = sum(losses) / len(losses) if losses else 0.0

        return predicted_rtt, predicted_tput, avg_jitter, avg_loss

predictor = CapacityPredictor()