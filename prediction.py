#!/usr/bin/env python3
import time
import numpy as np
from db_utils import get_conn
from config import DB_PATH

class CapacityPredictor:
    def __init__(self):
        # We don't need heavy TensorFlow/Keras for simple 30s linear trends.
        # Numpy's polyfit is much faster for real-time networking.
        pass

    def get_history(self, interface_ip, window_seconds=60):
        """Fetch history with timestamps for regression."""
        conn = get_conn(DB_PATH)
        cur = conn.cursor()
        # We need the timestamp (last_check) to use as our X-axis
        cur.execute("""SELECT last_check, avg_rtt, success_rate 
                       FROM interface_stats 
                       WHERE interface_ip = ? AND last_check > ?
                       ORDER BY last_check ASC""",
                    (interface_ip, time.time() - window_seconds))
        results = cur.fetchall()
        conn.close()
        return results

    def predict_next_30s(self, interface_ip):
        """
        Calculates a linear trend and predicts values 30 seconds into the future.
        """
        history = self.get_history(interface_ip)
        
        if len(history) < 5:  # Need a minimum sample size to establish a trend
            return None, None

        # Prepare data
        times = np.array([row[0] for row in history])
        rtts = np.array([row[1] for row in history])
        throughputs = np.array([row[2] for row in history])

        # Normalize time (seconds since first sample) to keep math stable
        t0 = times[0]
        x = times - t0
        target_x = (time.time() - t0) + 30  # Where we want to be in 30 seconds

        # 1. Predict RTT (Latency)
        # polyfit(x, y, 1) returns [slope, intercept]
        rtt_slope, rtt_intercept = np.polyfit(x, rtts, 1)
        predicted_rtt = (rtt_slope * target_x) + rtt_intercept

        # 2. Predict Throughput (Capacity)
        tput_slope, tput_intercept = np.polyfit(x, throughputs, 1)
        predicted_tput = (tput_slope * target_x) + tput_intercept

        # Ensure we don't return impossible negative values
        return max(0.1, predicted_rtt), max(0, predicted_tput)

# Global predictor instance
predictor = CapacityPredictor()