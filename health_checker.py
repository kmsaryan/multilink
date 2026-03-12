#!/usr/bin/python3
import time
import psutil
import netifaces
import socket
import struct
import sys
import numpy as np
import signal
import logging
import os
from datetime import datetime
from config import DB_PATH, RECEIVER_IP, HEALTH_PORT
from db_utils import update_interface_health, init_sender_db, get_conn

# Setup logging with file and console output
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

def setup_logger(interface_ip):
    logger = logging.getLogger(f'health_checker_{interface_ip}')
    logger.setLevel(logging.DEBUG)
    
    # File handler - captures everything
    log_file = os.path.join(LOG_DIR, f'health_checker_{interface_ip.replace(".", "_")}.log')
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    
    # Console handler - shows INFO and above
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    
    # Formatter with timestamp
    formatter = logging.Formatter('[%(asctime)s] [%(levelname)-8s] %(message)s', 
                                  datefmt='%Y-%m-%d %H:%M:%S')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

def get_interface_name_for_ip(interface_ip):
    for iface in netifaces.interfaces():
        addrs = netifaces.ifaddresses(iface).get(netifaces.AF_INET, [])
        for addr in addrs:
            if addr.get('addr') == interface_ip:
                return iface
    raise ValueError(f"No interface found for IP {interface_ip}")

def measure_rtt_reused(sock, dest_ip, dest_port):
    try:
        probe_pkt = b'\x02' + b'\x00' * 16 + struct.pack("!I", 0)
        start = time.perf_counter() 
        sock.sendto(probe_pkt, (dest_ip, dest_port))
        
        resp, _ = sock.recvfrom(1024)
        rtt_ms = (time.perf_counter() - start) * 1000

        if len(resp) >= 1 and resp[0] == 0x02:
            return rtt_ms
        return None
    except socket.timeout:
        return None
    except Exception as e:
        print(f"RTT Probe Exception: {e}")
        return None

def calculate_throughput_bps(interface_name, prev_counters, interval):
    net_io = psutil.net_io_counters(pernic=True)
    curr = net_io.get(interface_name)
    if not curr or not prev_counters:
        return 0.0, curr

    delta_bytes = (curr.bytes_sent + curr.bytes_recv) - (prev_counters.bytes_sent + prev_counters.bytes_recv)
    throughput_bps = (delta_bytes * 8) / interval
    return max(0.0, throughput_bps), curr

def run_health_worker(interface_ip):
    logger = setup_logger(interface_ip)
    logger.info(f"Health checker worker started for {interface_ip}")
    
    interval = 1 
    prev_counters = None
    
    rtt_samples = []
    rtt_window = 10 
    
    probe_history = [] 
    loss_window = 20
    consecutive_timeouts = 0
    max_consecutive_timeouts = 10

    PROBE_SIZE_BITS = 168 

    try:
        iface_name = get_interface_name_for_ip(interface_ip)
        dest_port = HEALTH_PORT  
        logger.info(f"Monitoring {interface_ip} on interface {iface_name} -> Target {RECEIVER_IP}:{dest_port}")
    except ValueError as e:
        logger.error(f"Failed to find interface for IP {interface_ip}: {e}")
        return

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind((interface_ip, 0))
        sock.settimeout(0.5) 

        while True:
            try:
                rtt = measure_rtt_reused(sock, RECEIVER_IP, dest_port)
                throughput_bps, curr_counters = calculate_throughput_bps(iface_name, prev_counters, interval)
                prev_counters = curr_counters

                probe_history.append(rtt is not None)
                if len(probe_history) > loss_window: probe_history.pop(0)
                current_loss_rate = (probe_history.count(False) / len(probe_history)) * 100

                if rtt is not None:
                    rtt_samples.append(rtt)
                    if len(rtt_samples) > rtt_window: rtt_samples.pop(0)
                    
                    avg_rtt = sum(rtt_samples) / len(rtt_samples)
                    
                    current_jitter = 0.0
                    if len(rtt_samples) > 1:
                        differences = [abs(rtt_samples[i] - rtt_samples[i-1]) for i in range(1, len(rtt_samples))]
                        current_jitter = sum(differences) / len(differences)

                    consecutive_timeouts = 0  # Reset timeout counter on successful probe
                    
                    rtt_seconds = rtt / 1000.0
                    instant_bitrate = (PROBE_SIZE_BITS * 2) / rtt_seconds if rtt_seconds > 0 else 0.0

                    update_interface_health(DB_PATH, interface_ip, rtt, throughput_bps, current_jitter, current_loss_rate, instant_bitrate)
                    
                    try:
                        conn = get_conn(DB_PATH)
                        cur = conn.cursor()
                        cur.execute("""INSERT INTO interface_metrics_history 
                                       (interface_ip, timestamp, uplink_rtt, throughput, jitter, loss_rate, instant_bitrate) 
                                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                                    (interface_ip, time.time(), rtt, throughput_bps, current_jitter, current_loss_rate, instant_bitrate))
                        conn.commit()
                        conn.close()
                    except Exception as db_err:
                        logger.error(f"Database error: {db_err}")
                    
                    # Log at appropriate level based on RTT
                    if rtt > 500:
                        logger.warning(f"[{interface_ip}] HIGH LATENCY: RTT={rtt:6.2f}ms | Jitter: {current_jitter:5.2f}ms | Loss: {current_loss_rate:4.1f}%")
                    else:
                        logger.debug(f"[{interface_ip}] RTT: {rtt:6.2f}ms | Jitter: {current_jitter:5.2f}ms | Loss: {current_loss_rate:4.1f}%")
                
                else:
                    consecutive_timeouts += 1
                    current_jitter = 0.0 
                    instant_bitrate = 0.0
                    
                    update_interface_health(DB_PATH, interface_ip, 999.9, throughput_bps, current_jitter, current_loss_rate, instant_bitrate)
                    
                    try:
                        conn = get_conn(DB_PATH)
                        cur = conn.cursor()
                        cur.execute("""INSERT INTO interface_metrics_history 
                                       (interface_ip, timestamp, uplink_rtt, throughput, jitter, loss_rate, instant_bitrate) 
                                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                                    (interface_ip, time.time(), 999.9, throughput_bps, current_jitter, current_loss_rate, instant_bitrate))
                        conn.commit()
                        conn.close()
                    except Exception as db_err:
                        logger.error(f"Database error during timeout logging: {db_err}")

                    # Log escalation for consecutive timeouts
                    log_level = logging.WARNING if consecutive_timeouts < max_consecutive_timeouts else logging.CRITICAL
                    logger.log(log_level, f"LINK FAILURE [{consecutive_timeouts}/{max_consecutive_timeouts}]: RTT TIMEOUT | Loss: {current_loss_rate:4.1f}%")
                    
                    if consecutive_timeouts == max_consecutive_timeouts:
                        logger.critical(f"CRITICAL: Interface {interface_ip} has {consecutive_timeouts} consecutive timeouts. Link may be DOWN.")

                time.sleep(interval)

            except Exception as e:
                logger.error(f"Worker loop exception: {e}", exc_info=True)
                time.sleep(1)

def signal_handler(sig, frame):
    logger = logging.getLogger()
    logger.info("Health Checker shutting down via signal...")
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    if len(sys.argv) != 2:
        print("Usage: python3 health_checker.py <interface_ip>")
        sys.exit(1)
    
    init_sender_db(DB_PATH)
    run_health_worker(sys.argv[1])