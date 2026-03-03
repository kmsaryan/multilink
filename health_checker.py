#!/usr/bin/python3
import time
import psutil
import netifaces
import socket
import struct
import sys
import numpy as np
import signal
from config import DB_PATH, RECEIVER_IP, RECEIVER_PORT, WIFI_PORT, FIVEG_PORT, SATELLITE_PORT
from db_utils import update_interface_health, init_sender_db, get_conn

def get_port_for_interface(interface_ip):
    if interface_ip == "10.0.1.1": return 5001
    elif interface_ip == "10.0.2.1": return 5002
    elif interface_ip == "10.0.3.1": return 5003
    return RECEIVER_PORT

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

def calculate_throughput(interface_name, prev_counters, interval):
    net_io = psutil.net_io_counters(pernic=True)
    curr = net_io.get(interface_name)
    if not curr or not prev_counters:
        return 0.0, curr

    delta_bytes = (curr.bytes_sent + curr.bytes_recv) - (prev_counters.bytes_sent + prev_counters.bytes_recv)
    throughput_mbps = (delta_bytes * 8) / (interval * 1_000_000)
    return max(0.0, throughput_mbps), curr

def run_health_worker(interface_ip):
    interval = 1 
    prev_counters = None
    
    # NEW: Tracking windows for Jitter and Loss
    rtt_samples = []
    rtt_window = 10 
    
    probe_history = [] # True for success, False for timeout
    loss_window = 20

    try:
        iface_name = get_interface_name_for_ip(interface_ip)
        dest_port = get_port_for_interface(interface_ip)
        print(f" Monitoring {interface_ip} on {iface_name} -> Target Port {dest_port}")
    except ValueError as e:
        print(e)
        return

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind((interface_ip, 0))
        sock.settimeout(0.5) 

        while True:
            try:
                # 1. Measure RTT
                rtt = measure_rtt_reused(sock, RECEIVER_IP, dest_port)
                
                # 2. Measure Throughput
                throughput, curr_counters = calculate_throughput(iface_name, prev_counters, interval)
                prev_counters = curr_counters

                # --- NEW: Calculate Loss Rate ---
                probe_history.append(rtt is not None)
                if len(probe_history) > loss_window: probe_history.pop(0)
                
                # Percentage of False (Timeout) values in the window
                current_loss_rate = (probe_history.count(False) / len(probe_history)) * 100

                if rtt is not None:
                    rtt_samples.append(rtt)
                    if len(rtt_samples) > rtt_window: rtt_samples.pop(0)
                    
                    avg_rtt = sum(rtt_samples) / len(rtt_samples)
                    
                    # --- NEW: Calculate Jitter ---
                    # Jitter is the mean of absolute differences between consecutive RTTs
                    current_jitter = 0.0
                    if len(rtt_samples) > 1:
                        differences = [abs(rtt_samples[i] - rtt_samples[i-1]) for i in range(1, len(rtt_samples))]
                        current_jitter = sum(differences) / len(differences)

                    # 3. Update LATEST metrics (Now with real Jitter and Loss)
                    update_interface_health(DB_PATH, interface_ip, rtt, throughput, jitter=current_jitter, loss_rate=current_loss_rate)
                    # 4. Update HISTORY metrics
                    conn = get_conn(DB_PATH)
                    cur = conn.cursor()
                    cur.execute("""INSERT INTO interface_metrics_history 
                                   (interface_ip, timestamp, uplink_rtt, throughput, jitter, loss_rate) 
                                   VALUES (?, ?, ?, ?, ?, ?)""",
                                (interface_ip, time.time(), rtt, throughput, current_jitter, current_loss_rate))
                    
                    conn.commit()
                    conn.close()

                    print(f"[{interface_ip}] RTT: {rtt:6.2f}ms | Jitter: {current_jitter:5.2f}ms | Loss: {current_loss_rate:4.1f}% | Tput: {throughput:5.2f} Mbps")
                else:
                    # Timeout occurred
                    current_jitter = 0.0 # Can't calculate jitter on a dropped packet
                    update_interface_health(DB_PATH, interface_ip, 999.9, throughput, jitter=current_jitter, loss_rate=current_loss_rate)
                    print(f"[{interface_ip}] RTT: TIMEOUT | Jitter:   N/A | Loss: {current_loss_rate:4.1f}% | Tput: {throughput:5.2f} Mbps")

                time.sleep(interval)

            except Exception as e:
                print(f"Worker Loop Error: {e}")
                time.sleep(1)

def signal_handler(sig, frame):
    print("\n Health Checker exiting...")
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    if len(sys.argv) != 2:
        sys.exit(1)
    
    init_sender_db(DB_PATH)
    run_health_worker(sys.argv[1])