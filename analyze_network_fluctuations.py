#!/usr/bin/env python3

import re
import json
import os
import sys
import logging
from collections import defaultdict

# Setup logging
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

def setup_analysis_logger():
    logger = logging.getLogger('network_analysis')
    logger.setLevel(logging.DEBUG)
    
    log_file = os.path.join(LOG_DIR, 'network_analysis.log')
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

analysis_logger = setup_analysis_logger()

# Read the orchestrator log file
log_file_path = os.path.join(LOG_DIR, 'orchestrator.log')

if not os.path.exists(log_file_path):
    analysis_logger.error(f"Orchestrator log not found at {log_file_path}")
    analysis_logger.info("Please run: python3 orchestrator.py first to generate logs")
    sys.exit(1)

try:
    with open(log_file_path, 'r') as f:
        lines = f.readlines()
    analysis_logger.info(f"Loaded orchestrator log: {log_file_path} ({len(lines)} lines)")
except IOError as e:
    analysis_logger.error(f"Failed to read orchestrator log: {e}")
    sys.exit(1)
queue_statuses = []
reset_events = []
unhealthy_interfaces = 0
fluctuations = []

for i, line in enumerate(lines, 1):
    # Parse Queue Status lines
    if 'Queue Status:' in line:
        try:
            # Extract the dict from the line
            status_str = line.split('Queue Status:')[1].strip()
            status = eval(status_str)  # Parse the Python dict
            status['line_num'] = i
            queue_statuses.append(status)
        except:
            continue
    
    # Look for Reset events (network timeouts)
    if 'Reset' in line and 'timed-out chunks' in line:
        match = re.search(r'Reset (\d+) timed-out', line)
        if match:
            num_reset = int(match.group(1))
            reset_events.append({
                'line_num': i,
                'num_reset': num_reset,
                'text': line.strip()
            })
    
    # Count unhealthy interface messages
    if 'No healthy interfaces found in DB!' in line:
        unhealthy_interfaces += 1

# Analyze queue status fluctuations
analysis_logger.info("=" * 80)
analysis_logger.info("NETWORK ANALYSIS REPORT")
analysis_logger.info("=" * 80)
analysis_logger.info(f"\nTotal log lines: {len(lines)}")
analysis_logger.info(f"Queue status entries: {len(queue_statuses)}")
analysis_logger.info(f"Unhealthy interface warnings: {unhealthy_interfaces}")
analysis_logger.info(f"Network timeout reset events: {len(reset_events)}")

# Analyze sending queue fluctuations
if len(queue_statuses) > 1:
    analysis_logger.info("\n" + "=" * 80)
    analysis_logger.info("SENDING QUEUE ANALYSIS (detects in-flight packets)")
    analysis_logger.info("=" * 80)
    
    sending_values = []
    significant_fluctuations = []
    
    for i in range(len(queue_statuses)):
        if 'sending' in queue_statuses[i]:
            sending_values.append(queue_statuses[i]['sending'])
            
            if i > 0 and 'sending' in queue_statuses[i-1]:
                prev_sending = queue_statuses[i-1]['sending']
                curr_sending = queue_statuses[i]['sending']
                change = abs(curr_sending - prev_sending)
                
                # Flag significant changes (>200 chunks)
                if change > 200:
                    significant_fluctuations.append({
                        'position': i,
                        'from': prev_sending,
                        'to': curr_sending,
                        'change': curr_sending - prev_sending,
                        'abs_change': change,
                        'line_num': queue_statuses[i]['line_num']
                    })
    
    if sending_values:
        analysis_logger.info(f"Min sending queue: {min(sending_values)}")
        analysis_logger.info(f"Max sending queue: {max(sending_values)}")
        analysis_logger.info(f"Average sending queue: {sum(sending_values)/len(sending_values):.1f}")
        analysis_logger.info(f"Significant changes (>200 chunks): {len(significant_fluctuations)}")
        
        if significant_fluctuations:
            analysis_logger.info("\nTop 10 significant fluctuations:")
            sorted_fluct = sorted(significant_fluctuations, key=lambda x: x['abs_change'], reverse=True)
            for idx, fluct in enumerate(sorted_fluct[:10], 1):
                analysis_logger.info(f"  {idx}. Line {fluct['line_num']}: {fluct['from']} → {fluct['to']} "
                      f"(change: {fluct['change']:+d})")

# Analyze timeout events
analysis_logger.info("\n" + "=" * 80)
analysis_logger.info("NETWORK TIMEOUT EVENTS (indicates packet loss/retransmission)")
analysis_logger.info("=" * 80)

if reset_events:
    analysis_logger.info(f"\nTotal timeout reset events: {len(reset_events)}")
    total_reset_chunks = sum(e['num_reset'] for e in reset_events)
    analysis_logger.info(f"Total chunks reset to pending: {total_reset_chunks}")
    analysis_logger.debug(f"\nDetailed reset events (first 50):")
    
    for idx, event in enumerate(reset_events[:50], 1):
        analysis_logger.debug(f"  {idx}. Line {event['line_num']}: {event['num_reset']} chunks")

# Analyze pending queue to detect stalls
analysis_logger.info("\n" + "=" * 80)
analysis_logger.info("PENDING QUEUE ANALYSIS (shows processing backlog)")
analysis_logger.info("=" * 80)

pending_values = []
stall_periods = []

for i in range(len(queue_statuses)):
    if 'pending' in queue_statuses[i]:
        pending_values.append(queue_statuses[i]['pending'])
        
        # Detect stalls (pending count stays high or increases)
        if len(pending_values) > 10:
            recent_pending = pending_values[max(0, len(pending_values)-10):len(pending_values)]
            if recent_pending and pending_values[-1] > sum(recent_pending)/len(recent_pending):
                stall_periods.append({'position': i, 'pending': pending_values[-1]})

if pending_values:
    analysis_logger.info(f"Initial pending: {pending_values[0]}")
    analysis_logger.info(f"Final pending: {pending_values[-1]}")
    analysis_logger.info(f"Min pending: {min(pending_values)}")
    analysis_logger.info(f"Max pending: {max(pending_values)}")
    analysis_logger.info(f"Total chunks processed: {pending_values[0] - pending_values[-1]}")

analysis_logger.info("\n" + "=" * 80)
analysis_logger.info("SUMMARY OF NETWORK FLUCTUATIONS")
analysis_logger.info("=" * 80)

total_events = len(reset_events) + len(significant_fluctuations) + unhealthy_interfaces
analysis_logger.info(f"\nTotal network fluctuation indicators: {total_events}")
analysis_logger.info(f"  - Timeout reset events: {len(reset_events)}")
analysis_logger.info(f"  - Significant sending queue changes: {len(significant_fluctuations)}")
analysis_logger.info(f"  - Unhealthy interface warnings: {unhealthy_interfaces}")

if reset_events:
    avg_reset = sum(e['num_reset'] for e in reset_events) / len(reset_events)
    analysis_logger.info(f"\nAverage chunks per reset event: {avg_reset:.1f}")

analysis_logger.info("=" * 80)
analysis_logger.info(f"Analysis complete. Report saved to {log_file_path}")
