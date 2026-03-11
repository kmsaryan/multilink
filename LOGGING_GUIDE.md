# Multilink Logging & Documentation Guide

## Overview
All Python scripts now include comprehensive structured logging to capture network fluctuations, link failures, and system behavior for documentation and debugging.

## Log File Locations
All logs are stored in the `logs/` directory:
```
/usr/local/bin/multilink/logs/
├── health_checker_10_0_1_1.log    (Wi-Fi health metrics)
├── health_checker_10_0_2_1.log    (5G health metrics)
├── health_checker_10_0_3_1.log    (Satellite health metrics)
├── orchestrator.log               (Data orchestration & timeouts)
├── prediction_monitor.log         (Network predictions & scoring)
└── plot_results.log               (Analysis results)
```

## How to Run Scripts with Proper Logging

### Option 1: Use Provided Shell Scripts (Recommended)
```bash
# Start health checker with logging
./run_health.sh

# Start sender with logging  
./run_sender.sh

# Start orchestrator (in new terminal)
python3 orchestrator.py

# Start prediction monitor (in new terminal)
python3 prediction_monitor.py
```

**Why this is better:**
- All stdout/stderr automatically captured
- Logs rotate per interface (no single massive file)
- Easy to grep/analyze specific link failures
- File and console output simultaneously

### Option 2: Manual with Full Redirection (For Testing)
```bash
# Capture both stdout and stderr to file
python3 health_checker.py 10.0.2.1 >> logs/health_checker_10_0_2_1.log 2>&1 &
python3 prediction_monitor.py >> logs/prediction_monitor.log 2>&1 &
python3 orchestrator.py >> logs/orchestrator.log 2>&1 &
```

The `2>&1` redirects **stderr to stdout**, so errors are captured in logs.

## Understanding Log Levels

| Level | Color | Usage | Example |
|-------|-------|-------|---------|
| `DEBUG` | Grey | Normal operation | RTT measurements, predictions |
| `INFO` | Blue | Important milestones | Worker started, ACK counts |
| `WARNING` | Yellow | Network issues | High latency (>500ms), high loss (>10%) |
| `ERROR` | Red | Failures | Database errors, socket failures |
| `CRITICAL` | Red+Bold | Severe issues | Link DOWN (10+ consecutive timeouts) |

## Key Things to Look For in Logs

### 1. **Link Failures** (Health Checker Logs)
Look for patterns in `health_checker_*.log`:
```
[2026-03-11 14:32:15] [WARNING ] LINK FAILURE [1/10]: RTT TIMEOUT | Loss: 15.2%
[2026-03-11 14:32:16] [WARNING ] LINK FAILURE [2/10]: RTT TIMEOUT | Loss: 18.5%
[2026-03-11 14:32:17] [WARNING ] LINK FAILURE [3/10]: RTT TIMEOUT | Loss: 22.1%
[2026-03-11 14:32:20] [CRITICAL] CRITICAL: Interface 10.0.2.1 has 10 consecutive timeouts. Link may be DOWN.
```

**Recovery Example:**
```
[2026-03-11 14:32:21] [WARNING ] LINK FAILURE [1/10]: RTT TIMEOUT | Loss: 8.5%
[2026-03-11 14:32:22] [WARNING ] [10.0.2.1] RTT: 125.45ms | Jitter: 5.23ms | Loss: 3.2%   ← Link recovered
```

### 2. **Network Timeouts** (Orchestrator Logs)
Look in `orchestrator.log` for chunk retransmissions:
```
[2026-03-11 14:35:22] [WARNING ] NETWORK TIMEOUT: Reset 44 timed-out chunks to pending (30s timeout)
[2026-03-11 14:35:28] [WARNING ] NETWORK TIMEOUT: Reset 68 timed-out chunks to pending (30s timeout)
```

**What this means:** The sending interface didn't receive ACK packets within 30 seconds.

### 3. **Interface Health Degradation** (Prediction Monitor Logs)
```
[2026-03-11 14:40:15] [WARNING ] [10.0.3.1] HIGH LATENCY: RTT=562.3ms | F-RTT: 645.2ms | Loss: 2.1% | Score: 0.15
[2026-03-11 14:40:16] [WARNING ] [10.0.3.1] HIGH LOSS: RTT=85.2ms | Loss: 18.5% | Score: 0.08
```

**Interpretation:**
- RTT > 500ms = Interface struggling with latency
- Loss > 10% = Significant packet loss
- Score < 0.5 = Interface degraded, avoid if possible

## Grep Commands for Analysis

### Find all critical errors:
```bash
grep -r "CRITICAL" logs/
```

### Find all link failures on a specific interface:
```bash
grep "LINK FAILURE" logs/health_checker_10_0_2_1.log
```

### Find timeout events:
```bash
grep "NETWORK TIMEOUT" logs/orchestrator.log
```

### Count how many times interface went down:
```bash
grep -c "CRITICAL.*consecutive timeouts" logs/health_checker_*.log
```

### Find high-latency events:
```bash
grep "HIGH LATENCY" logs/prediction_monitor.log
```

### Get statistics from a time window:
```bash
# Events between 14:30 and 14:40
grep "14:3[0-9]" logs/orchestrator.log | wc -l
```

## Log Analysis for Documentation

### Example: Write a Report of Network Issues
```bash
#!/bin/bash
echo "=== NETWORK INCIDENT REPORT ==="
echo "Time: $(date)"
echo ""
echo "=== LINK FAILURES ==="
grep "CRITICAL" logs/health_checker_*.log | head -5
echo ""
echo "=== TIMEOUT EVENTS ==="
grep "NETWORK TIMEOUT" logs/orchestrator.log | head -5
echo ""
echo "=== DEGRADED INTERFACES ==="
grep "HIGH LATENCY\|HIGH LOSS" logs/prediction_monitor.log | head -5
```

## Real-Time Log Monitoring

### Watch orchestrator timeouts as they happen:
```bash
tail -f logs/orchestrator.log | grep "TIMEOUT\|CRITICAL"
```

### Monitor all health checker warnings:
```bash
tail -F logs/health_checker_*.log | grep "WARNING\|CRITICAL"
```

### Follow prediction monitor scoring in real-time:
```bash
tail -f logs/prediction_monitor.log | grep -v DEBUG
```

## Network Analysis Script

The `analyze_network_fluctuations.py` script has been updated with proper logging:

### Run with automatic logging:
```bash
# Option 1: Direct execution (recommended)
python3 analyze_network_fluctuations.py

# Option 2: Using the wrapper script
./run_analysis.sh
```

### What it analyzes:
- **Sending queue fluctuations**: Detects sudden drops in in-flight packets (>200 chunk changes)
- **Network timeouts**: Counts reset events and total chunks that had to be retransmitted
- **Unhealthy interfaces**: Reports when no interfaces were available for assignment
- **Pending queue analysis**: Shows processing bottlenecks and completion rates

### Example output:
```
[2026-03-11 20:40:15] [INFO    ] ================================================================================
[2026-03-11 20:40:15] [INFO    ] NETWORK ANALYSIS REPORT
[2026-03-11 20:40:15] [INFO    ] Total log lines: 5090
[2026-03-11 20:40:15] [INFO    ] Queue status entries: 2629
[2026-03-11 20:40:15] [INFO    ] Unhealthy interface warnings: 52
[2026-03-11 20:40:15] [INFO    ] Network timeout reset events: 500
[2026-03-11 20:40:15] [INFO    ] SUMMARY OF NETWORK FLUCTUATIONS
[2026-03-11 20:40:15] [INFO    ] Total network fluctuation indicators: 554
```

### View full analysis report:
```bash
cat logs/network_analysis.log
```

### Filter report by severity:
```bash
# Show only summary information
grep "\[INFO" logs/network_analysis.log | tail -20

# Show all analysis details
cat logs/network_analysis.log
```

### Automatic error detection:
If you run the analysis before the orchestrator, it will warn you:
```
[2026-03-11 20:39:26] [ERROR   ] Orchestrator log not found at .../logs/orchestrator.log
[2026-03-11 20:39:26] [INFO    ] Please run: python3 orchestrator.py first to generate logs
```

## Archiving & Cleanup

### Archive logs older than 7 days:
```bash
find logs/ -name "*.log" -mtime +7 -exec gzip {} \;
```

### View compressed logs:
```bash
zcat logs/health_checker_10_0_2_1.log.gz | grep "CRITICAL"
```

## Integration with plot_results.py

The analysis script can cross-reference logs:
```python
# After running analysis, check if there were timeout events
import os
log_file = 'logs/orchestrator.log'
with open(log_file) as f:
    timeouts = [line for line in f if 'TIMEOUT' in line]
    print(f"Found {len(timeouts)} timeout events during analysis period")
```

## Troubleshooting

**Problem:** Logs directory not created
```bash
mkdir -p /usr/local/bin/multilink/logs
chmod 777 /usr/local/bin/multilink/logs
```

**Problem:** Permission denied writing to logs
```bash
# Fix permissions
sudo chown $USER:$USER /usr/local/bin/multilink/logs
chmod 755 /usr/local/bin/multilink/logs
```

**Problem:** Log files too large
```bash
# Rotate manually
gzip logs/*.log
# Or implement log rotation in cron
```

## Best Practices

1. **Always check logs after running tests** - Tells you what actually happened vs. what you expected
2. **Archive old logs for auditing** - Keep 2 weeks of logs minimum
3. **Tag important test runs** - Add a marker in logs: `logger.info("=== TEST RUN: My Experiment ===" )`
4. **Use different terminals per script** - Easier to see logs together
5. **For documentation, use timestamped excerpts** - Makes incidents reproducible

---

**Example Documentation Entry:**
```markdown
### Incident: 5G Interface Timeout (2026-03-11 14:32:15)

Orchestrator logs show 44 chunks timed out on interface 10.0.2.1:
```
[2026-03-11 14:32:15] [WARNING ] NETWORK TIMEOUT: Reset 44 timed-out chunks...
```

Health checker shows 9 consecutive probe failures before recovery:
```
[2026-03-11 14:32:15] [WARNING ] LINK FAILURE [1/10]: RTT TIMEOUT
...
[2026-03-11 14:32:22] [DEBUG] [10.0.2.1] RTT: 145.2ms  ← Recovered
```

**Root Cause:** Temporary network congestion on 5G link
**Resolution:** Link auto-recovered within 7 seconds
```
