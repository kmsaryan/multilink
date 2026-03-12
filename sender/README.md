## Project Structure

This repository contains both the Sender (VM1) and Receiver (VM2) pipelines.
```
├── Receiver/              # VM2 Cloud endpoint scripts and 
│   ├── receiver.py        # Listens for UDP chunks
│   └── generate_receiver_report.py
├── orchestrator.py        # VM1: Brains of the routing logic
├── prediction_monitor.py  # VM1: Calculates future L3 metrics 
├── health_checker.py      # VM1: Probes interfaces for latency/loss
├── sender_worker.py       # VM1: Transmits data over designated interface
└── manager.py             # VM1: Ingests payloads into the database
```
## Project Contents

- `health_checker.py`: probes each interface and stores RTT/throughput/jitter/loss.
- `prediction_monitor.py`: computes predictive scores from recent history and writes `interface_predictions`.
- `orchestrator.py`: assigns pending chunks to interfaces based on current/predicted scores.
- `sender_worker.py`: sends assigned chunks for one interface and forwards ACKs.
- `manager.py`: registers payload files by chunking them into the DB.
- `plot_results.py`: generates post-run modeling analysis and plots.

## Environment Setup

### 1) Create and install virtual environment

```bash
cd /usr/local/bin/multilink
bash setup_venv.sh
```

### 2) Activate environment

```bash
source .venv/bin/activate
```

## Install Manually (Alternative)

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

## Typical Run Order (VM1)

1. Ensure receiver side receiver is running before starting  health workers:

```bash
./run_health.sh
```

2. Start predictor daemon:

```bash
python3 prediction_monitor.py
```

3. Start orchestrator:

```bash
python3 orchestrator.py
```

4. Start sender workers:

```bash
./run_sender.sh
```

5. Start manager (monitor mode):

```bash
python3 manager.py
```

6. Drop a test file into payload folder:

```bash
cd payloads
dd if=/dev/urandom of=testfile.txt bs=1M count=1
```

## Notes

- Main DB: `sender_coord.db`
- Key tables: `payloads`, `chunks`, `interface_stats`, `interface_metrics_history`, `interface_predictions`
- Generated figures are saved under `modeling_reports/`