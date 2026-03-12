# Sender

VM1 sender-side pipeline for multi-path UDP file transfer. Monitors link health across multiple interfaces, predicts quality, orchestrates chunk routing, and transmits data to the receiver.

## Directory Contents

```
sender/
├── config.py                       # Shared config: paths, ports, IPs
├── db_utils.py                     # SQLite helpers and schema init
├── health_checker.py               # Probes interfaces for RTT/jitter/loss
├── prediction_monitor.py           # Computes predictive link scores
├── orchestrator.py                 # Routes chunks to interfaces by score
├── sender_worker.py                # Transmits chunks over one interface
├── manager.py                      # Ingests payload files into the DB
├── Modeling.py                     # Capacity prediction model
├── prediction.py                   # Linear trend predictor
├── plot_results.py                 # Post-run analysis plots
├── analyze_network_fluctuations.py # Log-based network analysis
├── run_health.sh                   # Launches health checker workers
├── run_sender.sh                   # Launches sender workers
├── run_analysis.sh                 # Runs network fluctuation analysis
├── setup_venv.sh                   # Creates and installs virtualenv
├── requirements.txt                # Python dependencies
├── logs/                           # Runtime log files
├── payloads/                       # Input files to transfer
└── modeling_reports/               # Generated analysis figures
```

## Environment Setup

### 1) Create and install virtual environment

```bash
cd /usr/local/bin/multilink/sender
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

Ensure the receiver side is running before starting.

1. Start health checker workers:

```bash
cd /usr/local/bin/multilink/sender
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

5. Start manager (monitors payload folder and ingests files):

```bash
python3 manager.py
```

6. Drop a test file into the payload folder:

```bash
dd if=/dev/urandom of=payloads/testfile.bin bs=1M count=1
```

## Notes

- Main DB: `sender/sender_coord.db`
- Key tables: `payloads`, `chunks`, `interface_stats`, `interface_metrics_history`, `interface_predictions`
- Generated figures are saved under `sender/modeling_reports/`
- All scripts must be run from within `sender/` or via the absolute path `sender/run_*.sh`