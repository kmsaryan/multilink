# Receiver

VM2 receiver-side pipeline for multi-path UDP file transfer. Listens for incoming UDP chunks across all interfaces, tracks reassembly state via SQLite, and generates delivery reports.

## Directory Contents

```
Receiver/
├── config.py                    # Paths, ports, and IP configuration
├── db_utils.py                  # SQLite helpers and schema init
├── receiver.py                  # Listens for UDP chunks and reassembles payloads
├── generate_receiver_report.py  # Generates delivery report after transfer
├── setup_venv.sh                # Creates and installs virtualenv
├── requirements.txt             # Python dependencies
├── received/                    # Reassembled output files
└── results/                     # Generated reports and plots
```

## Prerequisites

- Linux
- Python 3.10+ (3.11 recommended)

## Environment Setup

```bash
cd /usr/local/bin/multilink/Receiver
chmod +x setup_venv.sh
./setup_venv.sh
source .venv/bin/activate
```

## Run receiver

```bash
cd /usr/local/bin/multilink/Receiver
python3 receiver.py
```

## Generate report

```bash
python3 generate_receiver_report.py
```

## Libraries used

### Third-party (in requirements.txt)

- pandas
- matplotlib

### Python standard library (included with Python)

- `receiver.py`: socket, struct, uuid, os, time, select, sqlite3
- `db_utils.py`: sqlite3, os, time
- `generate_receiver_report.py`: sqlite3, os

## Notes

- Configuration (ports, IP, paths) is defined in `config.py`.
- The receiver must be started before the sender-side workers.

## Outputs

- Runtime SQLite DB: `Receiver/receiver_state.db`
- Reassembled payloads: `Receiver/received/`
- Reports/plots: `Receiver/results/`
