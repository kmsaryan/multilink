# Receiver

UDP multi-path receiver with SQLite-based tracking and reporting.

## Prerequisites

- Linux
- Python 3.10+ (3.11 recommended)

## Quick setup

```bash
chmod +x setup_venv.sh
./setup_venv.sh
source .venv/bin/activate
```

## Run receiver

```bash
python receiver.py
```

## Generate report

```bash
python generate_receiver_report.py
```

## Libraries used

### Third-party (in requirements.txt)

- pandas
- matplotlib

### Python standard library (included with Python)

- receiver.py: socket, struct, uuid, os, time, select, sqlite3
- db_utils.py: sqlite3, os, time
- generate_receiver_report.py: sqlite3, os

## Notes

- Runtime database path, ports, and output directories are defined in `config.py`.
- Current defaults use absolute paths under this repository location.

## Outputs

- Runtime SQLite DB: `receiver_state.db`
- Reassembled payloads: `received/`
- Reports/plots: `results/`
