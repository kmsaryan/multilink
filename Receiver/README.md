# Receiver

VM2 receiver-side pipeline for the Multilink proof-of-concept. Listens for incoming UDP chunks on all interfaces, tracks reassembly state in SQLite, and confirms delivery via sequence-numbered acknowledgements.

## Directory Contents

```
Receiver/
├── config.py                    # Paths, ports, and IP configuration
├── db_utils.py                  # SQLite helpers and schema initialisation
├── receiver.py                  # UDP chunk listener and payload reassembler
├── generate_receiver_report.py  # Post-transfer delivery report
├── setup_venv.sh                # Virtual environment setup
├── requirements.txt             # Python dependencies
├── received/                    # Reassembled output files
└── results/                     # Delivery reports
```

## Run Order (VM2)

Start the receiver before the sender-side pipeline.

```bash
cd /usr/local/bin/multilink/Receiver
python3 receiver.py
```

## Notes

- Configuration (ports, IPs, paths) is centralised in `config.py`
- Runtime database: `Receiver/receiver_state.db`
- Reassembled payloads: `Receiver/received/`