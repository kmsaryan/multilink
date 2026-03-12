# Multilink

A multi-path UDP file transfer system that distributes data across multiple network interfaces (Wi-Fi, 5G, Satellite) between two VMs. The sender side monitors link health, predicts interface quality, and routes chunks dynamically; the receiver side reassembles them and generates delivery reports.

## Repository Structure

```
multilink/
├── sender/        # VM1 — sender-side pipeline (health checking, orchestration, transmission)
└── Receiver/      # VM2 — receiver-side pipeline (chunk assembly, reporting)
```

### `sender/`

Contains all scripts that run on the sending VM (VM1). Responsible for:

- Probing each network interface for RTT, jitter, and packet loss
- Predicting future link quality using historical metrics
- Assigning and routing payload chunks across interfaces
- Transmitting UDP chunks and handling acknowledgements
- Ingesting payload files into the coordination database

See [`sender/README.md`](sender/README.md) for setup and execution instructions.

### `Receiver/`

Contains all scripts that run on the receiving VM (VM2). Responsible for:

- Listening for incoming UDP chunks on all interfaces
- Tracking reassembly state via SQLite
- Generating delivery reports once a transfer completes

See [`Receiver/README.md`](Receiver/README.md) for setup and execution instructions.
