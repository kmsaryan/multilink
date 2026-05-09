# Multilink

Multilink is a proof-of-concept UDP file transfer system that distributes payload chunks across three heterogeneous network interfaces (Wi-Fi, 5G, Satellite) between two virtual machines. The sender periodically probes each interface, forecasts short-term link quality using linear regression, and assigns chunks proportionally to the best-performing paths. The receiver reassembles chunks in order and confirms delivery via sequence-numbered acknowledgements tracked in a SQLite database.

## Repository Structure

```
multilink/
├── sender/        # VM1 — health checking, prediction, orchestration, transmission
└── Receiver/      # VM2 — chunk assembly, acknowledgement, delivery reporting
```

### `sender/`

All scripts that run on the sending VM (VM1). The pipeline probes each interface for RTT, jitter, and packet loss at 2 Hz, scores interfaces using a blended linear regression predictor, assigns chunks to the highest-scoring path, transmits them over UDP, and tracks acknowledgement state in a local SQLite database. Unacknowledged chunks are detected at a configurable stale threshold and re-entered into the assignment pool without triggering retransmission storms.

See [`sender/README.md`](sender/README.md) for setup and execution instructions.

### `Receiver/`

All scripts that run on the receiving VM (VM2). The receiver listens on all interfaces concurrently, writes arriving chunks to SQLite, reassembles the payload once all sequence numbers are confirmed, and writes a delivery report with per-transfer timing and integrity data.

See [`Receiver/README.md`](Receiver/README.md) for setup and execution instructions.
