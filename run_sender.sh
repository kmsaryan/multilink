#!/bin/bash
cleanup() {
    echo "\n Stopping sender workers..."
    pkill -f sender_worker
    echo "All workers stopped."
    exit 0
}

# Trap Ctrl+C (SIGINT) and call cleanup
trap cleanup SIGINT


# Launch 3 sender workers (for Wi-Fi, 5G, SAT)
python3 /usr/local/bin/multilink/sender_worker.py 10.0.1.1 5001 &
python3 /usr/local/bin/multilink/sender_worker.py 10.0.2.1 5002 &
python3 /usr/local/bin/multilink/sender_worker.py 10.0.3.1 5003 &

# Wait indefinitely to keep the script running and allow trap to work
while true; do
    sleep 1
done
