#!/bin/bash
cleanup() {
    echo ""
    echo " Stopping sender workers..."
    pkill -f sender_worker
    echo "All workers stopped."
    exit 0
}

# Trap Ctrl+C (SIGINT) and call cleanup
trap cleanup SIGINT

# Create logs directory if it doesn't exist
mkdir -p /usr/local/bin/multilink/logs

# Launch 3 sender workers (for Wi-Fi, 5G, SAT)
# Redirect both stdout and stderr (2>&1) so errors are captured in logs
python3 /usr/local/bin/multilink/sender_worker.py 10.0.1.1 >> /usr/local/bin/multilink/logs/sender_worker_10_0_1_1.log 2>&1 &
python3 /usr/local/bin/multilink/sender_worker.py 10.0.2.1 >> /usr/local/bin/multilink/logs/sender_worker_10_0_2_1.log 2>&1 &
python3 /usr/local/bin/multilink/sender_worker.py 10.0.3.1 >> /usr/local/bin/multilink/logs/sender_worker_10_0_3_1.log 2>&1 &

echo "Sender workers started. Logs in /usr/local/bin/multilink/logs/"
echo "Press Ctrl+C to stop all workers."

# Wait indefinitely to keep the script running and allow trap to work
while true; do
    sleep 1
done
