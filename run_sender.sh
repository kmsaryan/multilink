#!/bin/bash
# Launch sender workers with keyboard interrupt handling
# NOTE: Python logging handles file writing via FileHandler
# We redirect stdout/stderr to /dev/null to avoid duplicate logs

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
mkdir -p /usr/local/bin/multilink/sender/logs

# Launch 3 sender workers (for Wi-Fi, 5G, SAT)
# Redirect stdout and stderr to /dev/null since Python FileHandler already writes to logs
python3 /usr/local/bin/multilink/sender/sender_worker.py 10.0.1.1 > /dev/null 2>&1 &
python3 /usr/local/bin/multilink/sender/sender_worker.py 10.0.2.1 > /dev/null 2>&1 &
python3 /usr/local/bin/multilink/sender/sender_worker.py 10.0.3.1 > /dev/null 2>&1 &

echo "Sender workers started. Logs in /usr/local/bin/multilink/sender/logs/"
echo "Monitor logs in real-time:"
echo "  tail -f sender/logs/sender_worker_10_0_1_1.log"
echo "  tail -f sender/logs/sender_worker_10_0_2_1.log"
echo "  tail -f sender/logs/sender_worker_10_0_3_1.log"
echo ""
echo "Press Ctrl+C to stop all workers."

# Wait indefinitely to keep the script running and allow trap to work
while true; do
    sleep 1
done
