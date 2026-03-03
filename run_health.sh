#!/bin/bash
# Launch health checker workers with keyboard interrupt handling

# Function to handle cleanup on Ctrl+C
cleanup() {
    echo "\n Stopping health checker workers..."
    pkill -f health_checker.py
    echo "All workers stopped."
    exit 0
}

# Trap Ctrl+C (SIGINT) and call cleanup
trap cleanup SIGINT

# Launch health checker workers for Wi-Fi, 5G, and SAT
python3 /usr/local/bin/multilink/health_checker.py 10.0.1.1 &
python3 /usr/local/bin/multilink/health_checker.py 10.0.2.1 &
python3 /usr/local/bin/multilink/health_checker.py 10.0.3.1 &

# Wait indefinitely to keep the script running
while true; do
    sleep 1
done