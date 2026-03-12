#!/bin/bash
# Launch health checker workers with keyboard interrupt handling
# NOTE: Python logging handles file writing via FileHandler
# We redirect stdout/stderr to /dev/null to avoid duplicate logs

# Function to handle cleanup on Ctrl+C
cleanup() {
    echo ""
    echo " Stopping health checker workers..."
    pkill -f health_checker.py
    echo "All workers stopped."
    exit 0
}

# Trap Ctrl+C (SIGINT) and call cleanup
trap cleanup SIGINT

# Create logs directory if it doesn't exist
mkdir -p /usr/local/bin/multilink/logs

# Launch health checker workers for Wi-Fi, 5G, and SAT
# Redirect stdout and stderr to /dev/null since Python FileHandler already writes to logs
python3 /usr/local/bin/multilink/health_checker.py 10.0.1.1 > /dev/null 2>&1 &
python3 /usr/local/bin/multilink/health_checker.py 10.0.2.1 > /dev/null 2>&1 &
python3 /usr/local/bin/multilink/health_checker.py 10.0.3.1 > /dev/null 2>&1 &

echo "Health checker workers started. Logs in /usr/local/bin/multilink/logs/"
echo "Monitor logs in real-time:"
echo "  tail -f logs/health_checker_10_0_1_1.log"
echo "  tail -f logs/health_checker_10_0_2_1.log"
echo "  tail -f logs/health_checker_10_0_3_1.log"
echo ""
echo "Press Ctrl+C to stop all workers."

# Wait indefinitely to keep the script running
while true; do
    sleep 1
done