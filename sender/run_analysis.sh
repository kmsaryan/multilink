#!/bin/bash
# Network analysis wrapper - runs analysis with proper logging

cd /usr/local/bin/multilink/sender

echo "Starting network fluctuation analysis..."
echo ""

# Check if orchestrator.log exists
if [ ! -f "logs/orchestrator.log" ]; then
    echo "ERROR: orchestrator.log not found!"
    echo "Please run the orchestrator first:"
    echo "  python3 orchestrator.py"
    exit 1
fi

# Run the analysis script
python3 analyze_network_fluctuations.py

echo ""
echo "Analysis complete!"
echo "Full report saved to: logs/network_analysis.log"
echo ""
echo "View detailed results:"
echo "  cat logs/network_analysis.log"
