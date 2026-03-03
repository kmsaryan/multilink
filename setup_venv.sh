#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${ROOT_DIR}/.venv"

echo "Creating virtual environment in ${VENV_DIR}"
python3 -m venv "${VENV_DIR}"

source "${VENV_DIR}/bin/activate"
python -m pip install --upgrade pip setuptools wheel
python -m pip install -r "${ROOT_DIR}/requirements.txt"

echo
echo "Setup complete. Activate with:"
echo "source ${VENV_DIR}/bin/activate"
