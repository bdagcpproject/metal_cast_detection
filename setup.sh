#!/bin/bash
# Create a Python virtual environment and install dependencies.
python3 -m venv metal_cast_detection
source env/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
echo "Setup complete. Activate your environment with: source env/bin/activate."