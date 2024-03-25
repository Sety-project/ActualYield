#!/bin/bash

cd ~/ActualYield

# Activate the virtual environment
source ./.venv/bin/activate

# Run the Python script
python3 ./cli.py snapshot ./.streamlit/secrets.toml >> ./cli.log

# Deactivate the virtual environment
deactivate
