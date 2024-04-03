#!/bin/bash

cd ~/actualyield

# Activate the virtual environment
source ./venv/bin/activate

# Run the Python script
python3 ./cli.py snapshot >> ./cli.log

# Deactivate the virtual environment
deactivate
