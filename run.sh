#!/bin/bash
cd ~/actualyield
source ./venv/bin/activate

git pull origin main
pip install -r requirements.txt

python3 ./cli.py snapshot >> ./cli.log

# Deactivate the virtual environment
deactivate
