
#!/bin/bash
set -e

# Update system
sudo apt update -y && sudo apt install -y python3-pip python3-venv

# Create venv
python3 -m venv venv
source venv/bin/activate

# Upgrade tools
pip install --upgrade pip setuptools wheel

# Install requirements
pip install -r requirements.txt

# Download spaCy model
python3 -m spacy download en_core_web_lg
