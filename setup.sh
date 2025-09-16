
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

# Inside setup.sh
echo "Downloading Enron dataset..."
wget https://www.cs.cmu.edu/~./enron/enron_mail_20150507.tar.gz -O enron.tgz
mkdir -p maildir
tar -xzf enron.tgz -C .
rm enron.tgz
