#!/bin/bash

# Exit immediately if any command fails
set -e

echo "Starting Docker containers..."
cd examples/s3-simulator
docker-compose up -d
cd ../..

# Wait for LocalStack to fully start
echo "Waiting for LocalStack to initialize..."
sleep 10

# Path to the virtual environment directory
VENV_DIR=".venv"

# Create the virtual environment if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
    python3 -m venv "$VENV_DIR"
    echo "Virtual environment created."
fi

# Activate the virtual environment
source "$VENV_DIR/bin/activate"

# Install Python dependencies
echo "Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Run initialization script to create S3 bucket
echo "Initializing S3 bucket..."
python3 examples/s3-simulator/init_s3.py

# Run crawler to download fresh data
echo "Running crawler to fetch data..."
cd assignments/assignment_2
python3 crawlers.py
cd ../..

# Upload files to the S3 bucket
echo "Uploading files to S3..."
cd examples/s3-simulator
python3 upload_to_s3.py
cd ../..

echo "Project setup completed."