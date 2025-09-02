#!/bin/bash

echo "Data Pipeline Quick Start"
echo "============================"

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "ERROR: Docker is not running. Please start Docker and try again."
    exit 1
fi

# Check if .env exists, if not create from template
if [ ! -f .env ]; then
    echo "Creating .env file from template..."
    if [ -f config.env.example ]; then
        cp config.env.example .env
        echo "SUCCESS: .env file created from config.env.example"
    else
        echo "ERROR: config.env.example not found. Please create .env manually."
        exit 1
    fi
else
    echo "SUCCESS: .env file already exists"
fi

# Start the pipeline
echo "Starting the data pipeline..."
docker compose up --build -d

# Wait for services to be ready
echo "Waiting for services to be ready..."
sleep 30

# Check service status
echo "Checking service status..."
docker compose ps

echo ""
echo "Pipeline is starting up!"
echo ""
echo "Service URLs:"
echo "  - API Docs: http://localhost:8000/docs"
echo "  - MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
echo "  - RabbitMQ Management: http://localhost:15672 (guest/guest)"
echo ""
echo "Next steps:"
echo "  1. Wait a few more minutes for all services to be ready"
echo "  2. Check the API docs at: http://localhost:8000/docs"
echo "  3. Monitor logs: docker compose logs -f"
echo ""
echo "To check logs:"
echo "  docker compose logs -f [service_name]"
echo ""
echo "To stop:"
echo "  docker compose down"
