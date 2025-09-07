#!/bin/bash

# Data Pipeline Startup Script
echo "Starting Salim Data Pipeline..."

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "docker-compose is required but not installed."
    exit 1
fi

# Create necessary directories
mkdir -p logs
mkdir -p data/postgres
mkdir -p data/rabbitmq
mkdir -p data/s3

# Start the pipeline
echo "Starting all services..."
docker-compose -f docker-compose-pipeline.yml up --build -d

# Wait for services to be ready
echo "Waiting for services to start..."
sleep 30

# Show service status
echo "Service status:"
docker-compose -f docker-compose-pipeline.yml ps

# Show logs for scheduler (main orchestrator)
echo ""
echo "Scheduler logs (last 20 lines):"
docker-compose -f docker-compose-pipeline.yml logs --tail=20 scheduler

echo ""
echo "Pipeline started successfully!"
echo ""
echo "Available endpoints:"
echo "  - API: http://localhost:8000"
echo "  - RabbitMQ Management: http://localhost:15672 (admin/admin)"
echo "  - Database: localhost:5432 (postgres/postgres)"
echo ""
echo "To view logs: docker-compose -f docker-compose-pipeline.yml logs [service-name]"
echo "To stop: docker-compose -f docker-compose-pipeline.yml down"
