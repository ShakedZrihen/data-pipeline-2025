#!/bin/bash

# Test Pipeline Script
echo "Starting Data Pipeline Test Environment..."

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "docker-compose is required but not installed."
    exit 1
fi

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "Creating .env file from example..."
    cp env.example .env
    echo "Please edit .env file with your configuration before continuing."
    echo "For testing, you can use the default values."
    read -p "Press Enter to continue with default test configuration..."
fi

# Start the test environment
echo "Starting test services..."
docker-compose -f docker-compose-test.yml up --build -d

# Wait for services to be ready
echo "Waiting for services to start..."
sleep 30

echo "Test environment started!"
echo ""
echo "Available test endpoints:"
echo "  - Test API: http://localhost:8001"
echo "  - Test RabbitMQ Management: http://localhost:15673 (admin/admin)"
echo "  - Test Database: localhost:5433 (postgres/postgres)"
echo ""
echo "Test Commands:"
echo "  1. Test Crawler (downloads 2 files max):"
echo "     docker-compose -f docker-compose-test.yml run --rm test-crawler"
echo ""
echo "  2. Test Extractor (processes latest files):"
echo "     docker-compose -f docker-compose-test.yml run --rm test-extractor"
echo ""
echo "  3. View Enricher logs (processes with limits):"
echo "     docker-compose -f docker-compose-test.yml logs -f test-enricher"
echo ""
echo "  4. Check database contents:"
echo "     docker-compose -f docker-compose-test.yml exec db psql -U postgres -d salim_test_db -c 'SELECT COUNT(*) FROM items;'"
echo ""
echo "  5. Check processed files:"
echo "     docker-compose -f docker-compose-test.yml exec db psql -U postgres -d salim_test_db -c 'SELECT * FROM processed_files;'"
echo ""
echo "To stop test environment:"
echo "  docker-compose -f docker-compose-test.yml down -v"
