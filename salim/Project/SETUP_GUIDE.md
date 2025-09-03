# Teacher Setup Guide - Data Pipeline

This guide will help you set up and run the complete data pipeline using Docker.

## Prerequisites

1. **Docker Desktop** - Download and install from [https://www.docker.com/products/docker-desktop/](https://www.docker.com/products/docker-desktop/)
2. **Windows 10/11** - The scripts are designed for Windows
3. **At least 4GB RAM** - Docker needs sufficient memory to run all services

## Installation Steps

### Step 1: Install Docker Desktop

1. Download Docker Desktop for Windows
2. Run the installer and follow the prompts
3. Restart your computer when prompted
4. Start Docker Desktop (it will appear in your system tray)
5. Wait for Docker to fully start (the whale icon should stop animating)

### Step 2: Verify Docker Installation

Open Command Prompt or PowerShell and run:

```bash
docker --version
docker-compose --version
```

You should see version numbers for both commands.

### Step 3: Download the Project

1. Extract the project files to a folder (e.g., `C:\data-pipeline-2025\salim\Project\`)
2. Open Command Prompt or PowerShell in that folder

## Running the Pipeline

### Option 1: Using the Batch Script (Easiest)

1. Double-click `start-pipeline.bat`
2. The script will:
   - Check if Docker is running
   - Start all services
   - Show service status
   - Monitor progress

### Option 2: Using PowerShell Script

1. Right-click `start-pipeline.ps1`
2. Select "Run with PowerShell"
3. If prompted about execution policy, type `Y` and press Enter

### Option 3: Manual Commands

1. Open Command Prompt in the project folder
2. Run: `docker-compose up -d`
3. Monitor with: `docker-compose logs -f`

## What Happens When You Start

The pipeline will start these services in order:

1. **S3 Simulator** (LocalStack) - Port 4566
2. **RabbitMQ** - Ports 5672, 15672
3. **PostgreSQL** - Port 5432
4. **Crawler** - Downloads supermarket data
5. **Extractor** - Processes downloaded files
6. **Enricher** - Enriches and stores data
7. **API** - Provides REST API access - Port 3001

## Monitoring Progress

### Check Service Status

```bash
docker-compose ps
```

### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f crawler
docker-compose logs -f extractor
docker-compose logs -f enricher
docker-compose logs -f api
```

### Access Management Interfaces

- **S3**: http://localhost:4566
- **RabbitMQ Management**: http://localhost:15672 (admin/admin)
- **API**: http://localhost:3001
- **API Documentation**: http://localhost:3001/api-docs

## Testing the Pipeline

### Wait for Services to Start

Services take 2-5 minutes to fully start. You'll know they're ready when:

1. All services show "healthy" status
2. The API responds to requests
3. No more error messages in logs

### Test the API

1. Open your browser to: http://localhost:3001/api-docs
2. Or run the test script: `test-api.bat`

### Sample API Calls

```bash
# Health check
curl http://localhost:3001/health

# Get products
curl http://localhost:3001/api/products

# Get products with limit
curl "http://localhost:3001/api/products?limit=5"
```

## Troubleshooting

### Common Issues

#### Docker Not Running
- Start Docker Desktop
- Wait for it to fully initialize
- Check system tray for Docker icon

#### Port Conflicts
If you get port conflict errors:
- Close other applications using ports 4566, 5672, 15672, 5432, or 3001
- Or change ports in `docker-compose.yml`

#### Memory Issues
- Increase Docker memory in Docker Desktop settings
- Go to Settings → Resources → Memory → Increase to 4GB+

#### Services Not Starting
- Check logs: `docker-compose logs [service-name]`
- Restart: `docker-compose restart [service-name]`
- Rebuild: `docker-compose up --build -d`

### Reset Everything

If you need to start fresh:

```bash
# Stop all services
docker-compose down

# Remove everything (data will be lost)
docker-compose down -v

# Start again
docker-compose up -d
```

## Stopping the Pipeline

### Option 1: Using the Script
Double-click `stop-pipeline.bat`

### Option 2: Manual Command
```bash
docker-compose down
```

### Option 3: Complete Cleanup
```bash
docker-compose down -v
```

## Understanding the Data Flow

1. **Crawler** downloads XML files from supermarket websites
2. **Extractor** processes these files and sends data to RabbitMQ
3. **Enricher** consumes data from RabbitMQ and stores it in PostgreSQL
4. **API** provides access to the stored data

## Expected Timeline

- **0-2 minutes**: Infrastructure services start (S3, RabbitMQ, PostgreSQL)
- **2-5 minutes**: Application services start and process data
- **5+ minutes**: Pipeline is fully operational and API is accessible

## Support

If you encounter issues:

1. Check the logs first: `docker-compose logs -f`
2. Verify Docker is running and has enough memory
3. Check if ports are available
4. Try restarting services: `docker-compose restart`

## Notes

- The pipeline uses local Docker containers, so no external services are required
- All data is stored locally in Docker volumes
- The setup includes health checks to ensure reliable startup
- Services automatically retry if dependencies aren't ready
- The pipeline can be stopped and started without losing data (unless using `-v` flag)
