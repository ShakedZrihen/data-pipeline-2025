# Data Pipeline - Docker Setup

This project implements a complete data pipeline for supermarket data crawling, extraction, enrichment, and API access using Docker containers.

## Architecture

The pipeline consists of the following services:

1. **S3 Simulator (LocalStack)** - Stores crawled data files
2. **RabbitMQ** - Message queue for data processing
3. **PostgreSQL** - Database for enriched data
4. **Crawler** - Python service that crawls supermarket websites
5. **Extractor** - Python service that processes S3 files and sends to RabbitMQ
6. **Enricher** - Python service that processes data from RabbitMQ and stores in database
7. **API** - Node.js service that provides REST API access to the data

## Prerequisites

- Docker Desktop installed and running
- At least 4GB of available RAM
- Internet connection for downloading Docker images

## Quick Start

### 1. Start the Pipeline

```bash
# Navigate to the project directory
cd salim/Project

# Start all services
docker-compose up -d
```

This will start all services in the background. You can monitor the logs with:

```bash
# View all logs
docker-compose logs -f

# View specific service logs
docker-compose logs -f crawler
docker-compose logs -f extractor
docker-compose logs -f enricher
docker-compose logs -f api
```

### 2. Monitor Service Status

```bash
# Check service status
docker-compose ps

# Check service health
docker-compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"
```

## Service Details

### S3 Simulator (LocalStack)
- **Port**: 4566
- **Purpose**: Simulates AWS S3 for local development
- **Bucket**: `test-bucket` (created automatically)
- **Access**: http://localhost:4566

### RabbitMQ
- **Ports**: 5672 (AMQP), 15672 (Management UI)
- **Credentials**: admin/admin
- **Management UI**: http://localhost:15672
- **Purpose**: Message queue for data processing

### PostgreSQL
- **Port**: 5432
- **Database**: postgres
- **Credentials**: postgres/8HeXmxYnvy5xu
- **Purpose**: Stores enriched supermarket data

### Crawler Service
- **Purpose**: Crawls supermarket websites and uploads data to S3
- **Configs**: yohananof, osherad, ramilevi, tivtaam, keshet, doralon
- **Output**: XML files uploaded to S3

### Extractor Service
- **Purpose**: Downloads files from S3, extracts them, and sends to RabbitMQ
- **Input**: S3 files
- **Output**: Messages to RabbitMQ queue

### Enricher Service
- **Purpose**: Processes messages from RabbitMQ and enriches data
- **Input**: RabbitMQ messages
- **Output**: Enriched data in PostgreSQL database

### API Service
- **Port**: 3001
- **Purpose**: REST API for querying supermarket data
- **Access**: http://localhost:3001
- **Documentation**: http://localhost:3001/api-docs

## Pipeline Flow

1. **Crawler** → Downloads data from supermarket websites → Uploads to S3
2. **Extractor** → Downloads from S3 → Extracts files → Sends to RabbitMQ
3. **Enricher** → Consumes from RabbitMQ → Processes data → Stores in PostgreSQL
4. **API** → Queries PostgreSQL → Provides REST API access

## Manual Execution (if needed)

If you need to run services manually or troubleshoot:

### Run Crawler Only
```bash
docker-compose up crawler
```

### Run Extractor Only
```bash
docker-compose up extractor
```

### Run Enricher Only
```bash
docker-compose up enricher
```

### Run API Only
```bash
docker-compose up api
```

## Troubleshooting

### Common Issues

1. **Port conflicts**: Ensure ports 4566, 5672, 15672, 5432, and 3001 are available
2. **Memory issues**: Increase Docker memory allocation in Docker Desktop settings
3. **Service dependencies**: Services wait for dependencies to be healthy before starting

### Reset Everything

```bash
# Stop all services
docker-compose down

# Remove all containers, networks, and volumes
docker-compose down -v

# Rebuild and start
docker-compose up --build -d
```

### Check Logs

```bash
# View logs for a specific service
docker-compose logs [service-name]

# Follow logs in real-time
docker-compose logs -f [service-name]

# View last 100 lines
docker-compose logs --tail=100 [service-name]
```

### Database Access

```bash
# Connect to PostgreSQL
docker exec -it postgres-db psql -U postgres -d postgres

# List tables
\dt

# Query data
SELECT * FROM products LIMIT 5;
```

## API Usage

Once the pipeline is running, you can access the API:

- **Base URL**: http://localhost:3001
- **Swagger Docs**: http://localhost:3001/api-docs
- **Health Check**: http://localhost:3001/health

### Example API Calls

```bash
# Get all products
curl http://localhost:3001/api/products

# Get products by supermarket
curl http://localhost:3001/api/products?supermarket=ramilevi

# Get products on sale
curl http://localhost:3001/api/products?on_sale=true
```

## Stopping the Pipeline

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (data will be lost)
docker-compose down -v
```

## Development

### Adding New Supermarkets

1. Add configuration in `crawler/configs/`
2. Update `crawler/run.py` with new config name
3. Rebuild crawler service: `docker-compose build crawler`

### Modifying API

1. Edit files in `api/` directory
2. Rebuild API service: `docker-compose build api`
3. Restart: `docker-compose up -d api`

## Support

If you encounter issues:

1. Check service logs: `docker-compose logs [service-name]`
2. Verify service health: `docker-compose ps`
3. Check resource usage: `docker stats`
4. Restart services: `docker-compose restart [service-name]`

## Notes

- The pipeline is designed to run completely in Docker containers
- All services are configured to wait for dependencies before starting
- Data persistence is handled through Docker volumes
- The setup includes health checks for reliable service startup
- Environment variables are configured in the docker-compose.yml file 