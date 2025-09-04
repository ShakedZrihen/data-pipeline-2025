# Supermarket Data Pipeline

A complete data pipeline for crawling, extracting, and enriching supermarket product data using Docker containers.

## Overview

This pipeline consists of four main services:
- **Crawler**: Downloads supermarket data files and uploads them to S3
- **Extractor**: Processes S3 files and extracts product data to RabbitMQ queues
- **Enricher**: Consumes data from RabbitMQ, enriches it with automated brand extraction, and saves to PostgreSQL
- **API Server**: RESTful API to query the processed data

## Prerequisites

- Docker and Docker Compose installed
- At least 4GB RAM available
- Internet connection for downloading data

## Quick Start

### 1. Clone and Navigate
```bash
cd salim/Project
```

### 2. Start the Entire Pipeline
```bash
docker-compose up -d
```

This will start all services:
- S3 Simulator (LocalStack)
- RabbitMQ Server
- PostgreSQL Database
- Crawler Service
- Extractor Service
- Enricher Service
- API Server

### 3. Check Service Status
```bash
docker-compose ps
```

### 4. Monitor Pipeline Progress
```bash
# View all logs
docker-compose logs -f

# View specific service logs
docker-compose logs -f crawler
docker-compose logs -f extractor
docker-compose logs -f enricher
docker-compose logs -f api
```

## Pipeline Flow

### Phase 1: Data Collection
1. **Crawler** downloads supermarket data files (.gz format)
2. **Crawler** uploads files to S3 bucket (`test-bucket`)
3. **S3** stores the raw data files

### Phase 2: Data Extraction
1. **Extractor** downloads files from S3
2. **Extractor** parses XML data and converts to JSON
3. **Extractor** publishes data to RabbitMQ queues:
   - `pricefull_queue` (product pricing data)
   - `promofull_queue` (promotional data)

### Phase 3: Data Enrichment
1. **Enricher** consumes messages from RabbitMQ queues
2. **Enricher** uses Claude API to extract brand information from Hebrew product names
3. **Enricher** saves enriched data to PostgreSQL database

### Phase 4: Data Access
1. **API Server** provides REST endpoints to query the data
2. **PostgreSQL** stores all processed and enriched data

## Monitoring the Pipeline

### Check Service Status
```bash
# View all running services
docker-compose ps

# Check specific service
docker-compose ps crawler
docker-compose ps extractor
docker-compose ps enricher
docker-compose ps api
```

### View Service Logs
```bash
# Recent logs (last 50 lines)
docker-compose logs --tail=50 enricher

# Follow logs in real-time
docker-compose logs -f enricher

# View logs since specific time
docker-compose logs --since="1h" enricher
```

### Check Data Progress
```bash
# Check S3 bucket contents
docker exec s3-simulator awslocal s3 ls s3://test-bucket/

# Check RabbitMQ queue status
docker exec rabbitmq-server rabbitmq-diagnostics -q list_queues name messages

# Check database table counts
docker exec postgres-db psql -U postgres -d postgres -c "SELECT COUNT(*) FROM items;"
docker exec postgres-db psql -U postgres -d postgres -c "SELECT COUNT(*) FROM stores;"
```

## Testing the API

Once the pipeline is running, test the API endpoints:

### Health Check
```bash
curl http://localhost:3001/health
```

### Get Products
```bash
# Get all products
curl http://localhost:3001/api/products

# Get limited products
curl http://localhost:3001/api/products?limit=10

# Search products
curl http://localhost:3001/api/products?q=bread
```

### Get Supermarkets
```bash
curl http://localhost:3001/api/supermarkets
```

### API Documentation
```bash
# Swagger UI
curl http://localhost:3001/api-docs
```

## Stopping the Pipeline

### Stop All Services
```bash
docker-compose down
```

### Stop Specific Service
```bash
docker-compose stop enricher
```

### Stop and Remove Volumes (Warning: Data Loss)
```bash
docker-compose down -v
```

## Troubleshooting

### Common Issues

#### 1. S3 Bucket Not Created
```bash
# Check if bucket exists
docker exec s3-simulator awslocal s3 ls

# Create bucket manually if needed
docker exec s3-simulator awslocal s3 mb s3://test-bucket
```

#### 2. Enricher Not Processing Data
```bash
# Check enricher logs
docker-compose logs enricher

# Restart enricher service
docker-compose restart enricher
```

#### 3. Database Connection Issues
```bash
# Check database health
docker exec postgres-db psql -U postgres -d postgres -c "SELECT 1;"

# Restart database
docker-compose restart postgres
```

#### 4. RabbitMQ Connection Issues
```bash
# Check RabbitMQ status
docker exec rabbitmq-server rabbitmq-diagnostics ping

# Restart RabbitMQ
docker-compose restart rabbitmq
```

### Service Restart Commands
```bash
# Restart entire pipeline
docker-compose restart

# Restart specific service
docker-compose restart crawler
docker-compose restart extractor
docker-compose restart enricher
docker-compose restart api
```

## Project Structure
