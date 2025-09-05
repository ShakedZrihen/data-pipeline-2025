# Supermarket Data Pipeline

A complete data pipeline for crawling, extracting, and enriching supermarket product data using Docker containers.

## Quick Start

### 1. Clone and Navigate
```bash
cd salim/Project
```

### 2. Start the Entire Pipeline
```bash
docker-compose up -d
```

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
feat
docker-compose logs -f api
```

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