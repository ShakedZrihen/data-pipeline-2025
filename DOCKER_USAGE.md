# 🐳 Docker Usage in Data Pipeline Project

This project uses Docker in multiple ways to demonstrate containerization concepts and deployment practices.

## 📁 Docker Files Overview

### 1. **AWS Lambda Packaging** 
- **File**: `salim/sqs-consumer/docker-build/Dockerfile`
- **Purpose**: Build Linux-compatible Lambda deployment packages
- **Base Image**: `public.ecr.aws/lambda/python:3.11`
- **Output**: 23MB deployment package with all dependencies

```bash
cd salim/sqs-consumer/docker-build/
docker build -t sqs-consumer-lambda:latest .
```

### 2. **Local Development Environment**
- **File**: `docker-compose.yml` 
- **Purpose**: Orchestrate entire application stack locally
- **Services**: API, Database, Redis, Crawler, Extractor, SQS Consumer

```bash
# Start entire stack
docker-compose up -d

# View logs
docker-compose logs -f

# Stop stack  
docker-compose down
```

### 3. **Local SQS Consumer Testing**
- **File**: `salim/sqs-consumer/docker-build/Dockerfile.local`
- **Purpose**: Local testing version of Lambda function
- **Difference**: Continuous polling vs Lambda event-driven

## 🚀 Usage Examples

### Build Lambda Package
```bash
cd salim/sqs-consumer/docker-build/
docker build -t sqs-consumer-lambda .

# Extract built package
docker create --name temp-container sqs-consumer-lambda
docker cp temp-container:/var/runtime/ ./lambda-package/
docker rm temp-container

# Create deployment zip
cd lambda-package/ && zip -r ../sqs-consumer-complete.zip .
```

### Local Development
```bash
# Copy environment template
cp .env.example .env
# Edit .env with your configuration

# Start development environment
docker-compose up -d postgres redis api

# Run crawler separately
docker-compose up crawler

# Monitor logs
docker-compose logs -f api crawler
```

### Test SQS Consumer Locally
```bash
# Start consumer in local mode
docker-compose up sqs-consumer-local

# Or run individually
cd salim/sqs-consumer/docker-build/
docker build -f Dockerfile.local -t sqs-consumer-local .
docker run --env-file ../../.env sqs-consumer-local
```

## 🏗️ Architecture Benefits

### **Containerization Advantages**:
1. **Linux Compatibility**: Lambda requires Linux binaries - Docker ensures compatibility
2. **Reproducible Builds**: Same environment across development and production
3. **Dependency Isolation**: No conflicts between project dependencies
4. **Easy Deployment**: Self-contained packages ready for AWS Lambda

### **Multi-Service Orchestration**:
1. **Service Discovery**: Services communicate via Docker network
2. **Environment Management**: Consistent configuration across services
3. **Scalability**: Individual services can be scaled independently
4. **Development Workflow**: Complete stack runs locally with one command

## 📊 Docker Usage Summary

| Component | Docker Usage | Purpose |
|-----------|-------------|---------|
| SQS Consumer | AWS Lambda base image | Production Lambda package |
| API Service | Python slim image | FastAPI application |
| Database | PostgreSQL Alpine | Data persistence |
| Cache | Redis Alpine | Performance optimization |
| Crawler | Custom Python image | Web scraping service |
| Extractor | Custom Python image | Data processing service |

## 🎯 Educational Value

This project demonstrates:
- **Single-container builds** (Lambda packaging)
- **Multi-container orchestration** (Docker Compose)
- **Environment-specific configurations** (local vs production)
- **Service communication** (container networking)
- **Volume management** (data persistence)
- **Production deployment patterns** (AWS Lambda containerization)