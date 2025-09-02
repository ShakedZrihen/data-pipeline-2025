# Final Assignment — Data Pipeline (Full Docker Setup)

A comprehensive data pipeline for collecting, processing, and serving supermarket pricing data with automatic database updates.

## Architecture Overview

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Crawler   │───▶│     S3      │───▶│  Extractor  │───▶│  RabbitMQ   │
│             │    │   (MinIO)   │    │             │    │             │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                                                              │
                                                              ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│     API     │◀───│ PostgreSQL  │◀───│  Enricher   │◀───│  MongoDB    │
│  (FastAPI)  │    │             │    │             │    │             │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
```

## Data Flow & Auto-Update Process

### **1. Data Collection (Crawler)**
- **Frequency**: Every hour automatically
- **Process**: Downloads PROMO and PRICE files from all supermarkets
- **Output**: Raw data files stored in S3/MinIO

### **2. Data Processing (Extractor)**
- **Trigger**: Monitors S3 for new files
- **Process**: Extracts and transforms raw data
- **Output**: Structured JSON messages sent to RabbitMQ

### **3. Data Enrichment (Enricher)**
- **Trigger**: Processes messages from RabbitMQ
- **Process**: Enhances product data with AI (OpenAI)
- **Output**: Enriched data stored in PostgreSQL

### **4. Database Auto-Update**
- **Real-time**: Updates happen automatically as new data arrives
- **Consistency**: Maintains data integrity and relationships
- **Performance**: Optimized queries and indexing

## Installation & Setup

### **Prerequisites**
- Docker and Docker Compose
- Python 3.11+
- Chrome/Chromium (for web scraping)

### **Quick Start**
```bash
# Clone the repository
git clone <repository-url>
cd final-pipeline

# Copy environment configuration
cp config.env.example .env

# Start all services
docker compose up -d

# Run initial data collection
docker compose exec crawler python crawler/run_crawler.py
```

## API Documentation

### **Interactive API Documentation**
- **Swagger UI**: http://localhost:8000/docs - Interactive API documentation where you can test endpoints directly
- **ReDoc**: http://localhost:8000/redoc - Alternative documentation format
- **OpenAPI Schema**: http://localhost:8000/openapi.json - Raw OpenAPI specification

### **API Endpoints**

#### **Supermarkets**
- `GET /supermarkets` - List all supermarkets
- `GET /supermarkets/{supermarket_id}` - Get specific supermarket details
- `GET /supermarkets/{supermarket_id}/products` - Get products from a specific supermarket

#### **Products**
- `GET /products` - Search and filter products with query parameters:
  - `q` or `name` - Search by product name
  - `promo` - Filter products with promotions (true/false)
  - `min_price` / `max_price` - Price range filtering
  - `supermarket_id` - Filter by supermarket ID
- `GET /products/barcode/{barcode}` - Get price comparison for a specific barcode across all supermarkets

### **API Examples**
```bash
# Get all products from Carrefour (supermarket_id=1)
curl "http://localhost:8000/products?supermarket_id=1"

# Search for products containing "milk"
curl "http://localhost:8000/products?name=milk"

# Find products in price range 5-10 ILS
curl "http://localhost:8000/products?min_price=5&max_price=10"

# Get products with promotions
curl "http://localhost:8000/products?promo=true"

# Compare prices for a specific barcode
curl "http://localhost:8000/products/barcode/7290111357657"

# Get all supermarkets
curl "http://localhost:8000/supermarkets"
```

### **Using Swagger UI**
1. Open your browser and navigate to: http://localhost:8000/docs
2. Explore the available endpoints in the interactive documentation
3. Click "Try it out" on any endpoint to test it
4. Fill in parameters and click "Execute" to see real responses
5. View the response with actual data from your pipeline

## Development & Contributing

### **Code Structure**
```
final-pipeline/
├── crawler/          # Web scraping logic
├── extractor/        # Data extraction and transformation
├── enricher/         # Data enrichment and storage
├── api/             # FastAPI REST endpoints
├── shared/          # Common utilities and configuration
├── infra/           # Infrastructure setup
└── docker-compose.yml
```
## Features

### Data Collection
- **6 Supermarket Crawlers**: Goodpharm, Zolbegadol, Carrefour, Yohananof, OsherAd, TivTaam
- **Automatic Downloads**: PROMO and PRICE files from each supermarket
- **Scheduled Execution**: Runs every hour automatically
- **S3 Storage**: MinIO integration for raw data storage

### Data Processing
- **Automatic Extraction**: Processes downloaded files automatically
- **Data Transformation**: Converts raw data to structured format
- **Queue Management**: RabbitMQ for reliable message processing
- **State Tracking**: MongoDB for processing state management

### Database Auto-Update
- **Real-time Updates**: Database automatically updated with new pricing data
- **Data Enrichment**: AI-powered product information enhancement
- **Structured Storage**: PostgreSQL with optimized schema
- **Data Consistency**: Automatic validation and error handling

### API & Access
- **RESTful API**: FastAPI with automatic Swagger documentation
- **Real-time Queries**: Live access to latest pricing data
- **Advanced Filtering**: Search by product, price, supermarket, etc.
- **Hebrew Support**: Full support for Hebrew text and characters

## Database Schema

### **Supermarkets Table**
```sql
CREATE TABLE supermarkets (
    supermarket_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    branch_name VARCHAR(100),
    city VARCHAR(100),
    address TEXT,
    website VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### **Products Table**
```sql
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    supermarket_id INTEGER REFERENCES supermarkets(supermarket_id),
    barcode VARCHAR(50) NOT NULL,
    canonical_name VARCHAR(255) NOT NULL,
    brand VARCHAR(100),
    category VARCHAR(100),
    size_value DECIMAL(10,2),
    size_unit VARCHAR(50),
    price DECIMAL(10,2) NOT NULL,
    currency VARCHAR(10) DEFAULT 'ILS',
    promo_price DECIMAL(10,2),
    promo_text TEXT,
    in_stock BOOLEAN DEFAULT true,
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```




