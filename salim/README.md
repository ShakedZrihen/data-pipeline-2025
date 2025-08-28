# Salim API with PostgreSQL

A FastAPI application with PostgreSQL database running in Docker containers.

## 🚀 Quick Start

<<<<<<< HEAD
1. **Create enviorment and install dependencies:**
  ```bash
    brew install localstack/tap/localstack-cli
    python3 -m venv .venv
    pip install -r requirements.txt
    source .venv/bin/activate # linux/mac
  ```
  ### Note:
  create a .env file in the root directory and add the following env vars:
  - `OPENAI_API_KEY`: OpenAI api key.
  - `POSTGRES_URI`: URI for supabase managed db.
****
2. **Start the services:**
=======
1. **Start all services:**
>>>>>>> f0f6c403225f05b821dcd40a57935a5f7046032d
   ```bash
    docker compose build
    docker compose up
   ```

<<<<<<< HEAD
3. **Monitor directory for files:**
  ```bash
    # TODO: create a cronjob to monitor crawled file downloads.
    # and upload to s3
  ```

4. **Run crawlers:**
  ```bash
    python3 salim/crawler/yohananof.py
    python3 salim/crawler/goodpharm.py
    python3 salim/crawler/citymarket.py
  ```


5. **Access the API:**
   - API Base URL: http://localhost:8000
   - Swagger Documentation: http://localhost:8000/docs
   - ReDoc Documentation: http://localhost:8000/redoc
   - Health Check: http://localhost:8000/health

6. ## 📋 Available Endpoints
=======
2. **Access the applications:**

   **Salim API:**
   - **API Base URL:** http://localhost:8000
   - **📚 Interactive API Documentation (Swagger):** http://localhost:8000/docs
   - **📖 Alternative Documentation (ReDoc):** http://localhost:8000/redoc
   - **🔍 OpenAPI Schema:** http://localhost:8000/openapi.json
   - **❤️ Health Check:** http://localhost:8000/health

   **Shopping Chat Application:**
   - **🛒 Chat Interface:** http://localhost:5173
   - **📡 Chat API:** http://localhost:3001
   - **🏥 Chat Health Check:** http://localhost:3001/health

3. **Database Connection:**
   - Host: localhost
   - Port: 5432
   - Database: salim_db
   - Username: postgres
   - Password: postgres

4. **Connect to PostgreSQL Database:**
   ```bash
   # Using psql command line tool
   psql -h localhost -p 5432 -U postgres -d salim_db

   # Using Docker exec to connect from within the container
   docker-compose exec db psql -U postgres -d salim_db

   # Using any SQL client with the connection details above
   ```

## 📋 Available Endpoints

### General
>>>>>>> f0f6c403225f05b821dcd40a57935a5f7046032d
- `GET /` - Welcome message
- `GET /health` - Health check

### Supermarkets
- `GET /supermarkets` - Get all supermarkets
- `GET /supermarkets/{id}` - Get specific supermarket
- `GET /supermarkets/{id}/products` - Get products from a specific supermarket

### Products & Price Comparison
- `GET /products` - Search products with advanced filters
  - `?q=milk` or `?name=milk` - Search by product name
  - `?brand=Tnuva` - Filter by brand
  - `?category=Dairy` - Filter by category
  - `?promo=true` - Show only products on sale
  - `?promo=false` - Show only regular-priced products
  - `?min_price=5&max_price=20` - Price range filter
  - `?supermarket_id=1` - Filter by specific supermarket
- `GET /products/{id}` - Get specific product by database ID
- `GET /products/barcode/{barcode}` - **Compare prices** across all supermarkets for same product

### Utility
- `GET /categories` - Get all available categories
- `GET /brands` - Get all available brands
- `GET /stats` - Get database statistics

## 🛒 Shopping Chat Application

The shopping chat is an AI-powered Hebrew assistant that helps users find the best prices across Israeli supermarkets.

### Features:
- **🔍 Product Search** - "איפה הכי זול לקנות חלב?" (Where's the cheapest milk?)
- **💰 Price Comparison** - Compare prices across Rami Levi, Yohananof, and Carrefour
- **🛒 Smart Shopping Baskets** - Find the best store for your entire shopping list
- **🏷️ Promotion Detection** - Identifies sales and special offers
- **🇮🇱 Hebrew Interface** - Fully Hebrew conversation interface

### Sample Queries:
- "כמה עולה לחם בכל החנויות?" (How much is bread in all stores?)
- "איפה כדאי לי לקנות את הסל שלי?" (Where should I shop for my basket?)
- "תראה לי מוצרים במבצע" (Show me products on sale)
- "השווה מחירי חלב" (Compare milk prices)

### 📚 Interactive Documentation
The API includes comprehensive interactive documentation:

- **Swagger UI** (`/docs`) - Try out endpoints directly in your browser
- **ReDoc** (`/redoc`) - Clean, responsive API documentation
- **OpenAPI Schema** (`/openapi.json`) - Machine-readable API specification

Features:
- ✨ **Try It Out** - Execute API calls directly from the browser
- 🏷️ **Request/Response Examples** - See sample data for all endpoints
- 🔍 **Search & Filter** - Find endpoints quickly
- 📝 **Detailed Descriptions** - Comprehensive endpoint documentation
- 🏪 **Organized by Tags** - Grouped by functionality (supermarkets, products, comparison, utilities)

## 🛠️ Development

### Running Locally (without Docker)

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Start PostgreSQL (using Docker):**
   ```bash
   docker-compose up db
   ```

3. **Run the API:**
   ```bash
   uvicorn app.main:app --reload
   ```

### Stopping Services

```bash
docker-compose down
```

To remove volumes as well:
```bash
docker-compose down -v
```

## 📁 Project Structure

```
salim/
├── app/
│   ├── main.py          # FastAPI application
│   └── routes/
│       ├── __init__.py
│       └── api/
│           ├── __init__.py
│           └── health.py
├── docker-compose.yml   # Docker services configuration
├── Dockerfile          # FastAPI container configuration
├── requirements.txt    # Python dependencies
└── README.md          # This file
```

## 🔧 Configuration

The application uses environment variables for configuration:

- `DATABASE_URL`: PostgreSQL connection string (automatically set in Docker)
- `PORT`: API server port (default: 8000)

## 🐳 Docker Services

- **api**: FastAPI application (port 8000)
- **db**: PostgreSQL database (port 5432)
  **rabbitmq**: RabbitMQ message queue(port 5672)
  **localstack** Localstack local aws services(port 4566)
- **shopping-chat**: AI-powered shopping assistant (ports 3001, 5173)
  - Chat API server on port 3001
  - React frontend on port 5173
  - Integrated with Claude AI for Hebrew conversations
