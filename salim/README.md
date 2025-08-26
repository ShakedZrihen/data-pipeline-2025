# Salim API with PostgreSQL

A FastAPI application with PostgreSQL database running in Docker containers.

## 🚀 Quick Start

1. **Create enviorment and install dependencies:**
  ```bash
    brew install localstack/tap/localstack-cli
    python3 -m venv .venv
    pip install -r requirements.txt
    source .venv/bin/activate # linux/mac
  ```
  ### Note:
  create a .env file in the root directory and add the following env vars:
  - `OPENAI_API_KEY`
  - `POSTGRES_URI`
****
2. **Start the services:**
   ```bash
    docker compose build
    docker compose up
   ```

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

6. **Database Connection:**
   - Host: localhost
   - Port: 5432
   - Database: salim_db
   - Username: postgres
   - Password: postgres

## 📋 Available Endpoints

- `GET /` - Welcome message
- `GET /api/v1/health` - Basic health check
- `GET /api/v1/health/detailed` - Detailed health check with component status

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
