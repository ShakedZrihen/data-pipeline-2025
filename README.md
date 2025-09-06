# Salim API with PostgreSQL

A FastAPI application with PostgreSQL database running in Docker containers.

## 🚀 Quick Start

1. **Start the services:**
   ```bash
   docker-compose up --build
   ```

2. **Access the API:**
   - API Base URL: http://localhost:8000
   - Swagger Documentation: http://localhost:8000/docs
   - ReDoc Documentation: http://localhost:8000/redoc
   - Health Check: http://localhost:8000/health

3. **Database Connection:**
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
