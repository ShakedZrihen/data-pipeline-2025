# 🚀 FastAPI Setup Instructions - Israeli Supermarket Price Tracker

## 📋 Prerequisites
- Python 3.11 or higher
- Git (to clone/pull the repository)

## 📦 Files You Need (Already in the repo)

### Core API Files:
```
salim/
├── app/
│   ├── models.py           # Database models
│   ├── database.py         # Database connection
│   ├── schemas.py          # API request/response schemas
│   └── routes/
│       └── api/
│           ├── __init__.py
│           ├── health.py
│           ├── supermarkets.py  # NEW - Supermarket endpoints
│           └── products.py      # NEW - Product endpoints
```

### Test Files (Optional but helpful):
```
salim/
├── test_db_models.py       # Test database connection
├── test_api_endpoints.py   # Test all API endpoints
└── api_summary.py          # Show API summary
```

## 🛠️ Installation Steps

### 1. Navigate to the project directory
```bash
cd path/to/data-pipeline-2025/salim
```

### 2. Install required Python packages
```bash
pip install fastapi uvicorn sqlalchemy psycopg2-binary pydantic requests
```

Or use the requirements.txt:
```bash
pip install -r requirements_api.txt
```

### 3. Start the API server
```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Or on Windows with specific Python:
```bash
python -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

## ✅ Verify Installation

### 1. Check if API is running:
Open browser and go to: http://localhost:8000/docs

You should see the Swagger UI documentation.

### 2. Test the endpoints:
```bash
# Test with Python script
python test_api_endpoints.py

# Or test with curl
curl http://localhost:8000/api/v1/health/
```

### 3. See API summary:
```bash
python api_summary.py
```

## 📍 Available Endpoints

- `GET /api/v1/health/` - Health check
- `GET /api/v1/supermarkets/` - List all supermarket chains
- `GET /api/v1/supermarkets/{provider}` - Get specific supermarket
- `GET /api/v1/supermarkets/{provider}/products` - Products from supermarket
- `GET /api/v1/products` - Search products
- `GET /api/v1/products/barcode/{barcode}` - Compare prices by barcode

## 🔍 Example API Calls

### Get all supermarkets:
```bash
curl http://localhost:8000/api/v1/supermarkets/
```

### Search for products with Hebrew:
```bash
curl "http://localhost:8000/api/v1/products?name=חלב&limit=5"
```

### Compare prices by barcode:
```bash
curl http://localhost:8000/api/v1/products/barcode/7290003643820
```

## 🗄️ Database Connection

The API connects to the production Supabase database automatically. The connection string is already configured in `app/database.py`:

```python
DATABASE_URL = "postgresql://postgres.nnzvfgjldslywfofkyet:Warrockaboalmrwan@aws-1-eu-central-1.pooler.supabase.com:6543/postgres"
```

No additional database setup needed!

## 🐛 Troubleshooting

### Issue: "Module not found" errors
**Solution:** Make sure all packages are installed:
```bash
pip install fastapi uvicorn sqlalchemy psycopg2-binary pydantic
```

### Issue: Port 8000 already in use
**Solution:** Use a different port:
```bash
uvicorn app.main:app --reload --port 8001
```

### Issue: Cannot connect to database
**Solution:** Check internet connection (database is cloud-hosted)

### Issue: Hebrew text not displaying correctly
**Solution:** Make sure your terminal/browser supports UTF-8 encoding

## 📚 Documentation Links

- **Swagger UI:** http://localhost:8000/docs
- **ReDoc:** http://localhost:8000/redoc
- **OpenAPI Schema:** http://localhost:8000/openapi.json

## 💡 Quick Test

After starting the server, try this quick test:
```python
import requests

# Test health endpoint
response = requests.get("http://localhost:8000/api/v1/health/")
print(response.json())

# Get supermarkets
response = requests.get("http://localhost:8000/api/v1/supermarkets/")
print(f"Found {len(response.json())} supermarket chains")
```

## 📞 Support

If you encounter any issues:
1. Check that all files are present in the `app/` directory
2. Ensure Python 3.11+ is installed
3. Verify internet connection (for database access)
4. Check the console output for error messages

---

**Note:** The API connects to the production Supabase database with real data that updates hourly from the crawler. No local database setup is required!
