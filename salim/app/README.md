# Salim API

A comprehensive REST API for supermarket data management, providing endpoints for products, promotions, and store information.

## 📁 Project Structure

```
app/
├── __init__.py
├── main.py                    # FastAPI application entry point
├── core/
│   ├── __init__.py
│   ├── config.py             # Application configuration
│   └── database.py           # Database connection setup
├── models/
│   ├── __init__.py
│   └── product.py            # Data models
├── schemas/
│   ├── __init__.py
│   └── product.py            # Pydantic request/response models
├── services/
│   ├── __init__.py
│   └── product_service.py    # Business logic layer
├── routes/
│   ├── __init__.py
│   ├── api.py               # Main API router
│   ├── products.py          # Product endpoints
│   ├── promotions.py        # Promotion endpoints
│   └── stores.py            # Store endpoints
└── utils/
    ├── __init__.py
    └── helpers.py           # Utility functions
```

## 🏗️ Architecture

This application follows a **layered architecture** pattern:

1. **Routes Layer** (`routes/`): HTTP request handling and response formatting
2. **Services Layer** (`services/`): Business logic and data processing
3. **Models Layer** (`models/`): Data structure definitions
4. **Schemas Layer** (`schemas/`): Request/response validation with Pydantic
5. **Core Layer** (`core/`): Configuration and infrastructure
6. **Utils Layer** (`utils/`): Common utilities and helpers

## 🚀 API Endpoints

### Base URL
- Development: `http://localhost:8000`
- API Documentation: `http://localhost:8000/docs`
- Alternative Documentation: `http://localhost:8000/redoc`

### Health Check
- `GET /` - Welcome message and API information
- `GET /health` - Health check endpoint

### Products API (`/api/v1/products`)

#### Get Product by Barcode
```
GET /api/v1/products/{item_code}
```
Returns all store occurrences of a product by barcode.

**Response:**
```json
{
  "products": [
    {
      "item_code": "7290000123456",
      "item_name": "מוצר לדוגמה",
      "store_id": "001",
      "chain_id": "7290027600007",
      "has_promotion": true,
      "discount_rate": 0.15,
      "price": 12.90,
      "store_address": "רחוב הדגמה 123, תל אביב"
    }
  ],
  "total": 1
}
```

#### Search Products by Name
```
GET /api/v1/products?q={search_query}
```
Case-insensitive search on product name.

**Parameters:**
- `q` (required): Search query (minimum 2 characters)

### Promotions API (`/api/v1/promotions`)

#### Get Promotions Sample
```
GET /api/v1/promotions?limit={limit}
```
Returns a sample of promotions for debugging.

**Parameters:**
- `limit` (optional): Maximum number of promotions to return (1-100, default: 25)

### Stores API (`/api/v1/stores`)

#### Get Stores
```
GET /api/v1/stores?chain_id={chain_id}
```
Returns unique stores, optionally filtered by chain.

**Parameters:**
- `chain_id` (optional): Filter stores by chain ID

## 🔧 Configuration

The application uses environment variables for configuration:

- `SUPABASE_URL`: Supabase project URL
- `SUPABASE_KEY`: Supabase anon/public key
- `DEBUG`: Enable debug mode (default: false)

Environment files are loaded in the following order:
1. Project root `.env` file
2. `consumer/.env` file (fallback)

## 📝 HTTP Status Codes

The API uses standard HTTP status codes:

- `200 OK`: Successful request
- `404 Not Found`: Resource not found
- `422 Unprocessable Entity`: Validation error
- `500 Internal Server Error`: Server error

## 🔍 Error Responses

All error responses follow a consistent format:

```json
{
  "error": "Error message",
  "detail": "Additional error details (optional)"
}
```

## 🧪 Running the Application

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Set up environment variables in `.env` file

3. Run the application:
   ```bash
   python -m app.main
   ```
   
   Or using uvicorn directly:
   ```bash
   uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
   ```

## 📖 API Documentation

Once the application is running, visit:
- Interactive API docs (Swagger UI): `http://localhost:8000/docs`
- Alternative API docs (ReDoc): `http://localhost:8000/redoc`

## 🎯 Features

- ✅ **RESTful API Design**: Follows REST principles with proper HTTP methods and status codes
- ✅ **Data Validation**: Request/response validation using Pydantic schemas
- ✅ **Error Handling**: Comprehensive error handling with meaningful messages
- ✅ **API Documentation**: Auto-generated interactive documentation
- ✅ **Layered Architecture**: Clean separation of concerns
- ✅ **Environment Configuration**: Flexible configuration management
- ✅ **CORS Support**: Cross-origin resource sharing enabled
- ✅ **Health Checks**: Built-in health check endpoints

## 🔄 Migration from Previous Version

The new structure provides:
1. **Better Organization**: Clear separation of concerns with dedicated directories
2. **Type Safety**: Pydantic models for request/response validation
3. **Error Handling**: Consistent error responses with proper HTTP status codes
4. **Documentation**: Auto-generated API documentation
5. **Maintainability**: Modular code structure for easier maintenance
6. **Scalability**: Easy to add new endpoints and features
