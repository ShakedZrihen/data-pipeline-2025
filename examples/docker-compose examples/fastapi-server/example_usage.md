# FastAPI Products API - Usage Examples

## Base URL
```
http://localhost:8000
```

## Endpoints

### 1. Root endpoint
```bash
curl http://localhost:8000/
```

### 2. GET /products - Get all products with available stores
```bash
# Default (limit=100)
curl http://localhost:8000/products

# With query string - limit results
curl "http://localhost:8000/products?limit=10"

# With query string - different limit
curl "http://localhost:8000/products?limit=5"
```

**Response format:**
```json
[
  {
    "product": {
      "id": 1,
      "item_code": "670087442040",
      "item_name": "פיתות עבודת יד קלזין",
      "item_price": 14.90,
      "availableInSupers": [
        {
          "store_id": "0084",
          "store_name": "Super Pharm Ramat Gan",
          "store_type": "pharmacy",
          "city": "Ramat Gan"
        }
      ]
    }
  }
]
```

### 3. GET /supers - Get all available stores
```bash
curl http://localhost:8000/supers
```

**Response format:**
```json
[
  {
    "id": 1,
    "store_id": "0084",
    "store_name": "Super Pharm Ramat Gan",
    "store_type": "pharmacy",
    "city": "Ramat Gan"
  }
]
```

### 4. GET /products/{barcode} - Get product by barcode
```bash
# Example with existing barcode
curl http://localhost:8000/products/670087442040

# Another example
curl http://localhost:8000/products/7290004121839
```

**Response format:**
```json
{
  "product": {
    "id": 1,
    "item_code": "670087442040",
    "item_name": "פיתות עבודת יד קלזין",
    "item_price": 14.90,
    "availableInSupers": [
      {
        "store_id": "0084",
        "store_name": "Super Pharm Ramat Gan",
        "store_type": "pharmacy",
        "city": "Ramat Gan"
      }
    ]
  }
}
```

## Query String Examples

### Products endpoint with different limits:
```bash
# Get only 1 product
curl "http://localhost:8000/products?limit=1"

# Get 50 products
curl "http://localhost:8000/products?limit=50"

# Maximum limit (1000)
curl "http://localhost:8000/products?limit=1000"
```

## Error Handling

### Product not found:
```bash
curl http://localhost:8000/products/nonexistent_barcode
```
Returns 404 with:
```json
{
  "detail": "Product not found"
}
```

### Invalid limit (too high):
```bash
curl "http://localhost:8000/products?limit=2000"
```
Returns 422 validation error.

## Interactive API Documentation

Visit http://localhost:8000/docs for Swagger UI documentation.