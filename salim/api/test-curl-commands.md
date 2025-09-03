# 🧪 API Testing with cURL Commands

Use these cURL commands to test your Supermarket API endpoints manually.

## 🏥 Health & Info Endpoints

### Health Check
```bash
curl http://localhost:3001/health
```

### API Root Info
```bash
curl http://localhost:3001/
```

## 🏪 Supermarkets Endpoints

### Get All Supermarkets
```bash
curl http://localhost:3001/api/supermarkets
```

### Get All Chains
```bash
curl http://localhost:3001/api/supermarkets/chains
```

### Get Supermarket by ID
```bash
curl http://localhost:3001/api/supermarkets/1
```

### Get Products from Specific Supermarket
```bash
curl http://localhost:3001/api/supermarkets/1/products
```

## 🔍 Products Endpoints

### Get All Products (with limit)
```bash
curl "http://localhost:3001/api/products?limit=10"
```

### Search Products by Name
```bash
curl "http://localhost:3001/api/products?name=חלב"
```

### Search Products by Hebrew Name
```bash
curl "http://localhost:3001/api/products?name=אסקימו"
```

### Filter by Price Range
```bash
curl "http://localhost:3001/api/products?min_price=5&max_price=20"
```

### Filter by Supermarket
```bash
curl "http://localhost:3001/api/products?supermarket_id=1"
```

### Filter by Chain
```bash
curl "http://localhost:3001/api/products?chain_id=7290058140886"
```

### Get Products by Chain
```bash
curl http://localhost:3001/api/products/chain/7290058140886
```

### Search by Barcode
```bash
curl http://localhost:3001/api/products/barcode/7290112498892
```

### Multiple Filters Combined
```bash
curl "http://localhost:3001/api/products?name=שוקולד&min_price=1&max_price=50&limit=5"
```

## ⚠️ Error Handling Tests

### Invalid Supermarket ID (should return 404)
```bash
curl http://localhost:3001/api/supermarkets/99999
```

### Invalid Route (should return 404)
```bash
curl http://localhost:3001/api/nonexistent
```

## 🎯 Advanced Testing

### Pretty Print JSON (with jq if available)
```bash
curl http://localhost:3001/api/supermarkets | jq '.'
```

### Test with Headers
```bash
curl -H "Accept: application/json" http://localhost:3001/api/products
```

### Test Rate Limiting
```bash
# Run this multiple times quickly
for i in {1..10}; do curl http://localhost:3001/health; echo "Request $i"; done
```

## 🚀 PowerShell Commands (Windows)

### Health Check
```powershell
Invoke-RestMethod -Uri "http://localhost:3001/health" -Method Get
```

### Get All Supermarkets
```powershell
Invoke-RestMethod -Uri "http://localhost:3001/api/supermarkets" -Method Get
```

### Search Products
```powershell
Invoke-RestMethod -Uri "http://localhost:3001/api/products?name=חלב" -Method Get
```

## 📊 Expected Results

### Health Check Response
```json
{
  "status": "OK",
  "timestamp": "2025-01-07T...",
  "uptime": 123.45
}
```

### Supermarkets Response
```json
[
  {
    "supermarket_id": 1,
    "chain_name": "רמי לוי שיווק השקמה",
    "store_name": "תלפיות",
    "address": "האומן,15",
    "city": "3001"
  }
]
```

### Products Response
```json
[
  {
    "item_id": 2,
    "canonical_name": "אסקימו דובדבן אבטיח",
    "price": 5.90,
    "brand": "נסטלה",
    "supermarket_name": "רמי לוי שיווק השקמה"
  }
]
```

## 🔧 Troubleshooting

### If you get "Connection refused":
- Make sure your API server is running: `npm start`
- Check the port: `http://localhost:3001`

### If you get "404 Not Found":
- Check the URL path is correct
- Verify the route exists in your API

### If you get "500 Internal Server Error":
- Check your database connection
- Look at the server logs for errors

## 🎉 Success Indicators

✅ **All endpoints return 200 status**  
✅ **JSON responses are properly formatted**  
✅ **Data matches your database content**  
✅ **Error handling works (404 for invalid routes)**  
✅ **Filters and search work correctly**  
✅ **Hebrew text displays properly**
