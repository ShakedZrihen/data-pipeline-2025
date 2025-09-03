@echo off
echo ========================================
echo    Testing Data Pipeline API
echo ========================================
echo.

echo Testing API endpoints...
echo.

echo 1. Testing health check...
curl -s http://localhost:3001/health
echo.
echo.

echo 2. Testing products endpoint...
curl -s http://localhost:3001/api/products
echo.
echo.

echo 3. Testing products with limit...
curl -s "http://localhost:3001/api/products?limit=5"
echo.
echo.

echo 4. Testing products by supermarket...
curl -s "http://localhost:3001/api/products?supermarket=ramilevi&limit=3"
echo.
echo.

echo ========================================
echo    API Test Complete
echo ========================================
echo.
echo If you see JSON responses above, the API is working!
echo If you see connection errors, wait a bit longer for services to start.
echo.
pause
