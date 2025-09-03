@echo off
echo ========================================
echo    Data Pipeline - Stopping Services
echo ========================================
echo.

echo Stopping all services...
docker-compose down

echo.
echo ========================================
echo    Services stopped successfully!
echo ========================================
echo.
echo To remove all data and volumes:
echo   docker-compose down -v
echo.
echo To start services again:
echo   docker-compose up -d
echo.
pause
