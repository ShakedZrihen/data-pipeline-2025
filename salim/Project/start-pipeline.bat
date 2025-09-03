@echo off
echo ========================================
echo    Data Pipeline - Starting Services
echo ========================================
echo.

echo Checking Docker status...
docker --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Docker is not installed or not running!
    echo Please install Docker Desktop and start it.
    pause
    exit /b 1
)

echo Docker is running. Starting services...
echo.

echo Starting all services in background...
docker-compose up -d

echo.
echo ========================================
echo    Services are starting up...
echo ========================================
echo.
echo S3 Simulator: http://localhost:4566
echo RabbitMQ Management: http://localhost:15672 (admin/admin)
echo PostgreSQL: localhost:5432
echo API: http://localhost:3001
echo.
echo To view logs: docker-compose logs -f
echo To stop services: docker-compose down
echo.
echo Waiting for services to be ready...
echo.

:wait_loop
echo Checking service status...
docker-compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"
echo.

echo Services are starting. This may take a few minutes...
echo Press Ctrl+C to stop monitoring, or wait for services to be ready.
echo.

timeout /t 30 /nobreak >nul
goto wait_loop
