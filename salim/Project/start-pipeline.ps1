# Data Pipeline - PowerShell Startup Script
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "    Data Pipeline - Starting Services" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check if Docker is running
Write-Host "Checking Docker status..." -ForegroundColor Yellow
try {
    $dockerVersion = docker --version 2>$null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Docker is running: $dockerVersion" -ForegroundColor Green
    } else {
        throw "Docker command failed"
    }
} catch {
    Write-Host "ERROR: Docker is not installed or not running!" -ForegroundColor Red
    Write-Host "Please install Docker Desktop and start it." -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit 1
}

Write-Host ""
Write-Host "Starting all services in background..." -ForegroundColor Yellow
docker-compose up -d

if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to start services!" -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit 1
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "    Services are starting up..." -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "S3 Simulator: http://localhost:4566" -ForegroundColor White
Write-Host "RabbitMQ Management: http://localhost:15672 (admin/admin)" -ForegroundColor White
Write-Host "PostgreSQL: localhost:5432" -ForegroundColor White
Write-Host "API: http://localhost:3001" -ForegroundColor White
Write-Host ""
Write-Host "To view logs: docker-compose logs -f" -ForegroundColor Gray
Write-Host "To stop services: docker-compose down" -ForegroundColor Gray
Write-Host ""

# Monitor services
Write-Host "Monitoring service status..." -ForegroundColor Yellow
Write-Host "Press Ctrl+C to stop monitoring" -ForegroundColor Gray
Write-Host ""

do {
    Clear-Host
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host "    Data Pipeline - Service Status" -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host ""
    
    # Show service status
    docker-compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"
    
    Write-Host ""
    Write-Host "Services are starting. This may take a few minutes..." -ForegroundColor Yellow
    Write-Host "Press Ctrl+C to stop monitoring" -ForegroundColor Gray
    Write-Host ""
    
    Start-Sleep -Seconds 30
} while ($true)
