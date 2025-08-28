# Data Pipeline: S3 → RabbitMQ → PostgreSQL

This example demonstrates a complete data pipeline that processes JSON price files through multiple stages:

1. **S3 Producer**: Uploads JSON files to LocalStack S3
2. **S3 to RabbitMQ**: Fetches files from S3 and sends individual items to RabbitMQ
3. **RabbitMQ to PostgreSQL**: Consumes messages and stores data in PostgreSQL

## Architecture

```
JSON Files → S3 (LocalStack) → RabbitMQ → PostgreSQL
```

## Services

- **LocalStack**: S3-compatible storage simulation
- **RabbitMQ**: Message broker with management UI
- **PostgreSQL**: Database for storing price items
- **S3 Producer**: Python service that uploads files to S3
- **S3 to RabbitMQ**: Python service that processes S3 files and publishes to RabbitMQ
- **RabbitMQ to PostgreSQL**: Python service that consumes messages and stores in database

## Quick Start

1. **Start all services**:
   ```bash
   docker-compose up -d
   ```

2. **Monitor logs**:
   ```bash
   docker-compose logs -f
   ```

3. **Access management interfaces**:
   - RabbitMQ Management: http://localhost:15672 (guest/guest)
   - PostgreSQL: localhost:5432 (postgres/postgres)

## Data Flow

1. The S3 Producer uploads the JSON file to LocalStack S3 every 60 seconds
2. S3 to RabbitMQ service monitors the S3 bucket and processes new files
3. Each price item from the JSON is sent as a separate message to RabbitMQ
4. RabbitMQ to PostgreSQL service consumes messages and stores them in the database

## Database Schema

The `price_items` table stores the processed data with the following structure:

- `id`: Primary key
- `source_file`: Original S3 file path
- `processed_at`: When the message was processed
- `item_code`, `item_name`, `manufacturer_name`: Product information
- `item_price`, `unit_of_measure_price`: Pricing data
- `quantity`, `unit_qty`, `unit_of_measure`: Quantity information
- `price_update_date`: When the price was last updated
- `item_status`, `allow_discount`, `is_weighted`: Status flags
- `item_id`: Item identifier
- `raw_data`: Full JSON data (JSONB)
- `created_at`: Record creation timestamp

## Environment Variables

Each service supports configuration through environment variables:

### S3 Producer
- `S3_BUCKET`: S3 bucket name (default: price-data)
- `SOURCE_FILE`: Path to source JSON file
- `UPLOAD_INTERVAL`: Upload interval in seconds (default: 60)

### S3 to RabbitMQ
- `S3_BUCKET`: S3 bucket name (default: price-data)
- `RABBITMQ_QUEUE`: Queue name (default: price-items)
- `CHECK_INTERVAL`: Check interval in seconds (default: 30)

### RabbitMQ to PostgreSQL
- `RABBITMQ_QUEUE`: Queue name (default: price-items)
- `POSTGRES_HOST`: PostgreSQL host (default: postgres)
- `POSTGRES_DB`: Database name (default: pricedb)
- `POSTGRES_USER`: Username (default: postgres)
- `POSTGRES_PASSWORD`: Password (default: postgres)

## Monitoring

1. **Check service status**:
   ```bash
   docker-compose ps
   ```

2. **View RabbitMQ queue status**: Visit http://localhost:15672

3. **Query PostgreSQL data**:
   ```bash
   docker-compose exec postgres psql -U postgres -d pricedb -c "SELECT COUNT(*) FROM price_items;"
   ```

## Troubleshooting

- **Services not starting**: Check logs with `docker-compose logs [service-name]`
- **Connection errors**: Ensure all services are healthy before dependent services start
- **Data not flowing**: Check RabbitMQ management UI for queue status
- **Database issues**: Verify PostgreSQL is accepting connections

## Cleanup

Stop and remove all services:
```bash
docker-compose down -v
```

This will also remove the persistent volumes containing S3, RabbitMQ, and PostgreSQL data.