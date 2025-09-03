# Data Enricher

## How to Run

### Prerequisites
- Python 3.8+
- PostgreSQL database
- RabbitMQ server
- Claude API access (for brand extraction)

### Setup
1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set up environment variables in `.env`:
```env
DATABASE_URL=postgresql://username:password@host:port/database
RABBITMQ_HOST=localhost
RABBITMQ_PORT=5672
RABBITMQ_USERNAME=admin
RABBITMQ_PASSWORD=admin
PRICEFULL_QUEUE=pricefull_queue
PROMOFULL_QUEUE=promofull_queue
DLQ_QUEUE=dead_letter_queue
BATCH_SIZE=10
TEST_MODE=false
```

3. Create database tables:
```bash
python create_tables.py
```

### Run the Enricher
```bash
python queue_consumer.py
```

## Configuration Options
- **TEST_MODE**: Limits processing to first 20 items for testing
- **PRICEFULL_ONLY**: Process only price data queues
- **PROMOFULL_ONLY**: Process only promotion data queues
- **BATCH_SIZE**: Number of messages to process in each batch
