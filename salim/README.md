# Data Pipeline - How to Run

## Step-by-Step Pipeline Execution

### 1. Start S3 Simulator and Create Bucket
```bash
cd examples/s3-simulator
docker-compose up -d
python create_bucket.py //if a bucket wasnet created
```

### 2. Run the Crawler
```bash
cd salim/Project/crawler
pip install -r requirements.txt
python run.py
```

### 3. Run the Extractor
```bash
cd salim/Project/extractor
pip install -r requirements.txt
python extractor.py --latest
```

### 4. Run the Enricher - be carefull some networks dont like to coomincagte use hot spot if problem
```bash
cd salim/Project/enricher
pip install -r requirements.txt
python create_tables.py
python load_stores.py
python queue_consumer.py
```

**Enricher test options:**
- `--test [N]`: Test mode, process N messages (default: 5)
- `--pricefull-only`: Only process PriceFull messages
- `--promofull-only`: Only process PromoFull messages

**Examples:**
```bash
python queue_consumer.py --test 10
python queue_consumer.py --pricefull-only
python queue_consumer.py --test 3 --pricefull-only
```

### 5. Run the Scheduler (Optional)
```bash
cd salim/Project/scheduler
python scheduler.py
```

## Alternative Commands

**Extractor options:**
- `--latest`: Process latest files
- `--all`: Process all files
- `--list`: Show files in S3
- `--no-rabbitmq`: Skip RabbitMQ
- `--keep-temp`: Keep temp files


## Environment Setup
```bash
cp env.exapmle .env
```
