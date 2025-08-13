# Data Extractor

Extracts and processes supermarket data files from S3, converts them to JSON, and sends them to RabbitMQ queues.

## What it does

- Downloads compressed (.gz) files from S3
- Extracts them to XML, then converts to JSON
- Sends processed data to RabbitMQ queues
- Cleans up temporary files

## Quick start

### 1. Start the services
```bash
cd examples/s3-simulator
docker-compose up -d
```

### 2. Run the extractor
```bash
conda activate data-pipeline
python extractor.py --latest
```

## Commands

| Command | What it does |
|---------|-------------|
| `--latest` | Process latest files from each supermarket |
| `--all` | Process all files (can be slow) |
| `--list` | Show files in S3 bucket |
| `--no-rabbitmq` | Skip sending to RabbitMQ |
| `--keep-temp` | Keep temporary files for debugging |

## Examples

```bash
# Process latest files
python extractor.py --latest

# Process only Rami Levi files
python extractor.py --all ramilevi/

# List what's available
python extractor.py --list

# Test without RabbitMQ
python extractor.py --latest --no-rabbitmq
```
