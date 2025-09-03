# Pipeline Flow


The pipeline runs every 10 minutes and processes data in small chunks.

## The 10-minute cycle

1. Crawler gets 2 files per store, puts them in S3
2. Extractor takes 5 files from S3, sends to RabbitMQ  
3. Enricher processes 50 messages, saves to database
4. Wait 10 minutes
5. Repeat

## Limits

**Crawler**: Max 2 files per store, 3 minute timeout
**Extractor**: Max 5 files per run, 5 minute timeout  
**Enricher**: Max 50 messages per run, 5 minute timeout


## Docker

```bash
docker-compose up
```

