### Build project

docker compose down -v
docker compose up -d --build localstack postgres
docker compose up -d --build crawler extractor
docker compose up -d --build price-consumer api

### Check queues

docker exec -it localstack awslocal s3 ls
docker exec -it localstack awslocal sqs list-queues

### Logs

docker logs -f final-proj-crawler-1
docker logs -f final-proj-extractor-1
docker logs -f price-consumer

### See .gz files

docker exec -it localstack awslocal s3 ls s3://price-data --recursive

# See processed json files

docker exec -it localstack awslocal s3 ls s3://govil-price-lists/processed-json --recursive

# See queue

docker exec -it localstack awslocal sqs get-queue-attributes   --queue-url http://localstack:4566/000000000000/price-extractor-events   --attribute-names ApproximateNumberOfMessages

### Check tables

docker exec -it postgres psql -U postgres -d pricedb -c "SELECT COUNT(*) FROM price_items;"
docker exec -it postgres psql -U postgres -d pricedb -c "SELECT provider,branch,product,price,ts FROM price_items ORDER BY ts DESC LIMIT 10;"

### Brand coverage

docker exec -it postgres psql -U postgres -d pricedb -c \
"SELECT COUNT(*) total,
        COUNT(brand) brand_filled,
        ROUND(100.0*COUNT(brand)/COUNT(*),1) pct_brand
 FROM price_items;"


 ### See actual separated brands

docker exec -it postgres psql -U postgres -d pricedb -c \
"SELECT product, brand, category, size_value, size_unit
 FROM price_items
 WHERE brand IS NOT NULL
 ORDER BY ts DESC
 LIMIT 20;"

### barcode check : 
docker exec -it postgres psql -U postgres -d pricedb -c \
"SELECT COUNT(*) AS with_barcode
FROM price_items
WHERE barcode IS NOT NULL;

SELECT provider, branch, product, barcode, price, ts
FROM price_items
WHERE barcode IS NOT NULL
ORDER BY ts DESC
LIMIT 20;"

 ## API

Visit: http://localhost:8000/docs


