FROM python:3.12-slim

WORKDIR /app
COPY price-consumer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY price-consumer/ /app

CMD bash -lc "python consumer.py migrate && while true; do python consumer.py consume-batch --max-messages 10 --wait 10; sleep 2; done"
