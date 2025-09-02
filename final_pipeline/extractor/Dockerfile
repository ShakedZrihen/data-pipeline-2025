FROM python:3.11-slim
WORKDIR /app
COPY ./extractor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ./extractor/main.py /app/main.py
COPY ./extractor/provider_parsers.py /app/provider_parsers.py
COPY ./shared /app/shared
CMD ["python", "main.py"]
