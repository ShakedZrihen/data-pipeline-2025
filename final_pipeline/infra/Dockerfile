FROM python:3.11-slim
WORKDIR /app
COPY ./infra/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ./infra/init_minio.py .
CMD ["python", "init_minio.py"]
