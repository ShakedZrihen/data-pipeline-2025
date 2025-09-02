FROM python:3.11-slim
WORKDIR /app
COPY ./enricher/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ./enricher/main.py /app/main.py
COPY ./enricher/models.py /app/models.py
COPY ./enricher/db.py /app/db.py
COPY ./enricher/openai_enricher.py /app/openai_enricher.py
COPY ./shared /app/shared
CMD ["python", "main.py"]
