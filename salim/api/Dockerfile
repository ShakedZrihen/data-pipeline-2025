FROM python:3.12-slim

WORKDIR /app
COPY . .

RUN pip install psycopg2-binary "fastapi[standard]"

CMD ["fastapi","run","main.py","--port","8000"]
