FROM python:3.13-slim

WORKDIR /app

COPY . .

RUN pip install python-dotenv psycopg2-binary "fastapi[standard]"

CMD ["fastapi","run","main.py","--port","8000"]
