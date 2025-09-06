FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir  fastapi  "uvicorn[standard]"  sqlalchemy  psycopg2-binary  python-dotenv pydantic
# Copy api files
COPY . /app/api

EXPOSE 8000

# Run the api
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]