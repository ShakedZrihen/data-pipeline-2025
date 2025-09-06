FROM python:3.11-slim

WORKDIR /app

<<<<<<< HEAD
# Copy requirements first for better caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ ./app/

# Expose port
EXPOSE 8000

# Run the application
=======
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt


COPY app/ ./app/


EXPOSE 8000


>>>>>>> final-project-edit
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"] 