FROM python:3.11-slim

WORKDIR /app

# Copy requirements from parent directory
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy entire salim directory to maintain module structure
COPY . ./salim/

# Set PYTHONPATH to include the app directory
ENV PYTHONPATH=/app

# Run consumer
CMD ["python", "salim/consumer/main.py"]
