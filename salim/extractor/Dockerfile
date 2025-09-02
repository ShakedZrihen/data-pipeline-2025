FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install boto3 botocore

# Copy crawler function
COPY . .

# Run the extractor
CMD ["python", "run.py"]