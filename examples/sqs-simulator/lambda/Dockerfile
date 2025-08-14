FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install boto3 botocore

# Copy lambda function
COPY handler.py .

# Expose port
EXPOSE 8081

# Run the handler
CMD ["python", "handler.py"]