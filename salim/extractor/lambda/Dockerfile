FROM python:3.9-slim

WORKDIR /app

# Install boto3
RUN pip install boto3

# Copy the lambda function
COPY handler.py .

# Run the handler
CMD ["python", "handler.py"]