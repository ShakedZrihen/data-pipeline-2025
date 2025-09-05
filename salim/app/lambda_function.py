"""
AWS Lambda adapter for the Salim FastAPI application.
This file allows the FastAPI app to run on AWS Lambda using Mangum.
"""

from mangum import Mangum
from main import app

# Create Lambda handler using Mangum adapter
handler = Mangum(app)

def lambda_handler(event, context):
    """AWS Lambda entry point"""
    return handler(event, context)