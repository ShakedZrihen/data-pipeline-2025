import json
import os
from decimal import Decimal
from typing import Dict, Any
import boto3



DATABASE_URL = os.getenv("DATABASE_URL")
DLQ_QUEUE_NAME = os.getenv("DLQ_QUEUE_NAME")
REGION = os.getenv('REGION')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
ENDPOINT_URL = os.getenv('ENDPOINT_URL')



sqs = boto3.client(
    'sqs',
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=REGION
)



def validate_supermarket(data: Dict[str, Any]):
    errors = []

    branch_name = data.get("branch_name")
    if not isinstance(branch_name, str) or not branch_name.strip():
        errors.append({"field": "branch_name", "message": "branch_name must be a non-empty string"})

    name = data.get("name")
    if not isinstance(name, str) or not name.strip():
        errors.append({"field": "name", "message": "name must be a non-empty string"})
    

    city = data.get("city")
    if not isinstance(city, str):
        errors.append({"field": "city", "message": "city must be a string"})

    address = data.get("address")
    if not isinstance(address, str):
        errors.append({"field": "address", "message": "address must be a string"})

    if errors:
        return False, {"type": "validation_error", "errors": errors}
    return True, {}


def validate_product(data: Dict[str, Any]):
    errors = []

    barcode = data.get("barcode")
    if not isinstance(barcode, str) or not barcode.strip():
        errors.append({"field": "barcode", "message": "barcode must be a non-empty string"})

    canonical_name = data.get("canonical_name")
    if canonical_name is not None and not isinstance(canonical_name, str):
        errors.append({"field": "canonical_name", "message": "canonical_name must be a string or None"})

    brand = data.get("brand")
    if brand is not None and not isinstance(brand, str):
        errors.append({"field": "brand", "message": "brand must be a string or None"})

    category = data.get("category")
    if category is not None and not isinstance(category, str):
        errors.append({"field": "category", "message": "category must be a string or None"})

    if errors:
        return False, {"type": "validation_error", "errors": errors}
    
    return True, {}



def send_error_message(queue_url, error_message):
    try:
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(error_message)
        )
        return response
    except Exception as e:
        print(f"Failed to send error message to SQS: {e}")
        raise


def validate_price_without_promo(data: Dict[str, Any]):

    errors = []

    product_id = data.get("product_id")
    if product_id is None or not isinstance(product_id, int):
        errors.append({"field": "product_id", "message": "product_id must be an integer and present"})

    supermarket_id = data.get("supermarket_id")
    if supermarket_id is None or not isinstance(supermarket_id, int):
        errors.append({"field": "supermarket_id", "message": "supermarket_id must be an integer and present"})

    price = data.get("price")
    if price is None:
        errors.append({"field": "price", "message": "price must be provided"})
    elif not isinstance(price, (int, float, Decimal)):
        errors.append({"field": "price", "message": "price must be a number"})

    size_value = data.get("size_value")
    if size_value is not None and not isinstance(size_value, (int, float, Decimal)):
        errors.append({"field": "size_value", "message": "size_value must be a number or None"})

    size_unit = data.get("size_unit")
    if size_unit is not None and not isinstance(size_unit, str):
        errors.append({"field": "size_unit", "message": "size_unit must be a string or None"})

    if errors:
        return False, {"type": "validation_error", "errors": errors}
    return True, {}


def validate_price_with_promo(data: Dict[str, Any]):
    
    errors = []

    product_id = data.get("product_id")
    if product_id is None or not isinstance(product_id, int):
        errors.append({"field": "product_id", "message": "product_id must be an integer and present"})

    supermarket_id = data.get("supermarket_id")
    if supermarket_id is None or not isinstance(supermarket_id, int):
        errors.append({"field": "supermarket_id", "message": "supermarket_id must be an integer and present"})

    promo_price = data.get("promo_price")
    if promo_price is None:
        errors.append({"field": "promo_price", "message": "promo_price must be provided"})
    elif not isinstance(promo_price, (int, float, Decimal)):
        errors.append({"field": "promo_price", "message": "promo_price must be a number"})

    promo_text = data.get("promo_text")
    if promo_text is not None and not isinstance(promo_text, str):
        errors.append({"field": "promo_text", "message": "promo_text must be a string or None"})

    if errors:
        return False, {"type": "validation_error", "errors": errors}
    return True, {}