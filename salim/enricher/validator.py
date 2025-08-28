import json
import os
from decimal import Decimal
from typing import Any, Dict
import boto3
from dotenv import load_dotenv
load_dotenv()


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

    branch_number = data.get("branch_number")
    if not isinstance(branch_number, str) or not branch_number.strip():
        errors.append({"field": "branch_number", "message": "branch_number must be a non-empty string"})
    
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

    product_name = data.get("product_name")
    if not isinstance(product_name, str) or not product_name.strip():
        errors.append({"field": "product_name", "message": "product_name must be a non-empty string"})
    
    product_brand = data.get("product_brand")
    if product_brand is not None and not isinstance(product_brand, str):
        errors.append({"field": "product_brand", "message": "product_brand must be a string or None"})

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

    barcode = data.get("barcode")
    if not isinstance(barcode, str) or not barcode.strip():
        errors.append({"field": "barcode", "message": "barcode must be a non-empty string"})
    
    branch_number = data.get("branch_number")
    if not isinstance(branch_number, str) or not branch_number.strip():
        errors.append({"field": "branch_number", "message": "branch_number must be a non-empty string"})
    
    timestamp = data.get("date")
    try:
        if not isinstance(timestamp, str) or not timestamp.strip():
            raise ValueError
    except Exception:
        errors.append({"field": "date", "message": "date must be a valid ISO datetime string"})

    promo_exists = data.get("promo_exists", False)
    if not isinstance(promo_exists, bool):
        errors.append({"field": "promo_exists", "message": "promo_exists must be a boolean"})
    

    price = data.get("price")
    if not isinstance(price, (int, float, Decimal)):
        errors.append({"field": "price", "message": "price must be a number"})

    if errors:
        return False, {"type": "validation_error", "errors": errors}
    
    
    return True, {}


def validate_price_with_promo(data: Dict[str, Any]):
    errors = []

    barcode = data.get("barcode")
    if not isinstance(barcode, str) or not barcode.strip():
        errors.append({"field": "barcode", "message": "barcode must be a non-empty string"})
    

    branch_number = data.get("branch_number")
    if not isinstance(branch_number, str) or not branch_number.strip():
        errors.append({"field": "branch_number", "message": "branch_number must be a non-empty string"})
    

    timestamp = data.get("date")
    try:
        if not isinstance(timestamp, str) or not timestamp.strip():
            raise ValueError
    except Exception:
        errors.append({"field": "date", "message": "date must be a valid ISO datetime string"})

    promo_exists = data.get("promo_exists", False)
    if not isinstance(promo_exists, bool):
        errors.append({"field": "promo_exists", "message": "promo_exists must be a boolean"})
    
    promo_price = data.get("promo_price")
    if promo_price is not None:
        if not isinstance(promo_price, (int, float, Decimal)):
            print(f"Invalid promo_price: {promo_price}")
            errors.append({"field": "promo_price", "message": "promo_price must be a number"})

    promo_date_start = data.get("promo_date_start")
    if promo_date_start is not None:
        try:
            if not isinstance(promo_date_start, str) or not promo_date_start.strip():
                raise ValueError
        except Exception:
            errors.append({"field": "promo_date_start", "message": "promo_date_start must be a valid ISO datetime string"})

    promo_date_end = data.get("promo_date_end")
    if promo_date_end is not None:
        try:
            if not isinstance(promo_date_end, str) or not promo_date_end.strip():
                raise ValueError
        except Exception:
            errors.append({"field": "promo_date_end", "message": "promo_date_end must be a valid ISO datetime string"})

    promo_max_qty = data.get("promo_max_qty")
    if promo_max_qty is not None:
        if not isinstance(promo_max_qty, int):
            errors.append({"field": "promo_max_qty", "message": "promo_max_qty must be an integer"})

    promo_min_qty = data.get("promo_min_qty")
    if promo_min_qty is not None:
        if not isinstance(promo_min_qty, int):
            errors.append({"field": "promo_min_qty", "message": "promo_min_qty must be an integer"})
       
    # price = data.get("price")
    # if not isinstance(price, (int, float, Decimal)):
    #     errors.append({"field": "price", "message": "price must be a number"})

    if errors:
        return False, {"type": "validation_error", "errors": errors}
    
    return True, {}