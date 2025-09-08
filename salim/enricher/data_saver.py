from datetime import datetime, timezone
from typing import Dict, Any
from validator import send_error_message, validate_price_with_promo, validate_price_without_promo, validate_product, validate_supermarket
from sqlalchemy.orm import Session
from db_schema import Product, Supermarket, ProductPrice, Base, DATABASE_URL
from sqlalchemy import create_engine
from store_address_enricher import main as enrich_store_addresses


def init_db():
    try:
        engine = create_engine(DATABASE_URL)
        Base.metadata.create_all(engine)
        return engine
    except Exception as e:
        print(f"Failed to initialize database: {e}")
        raise

def save_product(session: Session, product_data: Dict[str, Any]) -> int:
    try:
        product = Product(**product_data)
        merged = session.merge(product)   
        session.flush()
        session.refresh(merged)
        session.commit()
        return int(merged.product_id)
    except Exception as e:
        session.rollback()
        print(f"Failed to save products: {e}")
        raise

def save_price(session: Session, price_data: Dict[str, Any]) -> None:
    try:
        price = ProductPrice(**price_data)
        session.merge(price)
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Failed to save price: {e}")
        raise

def save_supermarket(session: Session, market_data: Dict[str, Any]) -> int:
    try:
        market = Supermarket(**market_data)
        merged = session.merge(market)
        session.flush()
        session.refresh(merged)
        session.commit()
        return int(merged.supermarket_id)
    except Exception as e:
        session.rollback()
        print(f"Failed to save supermarket: {e}")
        raise

def save_enriched_data(chunks_by_source: Dict[str, Dict[str, Any]], dlq_url: str) -> None:
    engine = init_db()
    
    with Session(engine) as session:
        try:
            for source_key, chunk_data in chunks_by_source.items():
                parts = source_key.split('_')
                provider = parts[0]
                branch_number = parts[1]
                file_type = parts[2]
                ts_str = parts[3].replace('.gz', '')
                timestamp = datetime.fromtimestamp(int(ts_str), timezone.utc).isoformat()
                supermarket_id = None
                
                print(f"Processing {file_type} data for {provider} branch {branch_number}")
                existing_market = session.query(Supermarket).filter_by(
                    name=provider, branch_name=str(branch_number)).first()
                if existing_market:
                    supermarket_id = int(existing_market.supermarket_id)
                    print(f"Supermarket {provider} for branch {branch_number} already exists in database. sp_id: {supermarket_id}")
                else:
                    result = enrich_store_addresses(provider, branch_number)
                    city, address = result if result else (None, None)
                    data = {
                        "branch_name": str(branch_number),
                        "name": provider,
                        "city": city,
                        "address": address
                    }
                    result_valid, errors = validate_supermarket(data)
                    if result_valid:
                        supermarket_id = save_supermarket(session, data)
                        print(f"Saved new supermarket {provider} for branch {branch_number} with id {supermarket_id}")
                    else:
                        send_error_message(dlq_url, {"type": "validation_error_supermarket", "errors": {**errors, "provider": provider, "branch_name": branch_number}})
                        print(f"Validation errors found for {provider} branch {branch_number}: {errors}")
                        continue

                if 'price' in file_type:
                    print(f"Saving price data for {provider} branch {branch_number}")
                    for data in chunk_data['data']:
                        for item in data:

                            item_id = None
                            
                            product_data = {
                                "barcode": item.get("barcode") ,
                                "canonical_name": item.get("canonical_name"),
                                "brand": item.get("manufacture_name"),
                                "category": item.get("category"),
                            }

                            valid, errors = validate_product(product_data)
                            if not valid:
                                send_error_message(dlq_url, {"type": "validation_error_product", "errors": {**errors, "provider": provider, "branch_number": branch_number}})
                                print(f"Validation errors found for product {product_data['barcode']}: {errors}")
                                continue

                            item_id = save_product(session, product_data)

                            price_data = {
                                'product_id': item_id,
                                'supermarket_id': supermarket_id,
                                'size_value': float(item.get("quantity")) if item.get("quantity") else None,
                                'size_unit': item.get("unit_qty", ''),
                                'price': float(item.get("price")) if item.get("price") else None,          
                            }

                            valid_price, errors_price = validate_price_without_promo(price_data)
                            if not valid_price:
                                send_error_message(dlq_url, {"type": "validation_error_price", "errors": {**errors_price, "provider": provider, "branch_number": branch_number}})
                                print(f"Validation errors found for price data {price_data['product_id']}: {errors_price}")
                                continue

                            save_price(session, price_data)

                else:
                    for data in chunk_data['data']:
                        for item in data:
                            for barcode in item.get("items", []):
                                print(f"Saving promo data for {provider} branch {branch_number}, barcode {barcode}")
                                product_id = save_product(session, {
                                    "barcode": barcode.get("barcode"),
                                })
                                
                                promo_data = {
                                    'product_id': product_id,
                                    'supermarket_id': supermarket_id,
                                    'promo_price': item.get("promo_price"),
                                    'promo_text': item.get("promo_text"),
                                    'collected_at': timestamp
                                }
                            
                                valid_price, errors_price = validate_price_with_promo(promo_data)
                                if valid_price:
                                    save_price(session, promo_data)
                                else:
                                    send_error_message(dlq_url, {"type": "validation_error_price", "errors": {**errors_price, "provider": provider, "branch_number": branch_number}})
                                    print(f"Validation errors found for price data {promo_data['barcode']}: {errors_price}")
                                    continue


            print("Completed saving all data")

        except Exception as e:
            print(f"Failed to save data: {e}")
            raise