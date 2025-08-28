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

def save_product(session: Session, product_data: Dict[str, Any]) -> None:
    try:
        product = Product(**product_data)
        session.merge(product) 
        session.commit()
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

def save_supermarket(session: Session, market_data: Dict[str, Any]) -> None:
    try:
        market = Supermarket(
            branch_number=market_data['branch_number'],
            name=market_data.get('name', ''),
            city=market_data.get('city', ''),
            address=market_data.get('address', '')
        )
        session.merge(market)
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Failed to save supermarket: {e}")
        raise

def save_enriched_data(chunks_by_source: Dict[str, Dict[str, Any]], dlq_url: str) -> None:
    engine = init_db()
    
    with Session(engine) as session:
        try:
            for source_key, chunk_data in chunks_by_source.items():
                provider = source_key.split('/')[0]  
                branch_number = source_key.split('/')[1] 
                file_type = source_key.split('/')[2].split('_')[0]
                existing_market = session.query(Supermarket).filter_by(
                    name=provider, branch_number=branch_number).first()
                if existing_market:
                    print(f"Supermarket {provider} for branch {branch_number} already exists in database")
                else:
                    result = enrich_store_addresses(provider, branch_number)
                    city, address = result if result else (None, None)
                    data = {
                        "branch_number": branch_number,
                        "name": provider,
                        "city": city,
                        "address": address
                    }
                    result_valid, errors = validate_supermarket(data)
                    if result_valid:
                        save_supermarket(session, data)
                    else:
                        send_error_message(dlq_url, {"type": "validation_error_supermarket", "errors": {**errors, "provider": provider, "branch_number": branch_number}})
                        print(f"Validation errors found for {provider} branch {branch_number}: {errors}")
                
                if 'price' in file_type:
                    print(f"Saving price data for {provider} branch {branch_number}")
                    for data in chunk_data['data']:
                        for item in data:
                        
                            product_data = {
                                "barcode": item.get("barcode"),
                                "product_name": item.get("product_name"),
                                "product_brand": item.get("manufacture_name"),
                            }

                            valid, errors = validate_product(product_data)
                            if not valid:
                                send_error_message(dlq_url, {"type": "validation_error_product", "errors": {**errors, "provider": provider, "branch_number": branch_number}})
                                print(f"Validation errors found for product {product_data['barcode']}: {errors}")
                                continue

                            save_product(session, product_data)
                            price_data = {
                                "barcode": item.get("barcode"),
                                "branch_number": item.get("branch_id"),
                                "date": item.get("timestamp"),
                                "promo_exists": item.get("promo_exists", False),
                                "price": item.get("price"),
                            }
                            valid_price, errors_price = validate_price_without_promo(price_data)
                            if not valid_price:
                                send_error_message(dlq_url, {"type": "validation_error_price", "errors": {**errors_price, "provider": provider, "branch_number": branch_number}})
                                print(f"Validation errors found for price data {price_data['barcode']}: {errors_price}")
                                continue

                            save_price(session, price_data)

                else:
                    for data in chunk_data['data']:
                        for item in data:
                            save_product(session, {
                                "barcode": item.get("barcode")
                            })
                            
                            promo_data = {
                                "barcode": item.get("barcode"),
                                "branch_number": item.get("branch_id"),
                                "date": item.get("timestamp"),
                                "promo_exists": item.get("promo_exists", False),
                                "promo_price": item.get("promo_price", 0.0),
                                "promo_date_start": item.get("promo_date_start"),
                                "promo_date_end": item.get("promo_date_end"),
                                "promo_max_qty": item.get("promo_max_qty"),
                                "promo_min_qty": item.get("promo_min_qty"),
                            }
                            valid_price, errors_price = validate_price_with_promo(promo_data)
                            if not valid_price:
                                send_error_message(dlq_url, {"type": "validation_error_price", "errors": {**errors_price, "provider": provider, "branch_number": branch_number}})
                                print(f"Validation errors found for price data {promo_data['barcode']}: {errors_price}")
                                continue

                            save_price(session, promo_data)

            print("Completed saving all data")

        except Exception as e:
            print(f"Failed to save data: {e}")
            raise