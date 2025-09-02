from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, ForeignKey, UniqueConstraint, func
from sqlalchemy.orm import relationship
from db import Base

class Supermarket(Base):
    __tablename__ = "supermarkets"
    supermarket_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False, default="Unknown")
    branch_name = Column(String)
    city = Column(String)
    address = Column(String)
    website = Column(String)
    created_at = Column(DateTime, server_default=func.now())

    products = relationship("Product", back_populates="supermarket")

class Product(Base):
    __tablename__ = "products"
    product_id = Column(Integer, primary_key=True, autoincrement=True)
    supermarket_id = Column(Integer, ForeignKey("supermarkets.supermarket_id"), nullable=False)
    barcode = Column(String, index=True)
    canonical_name = Column(String, nullable=False)
    brand = Column(String)
    category = Column(String)
    size_value = Column(Float)
    size_unit = Column(String)
    price = Column(Float, nullable=False)
    currency = Column(String, default="ILS")
    promo_price = Column(Float)
    promo_text = Column(String)
    in_stock = Column(Boolean, default=True)
    collected_at = Column(DateTime)

    supermarket = relationship("Supermarket", back_populates="products")
    __table_args__ = (UniqueConstraint("supermarket_id","barcode","collected_at", name="uq_price_snapshot"),)
