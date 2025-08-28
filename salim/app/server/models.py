from sqlalchemy import Column, Integer, String, Numeric, Boolean, DateTime, Text, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .database import Base

class Supermarket(Base):
    __tablename__ = "supermarkets"

    supermarket_id = Column(Integer, primary_key=True, index=True)
    name = Column(Text, nullable=False)
    branch_name = Column(Text)
    city = Column(Text)
    address = Column(Text)
    website = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationship
    products = relationship("Product", back_populates="supermarket")

class Product(Base):
    __tablename__ = "products"

    product_id = Column(Integer, primary_key=True, index=True)
    supermarket_id = Column(Integer, ForeignKey("supermarkets.supermarket_id", ondelete="CASCADE"), nullable=False)
    
    barcode = Column(Text, nullable=False, index=True)
    canonical_name = Column(Text, nullable=False)
    brand = Column(Text)
    category = Column(Text)
    size_value = Column(Numeric(12, 3))
    size_unit = Column(Text)
    
    price = Column(Numeric(12, 2), nullable=False)
    currency = Column(String(3), nullable=False, default='ILS')
    list_price = Column(Numeric(12, 2))
    promo_price = Column(Numeric(12, 2))
    promo_text = Column(Text)
    loyalty_only = Column(Boolean, default=False)
    in_stock = Column(Boolean)
    
    collected_at = Column(DateTime(timezone=True), server_default=func.now())
    source = Column(Text)
    raw_hash = Column(Text)

    # Relationship
    supermarket = relationship("Supermarket", back_populates="products")