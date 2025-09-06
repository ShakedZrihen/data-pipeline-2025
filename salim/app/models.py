"""
SQLAlchemy models for the Israeli Supermarket Price Tracking System.
These models match the exact database schema in Supabase.
"""

from sqlalchemy import Column, Integer, String, Text, Numeric, ForeignKey, TIMESTAMP, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import CITEXT

Base = declarative_base()


class Product(Base):
    """Product model matching the products table."""
    __tablename__ = 'products'
    
    product_id = Column(Integer, primary_key=True)
    barcode = Column(Text, unique=True, nullable=True)
    product_name = Column(CITEXT, nullable=False)  
    brand_name = Column(CITEXT, nullable=False, default='Unknown')
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())
    
   
    prices = relationship("Price", back_populates="product", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<Product(id={self.product_id}, name='{self.product_name}', brand='{self.brand_name}')>"


class Branch(Base):
    """Branch model matching the branches table."""
    __tablename__ = 'branches'
    
    branch_id = Column(Integer, primary_key=True)
    provider = Column(CITEXT, nullable=False, default='Unknown')  
    name = Column(CITEXT, nullable=False)  # Branch name
    address = Column(CITEXT, nullable=False, default='Unknown')
    city = Column(CITEXT, nullable=False, default='Unknown')
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())
    
    
    prices = relationship("Price", back_populates="branch", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<Branch(id={self.branch_id}, provider='{self.provider}', name='{self.name}')>"


class Price(Base):
    """Price model matching the prices table."""
    __tablename__ = 'prices'
    
    price_id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey('products.product_id', ondelete='CASCADE'), nullable=False)
    branch_id = Column(Integer, ForeignKey('branches.branch_id', ondelete='CASCADE'), nullable=False)
    price = Column(Numeric(12, 4), nullable=False)
    discount_price = Column(Numeric(12, 4), nullable=True)
    ts = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())
    final_price = Column(Numeric(12, 4))  
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())
    
   
    product = relationship("Product", back_populates="prices")
    branch = relationship("Branch", back_populates="prices")
    
    def __repr__(self):
        return f"<Price(id={self.price_id}, product_id={self.product_id}, price={self.price}, final={self.final_price})>"
