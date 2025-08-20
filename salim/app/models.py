from sqlalchemy import Column, Integer, String, Float, ForeignKey, DateTime, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .database import Base  # ודא שהייבוא הזה נכון למבנה הפרויקט שלך

class Product(Base):
    __tablename__ = "products"

    id = Column(Integer, primary_key=True, index=True)
    product_id = Column(String, unique=True, index=True, nullable=False)
    product_name = Column(String, nullable=False)
    
    # הקשר לטבלת המחירים
    prices = relationship("Price", back_populates="product")

class Price(Base):
    __tablename__ = "prices"

    id = Column(Integer, primary_key=True, index=True)
    product_key = Column(Integer, ForeignKey("products.id"), nullable=False)
    price = Column(Float, nullable=False)
    provider = Column(String, index=True)
    branch = Column(String, index=True)
    
    # חותמת זמן שתתעדכן אוטומטית
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    # הקשר חזרה לטבלת המוצרים
    product = relationship("Product", back_populates="prices")
    
    # מונע כפילות של אותו מוצר באותו סניף וספק
    __table_args__ = (UniqueConstraint('product_key', 'provider', 'branch', name='_product_provider_branch_uc'),)