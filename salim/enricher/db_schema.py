from __future__ import annotations
import os
from datetime import date, datetime
from decimal import Decimal
from sqlalchemy import (
     String, Date, Boolean, DECIMAL, ForeignKey, text, Integer, DateTime
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column



DATABASE_URL = os.getenv("DATABASE_URL")


class Base(DeclarativeBase):
    pass

class Product(Base):
    __tablename__ = "products"

    product_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)  
    barcode: Mapped[str] = mapped_column(String(32))
    canonical_name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    brand: Mapped[str] = mapped_column(String(255), nullable=True)
    category: Mapped[str] = mapped_column(String(255), nullable=True)
    created_at: Mapped[date] = mapped_column(Date, nullable=False, server_default=text("CURRENT_DATE"))

    def to_dict(self) -> dict:
        return {
            "product_id": self.product_id,
            "barcode": self.barcode,
            "canonical_name": self.canonical_name,
            "brand": self.brand,
            "category": self.category,
            "created_at": self.created_at,
        }

    def __repr__(self) -> str:
        return str(self.to_dict())


class Supermarket(Base):
    __tablename__ = "supermarkets"

    supermarket_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    branch_name: Mapped[str] = mapped_column(String(32))
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    city: Mapped[str] = mapped_column(String(120), nullable=False)
    address: Mapped[str] = mapped_column(String(255), nullable=False)
    website: Mapped[str] = mapped_column(String(255), nullable=True)
    created_at: Mapped[date] = mapped_column(Date, nullable=False, server_default=text("CURRENT_DATE"))

    def to_dict(self) -> dict:
        return {
            "supermarket_id": self.supermarket_id,
            "branch_name": self.branch_name,
            "name": self.name,
            "city": self.city,
            "address": self.address,
            "website": self.website,
            "created_at": self.created_at,
        }

    def __repr__(self) -> str:
        return str(self.to_dict())


class ProductPrice(Base):
    __tablename__ = "product_price"

    product_price_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    product_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("products.product_id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    supermarket_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("supermarkets.supermarket_id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )

    size_value: Mapped[Decimal] = mapped_column(DECIMAL(10, 3), nullable=True)
    size_unit: Mapped[str] = mapped_column(String(32), nullable=True)
    price: Mapped[Decimal] = mapped_column(DECIMAL(10, 2), nullable=False, default=0.0)
    currency: Mapped[str] = mapped_column(String(3), nullable=False, default="ILS")
    promo_price: Mapped[Decimal] = mapped_column(DECIMAL(10, 2), nullable=True)
    promo_text: Mapped[str] = mapped_column(String(512), nullable=True)
    in_stock: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    collected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP"))

    def to_dict(self) -> dict:
        return {
            "product_price_id": self.product_price_id,
            "product_id": self.product_id,
            "supermarket_id": self.supermarket_id,
            "barcode": self.barcode,
            "canonical_name": self.canonical_name,
            "brand": self.brand,
            "category": self.category,
            "size_value": float(self.size_value) if self.size_value is not None else None,
            "size_unit": self.size_unit,
            "price": float(self.price) if self.price is not None else None,
            "currency": self.currency,
            "promo_price": float(self.promo_price) if self.promo_price is not None else None,
            "promo_text": self.promo_text,
            "in_stock": bool(self.in_stock),
            "collected_at": self.collected_at.isoformat() if self.collected_at else None,
        }

    def __repr__(self) -> str:
        return str(self.to_dict())

