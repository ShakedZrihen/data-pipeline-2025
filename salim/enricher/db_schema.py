from __future__ import annotations
import os
from datetime import date
from decimal import Decimal
from dotenv import load_dotenv
load_dotenv()


from sqlalchemy import (
     String, Date, Boolean, DECIMAL, ForeignKey, text
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column



DATABASE_URL = os.getenv("DATABASE_URL")


class Base(DeclarativeBase):
    pass

class Product(Base):
    __tablename__ = "products"

    barcode: Mapped[str] = mapped_column(String(32), primary_key=True)
    product_name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    product_brand: Mapped[str] = mapped_column(String(255), nullable=True)
    

    def to_dict(self) -> dict:
        return {
            "barcode": self.barcode,
            "product_name": self.product_name,
            "product_brand": self.product_brand,
        }

    def __repr__(self) -> str:
        return str(self.to_dict())


class Supermarket(Base):
    __tablename__ = "supermarkets"

    branch_number: Mapped[str] = mapped_column(String(32), primary_key=True, autoincrement=False)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    city: Mapped[str] = mapped_column(String(120), nullable=False)
    address: Mapped[str] = mapped_column(String(255), nullable=False)

    def to_dict(self) -> dict:
        return {
            "branch_number": self.branch_number,
            "name": self.name,
            "city": self.city,
            "address": self.address,
        }

    def __repr__(self) -> str:
        return str(self.to_dict())


class ProductPrice(Base):
    __tablename__ = "product_price"

    barcode: Mapped[str] = mapped_column(
        String(32),
        ForeignKey("products.barcode",
        ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    )
    branch_number: Mapped[str] = mapped_column(
        String(32),
        ForeignKey("supermarkets.branch_number",
        ondelete="CASCADE"),
        nullable=False
    )

    date: Mapped[date] = mapped_column(Date, nullable=False)
    promo_exists: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    promo_date_start: Mapped[date] = mapped_column(Date, nullable=True)
    promo_date_end: Mapped[date] = mapped_column(Date, nullable=True)
    promo_price: Mapped[Decimal] = mapped_column(DECIMAL(10, 2), nullable=True)
    price: Mapped[Decimal] = mapped_column(DECIMAL(10, 2), nullable=False, default=0.0)  
    promo_max_qty: Mapped[Decimal] = mapped_column(DECIMAL(10, 2), nullable=True)      
    promo_min_qty: Mapped[Decimal] = mapped_column(DECIMAL(10, 2), nullable=True)    
    currency: Mapped[str] = mapped_column(String(3), nullable=False, default="שח")


    def to_dict(self) -> dict:
        return {
            "barcode": self.barcode,
            "branch_number": self.branch_number,
            "promo_exists": self.promo_exists,
            "promo_date_start": self.promo_date_start.isoformat() if self.promo_date_start else None,
            "promo_date_end": self.promo_date_end.isoformat() if self.promo_date_end else None,
            "promo_price": str(self.promo_price) if self.promo_price else None,
            "price": str(self.price),
            "promo_max_qty": self.promo_max_qty,
            "promo_min_qty": self.promo_min_qty,
            "currency": self.currency
        }

    def __repr__(self) -> str:
        return str(self.to_dict())

