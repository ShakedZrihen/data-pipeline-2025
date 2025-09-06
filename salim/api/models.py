from sqlalchemy import Column, Integer, Float, Text, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import uuid
from .database import Base


class Supermarket(Base):
    __tablename__ = "supers"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    address = Column(Text)
    provider = Column(Text)
    branch_number = Column(Integer)
    branch_name = Column(Text)

    # Relationship
    products = relationship("Product", back_populates="supermarket")


class Product(Base):
    __tablename__ = "products"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    branch_number = Column(Integer)
    name = Column(Text)
    barcode = Column(Text, index=True)
    updated_at = Column(DateTime(timezone=True), server_default=func.now())
    super_id = Column(UUID(as_uuid=True), ForeignKey("supers.id", ondelete="CASCADE"))
    price = Column(Float)

    # Relationships
    supermarket = relationship("Supermarket", back_populates="products")

