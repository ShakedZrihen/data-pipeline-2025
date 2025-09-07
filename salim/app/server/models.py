from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, Text, Numeric, TIMESTAMP, text

class Base(DeclarativeBase):
    pass

class Supermarket(Base):
    __tablename__ = "supermarket"
    supermarket_id: Mapped[int] = mapped_column(primary_key=True)
    provider:      Mapped[str]  = mapped_column(Text, nullable=False)
    branch_code:   Mapped[str]  = mapped_column(Text, nullable=False)
    name:          Mapped[str | None] = mapped_column(Text)
    branch_name:   Mapped[str | None] = mapped_column(Text)
    city:          Mapped[str | None] = mapped_column(Text)
    address:       Mapped[str | None] = mapped_column(Text)
    website:       Mapped[str | None] = mapped_column(Text)
    created_at:    Mapped[str | None] = mapped_column(TIMESTAMP(timezone=True), server_default=text("now()"))

class PriceItem(Base):
    __tablename__ = "price_item"
    provider:     Mapped[str] = mapped_column(Text, primary_key=True)
    branch_code:  Mapped[str] = mapped_column(Text, primary_key=True)
    product_code: Mapped[str] = mapped_column(Text, primary_key=True)
    product_name: Mapped[str] = mapped_column(Text, nullable=False)
    unit:         Mapped[str | None] = mapped_column(Text)
    price:        Mapped[float] = mapped_column(Numeric(10,2), nullable=False)
    ts:           Mapped[str] = mapped_column(TIMESTAMP(timezone=True), primary_key=True)

class PromoItem(Base):
    __tablename__ = "promo_item"
    provider:     Mapped[str] = mapped_column(Text, primary_key=True)
    branch_code:  Mapped[str] = mapped_column(Text, primary_key=True)
    product_code: Mapped[str] = mapped_column(Text, primary_key=True)
    description:  Mapped[str | None] = mapped_column(Text, primary_key=True)
    start_ts:     Mapped[str | None] = mapped_column(TIMESTAMP(timezone=True), primary_key=True)
    end_ts:       Mapped[str | None] = mapped_column(TIMESTAMP(timezone=True), primary_key=True)
    price:        Mapped[float | None] = mapped_column(Numeric(10,2))
    rate:         Mapped[float | None] = mapped_column(Numeric(10,4))
    quantity:     Mapped[int | None]   = mapped_column()