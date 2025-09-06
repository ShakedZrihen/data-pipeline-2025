"""
Database connection configuration for the FastAPI application.

Supports both `DATABASE_URL` (e.g., Docker Compose local Postgres) and
`SUPABASE_DB_URL` (Supabase pooled connection). If both are set,
`DATABASE_URL` takes precedence.
"""

import os
import logging
from typing import Generator

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import NullPool


load_dotenv()


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


DATABASE_URL = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
if not DATABASE_URL:
    raise ValueError(
        "Database URL not configured. Set either DATABASE_URL (for local Postgres) "
        "or SUPABASE_DB_URL (for Supabase) in the environment/.env."
    )


engine = create_engine(
    DATABASE_URL,
    poolclass=NullPool,
    echo=False,
    future=True,
    pool_pre_ping=True,
    connect_args={
        "connect_timeout": 10,
        "options": "-c statement_timeout=30000",
    },
)


SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
    future=True,
)


def get_db() -> Generator[Session, None, None]:
    """Dependency to provide a database session per-request."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def test_connection() -> bool:
    """Test the database connection."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            logger.info("Database connection successful!")
            return True
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False


def get_table_counts() -> dict:
    """Return row counts for main tables for health/observability."""
    try:
        with SessionLocal() as session:
            products_count = session.execute(text("SELECT COUNT(*) FROM products")).scalar()
            branches_count = session.execute(text("SELECT COUNT(*) FROM branches")).scalar()
            prices_count = session.execute(text("SELECT COUNT(*) FROM prices")).scalar()

            return {
                "products": products_count,
                "branches": branches_count,
                "prices": prices_count,
            }
    except Exception as e:
        logger.error(f"Error getting table counts: {e}")
        return {}

