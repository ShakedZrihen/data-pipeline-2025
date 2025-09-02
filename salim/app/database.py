"""
Database connection configuration for the FastAPI application.
Uses SQLAlchemy with connection pooling for optimal performance.
"""

import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import NullPool
from typing import Generator
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database URL from environment or use Supabase production
DATABASE_URL = os.getenv(
    "SUPABASE_DB_URL",
    "postgresql://postgres.nnzvfgjldslywfofkyet:Warrockaboalmrwan@aws-1-eu-central-1.pooler.supabase.com:6543/postgres"
)

# Create engine with connection pooling
# Using NullPool for Supabase since it has its own pooling
engine = create_engine(
    DATABASE_URL,
    poolclass=NullPool,  # Supabase handles pooling
    echo=False,  # Set to True for SQL query logging
    future=True,
    pool_pre_ping=True,  # Verify connections before using
    connect_args={
        "connect_timeout": 10,
        "options": "-c statement_timeout=30000"  # 30 second statement timeout
    }
)

# Create SessionLocal class
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
    future=True
)


def get_db() -> Generator[Session, None, None]:
    """
    Dependency to get database session.
    Yields a database session and ensures it's closed after use.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def test_connection() -> bool:
    """
    Test the database connection.
    Returns True if connection is successful, False otherwise.
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            logger.info("✅ Database connection successful!")
            return True
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        return False


def get_table_counts() -> dict:
    """
    Get row counts for all tables.
    Useful for quick health check.
    """
    try:
        with SessionLocal() as session:
            products_count = session.execute(text("SELECT COUNT(*) FROM products")).scalar()
            branches_count = session.execute(text("SELECT COUNT(*) FROM branches")).scalar()
            prices_count = session.execute(text("SELECT COUNT(*) FROM prices")).scalar()
            
            return {
                "products": products_count,
                "branches": branches_count,
                "prices": prices_count
            }
    except Exception as e:
        logger.error(f"Error getting table counts: {e}")
        return {}
