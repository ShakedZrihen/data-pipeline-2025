from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from shared.config import settings

engine = create_engine(settings.database_url, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
Base = declarative_base()
