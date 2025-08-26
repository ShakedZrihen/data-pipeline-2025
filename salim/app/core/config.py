"""Configuration settings for the application."""
import os
from typing import Optional
from pathlib import Path
from dotenv import load_dotenv


class Settings:
    """Application settings configuration."""
    
    def __init__(self):
        self._load_environment()
        
    def _load_environment(self):
        """Load environment variables from .env file."""
        # Try project-root .env first
        loaded = load_dotenv()
        
        # Fallback: explicit relative path to consumer/.env
        if not loaded:
            consumer_env = Path(__file__).resolve().parent.parent.parent / "consumer" / ".env"
            load_dotenv(consumer_env.as_posix())
    
    @property
    def supabase_url(self) -> str:
        """Get Supabase URL from environment."""
        url = os.getenv("SUPABASE_URL")
        if not url:
            raise ValueError("SUPABASE_URL environment variable is required")
        return url
    
    @property
    def supabase_key(self) -> str:
        """Get Supabase key from environment."""
        key = os.getenv("SUPABASE_KEY")
        if not key:
            raise ValueError("SUPABASE_KEY environment variable is required")
        return key
    
    @property
    def app_name(self) -> str:
        """Get application name."""
        return "Salim API"
    
    @property
    def app_version(self) -> str:
        """Get application version."""
        return "1.0.0"
    
    @property
    def debug(self) -> bool:
        """Get debug mode setting."""
        return os.getenv("DEBUG", "false").lower() == "true"


# Global settings instance
settings = Settings()
