"""Database connection and client setup."""
from supabase import create_client, Client
from app.core.config import settings


class DatabaseClient:
    """Supabase database client wrapper."""
    
    def __init__(self):
        self._client: Client = None
        
    @property
    def client(self) -> Client:
        """Get or create Supabase client instance."""
        if self._client is None:
            self._client = create_client(settings.supabase_url, settings.supabase_key)
        return self._client


# Global database client instance
db_client = DatabaseClient()
