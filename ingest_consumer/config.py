from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    aws_region: str = "us-east-1"
    aws_endpoint_url: str | None = None
    sqs_queue_url: str
    sqs_dlq_url: str | None = None
    sqs_max_batch: int = 10
    pg_dsn: str
    log_level: str = "INFO"
    metrics_port: int = 8000

    model_config = SettingsConfigDict(env_file=".env", env_prefix="", case_sensitive=False)

settings = Settings()
