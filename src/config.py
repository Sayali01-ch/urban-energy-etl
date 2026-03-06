"""Configuration management for the ETL pipeline."""

import os
from dotenv import load_dotenv
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # Data configuration
    data_source_type: str = os.getenv("DATA_SOURCE_TYPE", "csv")
    data_source_path: str = os.getenv("DATA_SOURCE_PATH", "./data/raw")
    
    # Processing configuration
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    output_format: str = os.getenv("OUTPUT_FORMAT", "csv")
    
    # Storage configuration
    output_path: str = os.getenv("OUTPUT_PATH", "./data/output")
    
    # API configuration
    api_timeout: int = int(os.getenv("API_TIMEOUT", "30"))
    max_retries: int = int(os.getenv("MAX_RETRIES", "3"))
    
    class Config:
        env_file = ".env"


# Load environment variables
load_dotenv()

# Create global settings instance
settings = Settings()
