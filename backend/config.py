import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

@dataclass
class Config:
    # AWS Configuration
    AWS_URL: str = os.getenv("AWS_URL", "http://localhost:9000")
    AWS_ACCESS_KEY_ID: str = os.getenv("AWS_ACCESS_KEY_ID", "")
    AWS_SECRET_ACCESS_KEY: str = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    AWS_REGION: str = os.getenv("AWS_REGION", "us-east-1")
    S3_BUCKET: str = os.getenv("S3_BUCKET", "")
    
    # Hive Configuration
    HIVE_HOST: str = os.getenv("HIVE_HOST", "localhost")
    HIVE_PORT: int = int(os.getenv("HIVE_PORT", "10000"))
    HIVE_USERNAME: str = os.getenv("HIVE_USERNAME", "hadoop")
    HIVE_DATABASE: str = os.getenv("HIVE_DATABASE", "default")
    
    # Processing Configuration
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "10000"))
    MAX_SAMPLE_SIZE: int = int(os.getenv("MAX_SAMPLE_SIZE", "1000"))
    
    # Server Configuration
    HOST: str = os.getenv("HOST", "0.0.0.0")
    PORT: int = int(os.getenv("PORT", "8000"))
    DEBUG: bool = os.getenv("DEBUG", "false").lower() == "true"
    WORKERS: int = int(os.getenv("WORKERS", "4"))
    
    def validate(self) -> None:
        """Validate required configuration"""
        required_fields = [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY", 
            "S3_BUCKET"
        ]
        
        missing_fields = []
        for field in required_fields:
            if not getattr(self, field):
                missing_fields.append(field)
        
        if missing_fields:
            raise ValueError(f"Missing required configuration: {', '.join(missing_fields)}")

# Global config instance
config = Config()