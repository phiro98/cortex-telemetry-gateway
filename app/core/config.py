import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    PROJECT_NAME: str = "Cortex Telemetry Gateway"
    API_V1_STR: str = "/v1"
    
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./test.db")
    
    STREAM_NAME: str = "stream:telemetry:pulses"
    CONSUMER_GROUP: str = "cortex_telemetry_group"
    CONSUMER_NAME: str = "cortex_worker_1"

    class Config:
        env_file = ".env"

settings = Settings()
