from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    # Database settings
    database_url: str = "sqlite:///./todo_app.db"  # Default to SQLite for development
    neon_database_url: str = ""  # Neon PostgreSQL database URL
    db_echo: bool = False  # Set to True to see SQL queries in logs

    # JWT settings
    secret_key: str = "your-default-secret-key-change-in-production"
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 15

    # Better Auth settings
    better_auth_secret: str = ""
    better_auth_url: str = ""

    # Application settings
    app_name: str = "Todo Application API"
    debug: bool = True
    environment: str = "development"  # development, staging, production

    # Logging settings
    log_level: str = "INFO"
    log_file: Optional[str] = "logs/app.log"

    # CORS settings
    allowed_origins: str = "*"  # Comma-separated list of allowed origins

    # URL settings
    backend_url: str = "https://ambreen-dev-todo-app.hf.space"
    frontend_url: str = "http://localhost:3000"

    class Config:
        env_file = ".env"


settings = Settings()