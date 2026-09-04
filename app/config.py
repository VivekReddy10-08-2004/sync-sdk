from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_url: str = "sqlite+aiosqlite:///./sync_dev.db"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="SYNC_",
        extra="ignore",
    )


settings = Settings()