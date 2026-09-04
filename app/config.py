from pydantic_settings import BaseSettings, SettingsConfigDict

""" DB engine configuration and settings. """

class Settings(BaseSettings):
    database_url: str = (
        "postgresql+asyncpg://postgres:postgres@localhost:5432/offline_sync"
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="SYNC_",
    )


settings = Settings()