from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import settings


engine_kwargs = {
    "echo": False,
}

# SQLite does not use normal server-side connection pooling
if settings.database_url.startswith("sqlite"):
    engine_kwargs["connect_args"] = {
        "check_same_thread": False,
    }
else:
    engine_kwargs["pool_pre_ping"] = True


engine = create_async_engine(
    settings.database_url,
    **engine_kwargs,
)

SessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)