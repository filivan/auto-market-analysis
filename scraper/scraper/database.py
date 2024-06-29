from sqlalchemy.orm import DeclarativeBase

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from scraper.config import settings


class Base(DeclarativeBase):
    pass


# async_engine = create_async_engine(
#     url=settings.DATABASE_URL,
#     echo=True,
# )

# async_session_factory = async_sessionmaker(async_engine)
