import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase


load_dotenv()
POSTGRESQL_URL = os.getenv("POSTGRESQL_URL")

class Base(DeclarativeBase):
    pass

engine = create_engine(
    POSTGRESQL_URL,
    echo=False,
    future=True
)

SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
    future=True
)


def get_pg():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
