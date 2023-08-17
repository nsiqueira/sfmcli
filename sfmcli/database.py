from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker

DATABASE_URL = 'sqlite:///db.db'

engine = create_engine(DATABASE_URL)
session = scoped_session(
    sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=engine,
    ),
)

Base = declarative_base()
Base.query = session.query_property()


def initialize_database():
    Base.metadata.create_all(bind=engine)
