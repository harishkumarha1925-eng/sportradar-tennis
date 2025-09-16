# db_handler.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config import DB_URL
from models import Base

# If using SQLite, allow connections across threads (Streamlit uses threads)
connect_args = {}
if DB_URL.startswith("sqlite"):
    connect_args = {"check_same_thread": False}

# create engine
engine = create_engine(DB_URL, echo=False, future=True, connect_args=connect_args)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

def init_db():
    """Create tables if they don't exist."""
    Base.metadata.create_all(bind=engine)
