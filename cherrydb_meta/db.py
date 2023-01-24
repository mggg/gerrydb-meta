"""Database connections (PostgreSQL and Redis)."""
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

Session = sessionmaker(create_engine(os.getenv("CHERRY_DATABASE_URI")))
