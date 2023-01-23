"""Database connections (PostgreSQL and Redis)."""
import os
import redis
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

Session = sessionmaker(create_engine(os.getenv("CHERRY_DATABASE_URI")))

def redis_init() -> redis.Redis:
    """Configures Redis from the `CHERRY_REDIS_URI` environment variable."""
    return redis.from_url()
   