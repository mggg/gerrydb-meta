"""Database connections."""
import json
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

if os.getenv("DB"):
    # AWS App Runner deployment: parse JSON blob with credentials.
    creds = json.loads(os.getenv("DB"))
    db_url = (
        f'postgresql://{creds["username"]}:{creds["password"]}'
        f'@{creds["host"]}:{creds["port"]}/{creds["dbname"]}'
    )
else:
    # Local development: use Postgres URL direcrly.
    db_url = os.getenv("CHERRY_DATABASE_URI")

Session = sessionmaker(create_engine(db_url))
