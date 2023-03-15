"""Database connections."""
import json
import os
import urllib.parse

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

if os.getenv("DB"):
    # AWS App Runner deployment: parse JSON blob with credentials.
    creds = json.loads(os.getenv("DB"))
    db_url = (
        f'postgresql://{creds["username"]}:{urllib.parse.quote(creds["password"])}'
        f'@{creds["host"]}:{creds["port"]}/{creds["dbname"]}'
    )
elif os.getenv("INSTANCE_CONNECTION_NAME"):
    username = os.environ["DB_USER"]
    password = urllib.parse.quote(os.environ["DB_PASS"])
    db_name = os.environ["DB_NAME"]
    socket = f'/cloudsql/{os.environ["INSTANCE_CONNECTION_NAME"]}'
    db_url = f"postgresql://{username}:{password}@/{db_name}?host={socket}"
else:
    # Local development: use Postgres URL direcrly.
    db_url = os.getenv("CHERRY_DATABASE_URI")

Session = sessionmaker(create_engine(db_url))
