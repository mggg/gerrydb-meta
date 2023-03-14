"""Database connections."""
import json
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

print("params:", os.getenv("INSTANCE_UNIX_SOCKET"), os.getenv("DB_USER"))

if os.getenv("DB"):
    # AWS App Runner deployment: parse JSON blob with credentials.
    creds = json.loads(os.getenv("DB"))
    db_url = (
        f'postgresql://{creds["username"]}:{creds["password"]}'
        f'@{creds["host"]}:{creds["port"]}/{creds["dbname"]}'
    )
elif os.getenv("INSTANCE_UNIX_SOCKET"):
    username = os.environ["DB_USER"]
    password = os.environ["DB_PASS"]
    db_name = os.environ["DB_NAME"]
    socket = os.environ["INSTANCE_UNIX_SOCKET"]
    db_url = (
        f"postgresql://{username}:{password}@/{db_name}"
        f"?unix_sock={socket}/.s.PGSQL.5432"
    )
else:
    # Local development: use Postgres URL direcrly.
    db_url = os.getenv("CHERRY_DATABASE_URI")

Session = sessionmaker(create_engine(db_url))
