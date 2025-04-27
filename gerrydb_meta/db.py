"""Database connections."""

import json
import os
import urllib.parse

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from uvicorn.config import logger as log

if os.getenv("INSTANCE_CONNECTION_NAME"):
    username = os.environ["DB_USER"]
    password = urllib.parse.quote(os.environ["DB_PASS"])
    db_name = os.environ["DB_NAME"]
    socket = f'/cloudsql/{os.environ["INSTANCE_CONNECTION_NAME"]}'
    db_url = f"postgresql://{username}:{password}@/{db_name}?host={socket}"
    # For Cloud Run deployments, credentials are written to a connection service file
    # on app initialization.
    # see https://www.postgresql.org/docs/current/libpq-pgservice.html
    ogr2ogr_db_config = "PG:service=gerrydb"

else:
    # Local development: use Postgres URL direcrly.
    db_url = os.getenv("GERRYDB_DATABASE_URI")
    if os.getenv("GERRYDB_RUN_TESTS"):
        db_url = os.getenv("GERRYDB_TEST_DATABASE_URI")

    ogr2ogr_db_config = f"PG:{db_url}"

log.debug("Using database URL: %s", db_url)
Session = sessionmaker(create_engine(db_url))
