"""Initializes a CherryDB instance."""
import os

import click
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from cherrydb_meta.admin import CherryAdmin
from cherrydb_meta.models import Base


@click.command()
@click.option("--name", help="Superuser name.", required=True)
@click.option("--email", help="Superuser email.", required=True)
@click.option("--reset", is_flag=True, help="Clear old data (dangerous).")
def main(name: str, email: str, reset: bool):
    """Initializes a CherryDB instance.

    Expects the `CHERRY_DATABASE_URI` environment variable to be set to
    a PostgreSQL connection string.
    """
    engine = create_engine(os.getenv("CHERRY_DATABASE_URI"))

    if reset:
        with engine.connect() as conn:
            conn.execute(text("DROP SCHEMA IF EXISTS cherrydb CASCADE"))
            conn.commit()

    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA cherrydb"))
        conn.commit()
    Base.metadata.create_all(engine)

    db = sessionmaker(engine)()
    admin = CherryAdmin(session=db)
    user = admin.user_create(name=name, email=email)
    api_key = admin.key_create(user=user)
    db.commit()
    db.close()

    print(api_key)
    with open(".cherryrc", "w") as fp:
        print(f'export CHERRY_TEST_API_KEY="{api_key}"', file=fp)
    os.environ["CHERRY_TEST_API_KEY"] = api_key


if __name__ == "__main__":
    main()
