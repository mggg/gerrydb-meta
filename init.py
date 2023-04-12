"""Initializes a GerryDB instance with a superuser."""
import os

import click
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from gerrydb_meta.admin import GerryAdmin
from gerrydb_meta.models import Base


@click.command()
@click.option("--name", help="Superuser name.", required=True)
@click.option("--email", help="Superuser email.", required=True)
@click.option(
    "--reset", is_flag=True, help="Clear old data and re-initialize schema (dangerous)."
)
@click.option("--init-schema", is_flag=True, help="Initialize schema from models.")
def main(name: str, email: str, reset: bool, init_schema: bool):
    """Initializes a GerryDB instance with a superuser.

    Expects the `GERRYDB_DATABASE_URI` environment variable to be set to
    a PostgreSQL connection string.
    """
    engine = create_engine(os.getenv("GERRYDB_DATABASE_URI"))
    db = sessionmaker(engine)()

    if reset:
        with engine.connect() as conn:
            conn.execute(text("DROP SCHEMA IF EXISTS gerrydb CASCADE"))
            conn.commit()

    if reset or init_schema:
        with engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA gerrydb"))
            conn.commit()
        Base.metadata.create_all(engine)

    admin = GerryAdmin(session=db)
    user = admin.user_create(name=name, email=email)
    api_key = admin.key_create(user=user)
    db.commit()
    db.close()

    print(api_key)
    with open(".gerryrc", "w") as fp:
        print(f'export GERRYDB_TEST_API_KEY="{api_key}"', file=fp)
    os.environ["GERRYDB_TEST_API_KEY"] = api_key


if __name__ == "__main__":
    main()
