"""Refresh DB."""
import os

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from cherrydb_meta.admin import CherryAdmin
from cherrydb_meta.models import Base

NAME = "Parker Rule"
EMAIL = "pjrule@me.com"

if __name__ == "__main__":
    engine = create_engine(os.getenv("CHERRY_DATABASE_URI"))
    with engine.connect() as conn:
        conn.execute(text("DROP SCHEMA cherrydb CASCADE"))
        conn.execute(text("CREATE SCHEMA cherrydb"))
        conn.commit()
    Base.metadata.create_all(engine)

    db = sessionmaker(engine)()
    admin = CherryAdmin(session=db)
    user = admin.user_create(name=NAME, email=EMAIL)
    api_key = admin.key_create(user=user)
    db.commit()
    db.close()

    print(api_key)
    with open(".cherryrc", "w") as fp:
        print(f'export CHERRY_USER="{NAME}"', file=fp)
        print(f'export CHERRY_API_KEY="{api_key}"', file=fp)
