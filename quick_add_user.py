import os

import click
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from gerrydb_meta.admin import GerryAdmin, grant_scope
from gerrydb_meta.models import ApiKey
from gerrydb_meta.enums import ScopeType

from hashlib import sha512
from pathlib import Path
from typing import Optional


scope_type_dict = {
    "locality:read": ScopeType.LOCALITY_READ,
    "locality:write": ScopeType.LOCALITY_WRITE,
    "meta:read": ScopeType.META_READ,
    "meta:write": ScopeType.META_WRITE,
    "namespace:create": ScopeType.NAMESPACE_CREATE,
    "namespace:read": ScopeType.NAMESPACE_READ,
    "namespace:write": ScopeType.NAMESPACE_WRITE,
    "namespace:write:derived": ScopeType.NAMESPACE_WRITE_DERIVED,
    "all": ScopeType.ALL,
}


# Note: This script is meant to live on the server
# and can only be run by an admin that is logged into the server.
@click.command()
@click.option("--name", help="User name.", required=True)
@click.option("--email", help="User email.", required=True)
@click.option("--key", help="User API key.")
@click.option(
    "-s",
    "--scope",
    help="List of scopes to grant to the user.",
    type=click.Choice(
        [
            "locality:read",
            "locality:write",
            "meta:read",
            "meta:write",
            "namespace:create",
            "namespace:read",
            "namespace:write",
            "namespace:write:derived",
            "all",
        ]
    ),
    multiple=True,
)
def main(
    name: str,
    email: str,
    key: Optional[str] = None,
    scope: Optional[list[str]] = None,
    namespace_group: Optional[str] = None,
):
    engine = create_engine(os.getenv("GERRYDB_DATABASE_URI"))
    db = sessionmaker(engine)()
    print(engine)
    print(db)

    admin = GerryAdmin(session=db)
    user = admin.user_create(name=name, email=email)
    if key:
        key_hash = sha512(key.encode("utf-8")).digest()
        print(f"Key hash: {key_hash.hex()}")
        db.add(ApiKey(user=user, key_hash=key_hash))

    api_key = key if key else admin.key_create(user=user)

    print(f"Created user {user} with API key {api_key}")

    for s in scope:
        grant_scope(
            db=db, user=user, scope=scope_type_dict[s], namespace_group=namespace_group
        )
        print(f"Granted {scope_type_dict[s]} scope to {user}")

    db.commit()
    db.close()


if __name__ == "__main__":
    main()
