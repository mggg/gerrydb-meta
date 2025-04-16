"""Administration tools for GerryDB."""

import csv
import os
import secrets
import string
from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import sha512
from pathlib import Path

import click
from sqlalchemy import create_engine
from sqlalchemy.orm import Session as SessionType
from sqlalchemy.orm import sessionmaker

from gerrydb_meta.enums import NamespaceGroup, ScopeType
from gerrydb_meta.models import ApiKey, ObjectMeta, User, UserScope
from uvicorn.config import logger as log
import os

GERRYDB_SQL_ECHO = bool(os.environ.get("GERRYDB_SQL_ECHO", False))

Session = sessionmaker(
    create_engine(os.getenv("GERRYDB_DATABASE_URI"), echo=GERRYDB_SQL_ECHO)
)

API_KEY_CHARS = string.ascii_lowercase + string.digits


def _generate_api_key() -> tuple[str, bytes]:
    """Generates a random API key (64 characters, a-z0-9).

    Returns:
        A 2-tuple containing:
            (1) The raw key.
            (2) A binary digest of the SHA512 hash of the key.
    """
    key = "".join(secrets.choice(API_KEY_CHARS) for _ in range(64))
    test_key = "7w7uv9mi575n2dhlmg3wqba2imv1aqdys387tpbtpermujy1tuyqbxetygx8u3fr"
    redraws = 0
    while key == test_key and redraws < 3:
        key = "".join(secrets.choice(API_KEY_CHARS) for _ in range(64))
        redraws += 1

    if redraws >= 3:
        raise RuntimeError("Failed to generate a unique API key.")

    key_hash = sha512(key.encode("utf-8")).digest()
    return key, key_hash


def grant_scope(
    db: SessionType,
    user: User,
    scope: ScopeType,
    *,
    namespace_group: NamespaceGroup | None = None,
) -> None:
    """Grants a scope to a user."""
    meta = ObjectMeta(
        created_by=user.user_id, notes="Used for authorization configuration only."
    )
    db.add(meta)
    db.flush()

    scope = UserScope(
        user_id=user.user_id,
        scope=scope,
        namespace_group=namespace_group,
        namespace_id=None,
        meta_id=meta.meta_id,
    )
    db.add(scope)
    db.flush()
    db.refresh(user)


@dataclass(frozen=True)
class GerryAdmin:
    """Common GerryDB administration operations."""

    session: SessionType

    def user_create(self, email: str, name: str, super_user: bool = False) -> User:
        """
        Returns a new user. If `super_user`, grants user global privileges.

        By default, a new user is only allowed to read from public namespaces.
        In order to write or do anything else, an admin must grant the user
        permissions on the back end.


        Args:
            email: The user's email address.
            name: The user's name.
            super_user: Whether to grant the user global privileges.

        Returns:
            The new user.
        """

        user = User(email=email, name=name)
        log.info("Created new user: %s", user)
        self.session.add(user)
        self.session.flush()
        self.session.refresh(user)

        if super_user:
            # global privileges
            grant_scope(self.session, user, ScopeType.ALL, namespace_group=None)
            grant_scope(
                self.session, user, ScopeType.ALL, namespace_group=NamespaceGroup.ALL
            )

        else:
            grant_scope(
                self.session,
                user,
                ScopeType.NAMESPACE_READ,
                namespace_group=NamespaceGroup.PUBLIC,
            )
            grant_scope(
                self.session,
                user,
                ScopeType.NAMESPACE_WRITE_DERIVED,
                namespace_group=NamespaceGroup.PUBLIC,
            )

        return user

    def user_find_by_email(self, email: str) -> User:
        """Returns an existing user by email or raises a `ValueError`."""
        user = self.session.query(User).filter(User.email == email).first()
        if user is None:
            raise ValueError(f"No user found with email {email}.")
        log.info("Found %s.", user)
        return user

    def user_deactivate(self, user: User) -> None:
        """Deactivates all API keys for a user."""
        for api_key in user.api_keys:
            log.info("Deactivating API key for %s", user)
            api_key.active = False
        log.info("Deactivated %d API keys for %s", len(user.api_keys), user)

    def key_create(self, user: User) -> ApiKey:
        """Creates a new API key for an existing user.

        Returns:
            The raw API key.
        """
        raw_key, key_hash = _generate_api_key()
        log.info("Generated new API key for %s.", user)
        self.session.add(ApiKey(user=user, key_hash=key_hash))
        return raw_key

    def create_test_key(self, user: User) -> str:
        """
        Creates a known API key for testing purposes.


        """
        user_confirmation = input(
            f"You are about to create a TESTING API key for user {user}. "
            f"This API key is NOT secure and should be considered public knowledge. "
            "Are you sure you want to continue? Y/N: "
        )

        if user_confirmation.lower() != "y":
            raise ValueError("User aborted API key creation.")

        raw_key = "7w7uv9mi575n2dhlmg3wqba2imv1aqdys387tpbtpermujy1tuyqbxetygx8u3fr"
        key_hash = sha512(raw_key.encode("utf-8")).digest()
        log.info("Generated new ***TESTING*** API key for %s.", user)
        self.session.add(ApiKey(user=user, key_hash=key_hash))
        return raw_key

    def key_by_raw(self, raw_key: User) -> ApiKey:
        """Returns an existing API by raw value or raises a `ValueError`."""
        key_hash = sha512(raw_key.encode("utf-8")).digest()
        db_key = self.session.query(ApiKey).filter(ApiKey.key_hash == key_hash).first()
        if db_key is None:
            raise ValueError("No API key found with hash of specified raw key.")
        return db_key

    def key_deactivate(self, api_key: ApiKey) -> None:
        """Deactivates an API key for a user."""
        log.info("Deactivating API key...")
        api_key.active = False


@click.group()
def cli():
    """Administration tools for GerryDB."""


@contextmanager
def admin_context():
    session = Session()
    admin = GerryAdmin(session)
    try:
        yield admin
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


@cli.command("user:create")
@click.option("--email", required=True)
@click.option("--name", required=True)
def user_create(email: str, name: str):
    """Creates a user with an active API key."""
    with admin_context() as admin:
        user = admin.user_create(email=email, name=name)
        raw_key = admin.key_create(user)
        print(f"New API key for new {user}: {raw_key}")


@cli.command("user:create:bulk")
@click.option("--roster", "roster_path", type=click.Path(path_type=Path), required=True)
@click.option("--keys", "keys_path", type=click.Path(path_type=Path), required=True)
def user_create(roster_path: Path, keys_path: Path):
    """Creates users with active API keys in bulk.

    Expects a roster CSV with `name` and `email` columns.
    Generates a CSV with `name`, `email`, and `key` columns.
    """
    accounts = []
    with admin_context() as admin, open(roster_path, newline="") as roster_fp:
        for row in csv.DictReader(roster_fp):
            name = row["name"].strip()
            email = row["email"].lower().strip()
            user = admin.user_create(name=name, email=email)
            raw_key = admin.key_create(user)
            accounts.append(
                {"name": user.name, "email": user.email, "key": str(raw_key)}
            )

    print(f"Generated {len(accounts)} new accounts.")
    with open(keys_path, "w", newline="") as keys_fp:
        writer = csv.DictWriter(keys_fp, fieldnames=["name", "email", "key"])
        writer.writeheader()
        for account in accounts:
            writer.writerow(account)


@cli.command("key:create")
@click.option("--email", required=True)
def key_create(email: str):
    """Creates a new API key for an existing user."""
    with admin_context() as admin:
        user = admin.user_find_by_email(email)
        raw_key = admin.key_create(user)
        print(f"New API key for existing {user}: {raw_key}")


@cli.command("user:deactivate")
@click.option("--email", required=True)
def user_deactivate(email: str):
    """Deactivates all API keys associated with a user."""
    with admin_context() as admin:
        user = admin.user_find_by_email(email)
        admin.user_deactivate(user)


@cli.command("key:deactivate")
@click.option("--key", required=True)
def key_deactivate(key: str):
    """Deactivates a single API key."""
    with admin_context() as admin:
        db_key = admin.key_by_raw(key)
        admin.key_deactivate(db_key)


if __name__ == "__main__":
    cli()
