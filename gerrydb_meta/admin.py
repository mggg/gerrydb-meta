"""Administration tools for GerryDB."""
import logging
import os
import random
import string
from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import sha512

import click
from sqlalchemy import create_engine
from sqlalchemy.orm import Session as SessionType
from sqlalchemy.orm import sessionmaker

from gerrydb_meta.enums import NamespaceGroup, ScopeType
from gerrydb_meta.models import ApiKey, ObjectMeta, User, UserScope

log = logging.getLogger()
Session = sessionmaker(create_engine(os.getenv("GERRYDB_DATABASE_URI")))

API_KEY_CHARS = string.ascii_lowercase + string.digits


def _generate_api_key() -> tuple[str, bytes]:
    """Generates a random API key (64 characters, a-z0-9).

    Returns:
        A 2-tuple containing:
            (1) The raw key.
            (2) A binary digest of the SHA512 hash of the key.
    """
    key = "".join(random.choice(API_KEY_CHARS) for _ in range(64))
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

    def user_create(self, email: str, name: str) -> User:
        """Returns a new user."""
        user = User(email=email, name=name)
        log.info("Created new user: %s", user)
        self.session.add(user)
        self.session.flush()
        self.session.refresh(user)

        # TODO: don't grant broad privileges by default!
        grant_scope(self.session, user, ScopeType.ALL, namespace_group=None)
        grant_scope(
            self.session, user, ScopeType.ALL, namespace_group=NamespaceGroup.ALL
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
