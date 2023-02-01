"""Database and authorization dependencies for CherryDB endpoints."""
import re
from hashlib import sha512
from http import HTTPStatus
from typing import Generator
from uuid import UUID

from fastapi import Depends, Header, HTTPException
from sqlalchemy.orm import Session

from cherrydb_meta import crud, models
from cherrydb_meta.db import Session
from cherrydb_meta.enums import ScopeType
from cherrydb_meta.scopes import ScopeManager

API_KEY_PATTERN = re.compile(r"[0-9a-z]{64}")


def get_db() -> Generator:
    try:
        db = Session(future=True)
        yield db
        db.commit()
    finally:
        db.close()


def get_user(
    db: Session = Depends(get_db), x_api_key: str | None = Header(default=None)
) -> models.User:
    """Retrieves the user associated with an API key."""
    if x_api_key is None:
        raise HTTPException(
            status_code=HTTPStatus.UNAUTHORIZED, detail="API key required."
        )

    key_raw = x_api_key.lower()
    if re.match(API_KEY_PATTERN, key_raw) is None:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST, detail="Invalid API key format."
        )

    key_hash = sha512(key_raw.encode("utf-8")).digest()
    api_key = crud.api_key.get(db=db, id=key_hash)
    if api_key is None:
        raise HTTPException(
            status_code=HTTPStatus.UNAUTHORIZED, detail="Unknown API key."
        )
    if not api_key.active:
        raise HTTPException(
            status_code=HTTPStatus.UNAUTHORIZED, detail="API key is not active."
        )
    return api_key.user


def get_obj_meta(
    db: Session = Depends(get_db),
    user: models.User = Depends(get_user),
    x_cherry_meta_id: str | None = Header(default=None),
) -> models.ObjectMeta:
    """Retrieves the object metadata referenced in a request header."""
    if x_cherry_meta_id is None:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST, detail="Object metadata ID required."
        )

    try:
        meta_uuid = UUID(x_cherry_meta_id)
    except ValueError:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail="Object metadata ID is not a valid UUID hex string.",
        )

    obj_meta = crud.obj_meta.get(db=db, id=meta_uuid)
    if obj_meta is None:
        raise HTTPException(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            detail="Unknown object metadata ID.",
        )
    if obj_meta.created_by != user.user_id:
        raise HTTPException(
            status_code=HTTPStatus.FORBIDDEN,
            detail="Cannot use metadata object created by another user.",
        )
    return obj_meta


def get_scopes(
    user: models.User = Depends(get_user),
) -> ScopeManager:
    """Returns a user-level scope manager."""
    return ScopeManager(user=user)


def no_perms(msg: str) -> str:
    """Generates a permissions-related error string."""
    return f"You do not have sufficient permissions to {msg}."


def global_scope_check(scope: ScopeType, message: str):
    """Returns a dependency that raises 403 Forbidden if a scope requirement fails."""

    def dependency(scopes: ScopeManager = Depends(get_scopes)):
        if not scopes.has_global_scope(scope):
            raise HTTPException(
                status_code=HTTPStatus.FORBIDDEN, detail=no_perms(message)
            )

    return dependency


can_read_localities = global_scope_check(ScopeType.LOCALITY_READ, "read localities")
can_write_localities = global_scope_check(ScopeType.LOCALITY_WRITE, "write localities")
can_write_meta = global_scope_check(ScopeType.META_WRITE, "write metadata")
can_create_namespace = global_scope_check(
    ScopeType.NAMESPACE_CREATE, "create namespaces"
)
