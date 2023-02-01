"""Endpoints for generic object metadata."""
from http import HTTPStatus
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from cherrydb_meta import crud, models, schemas
from cherrydb_meta.scopes import ScopeManager
from cherrydb_meta.api.deps import can_write_meta, get_db, get_user, get_scopes

router = APIRouter()


@router.get("/{uuid}", response_model=schemas.ObjectMeta)
def get_obj_meta(
    *,
    uuid: str,
    db: Session = Depends(get_db),
    user: Session = Depends(get_user),
    scopes: ScopeManager = Depends(get_scopes),
) -> models.ObjectMeta:
    try:
        parsed_uuid = UUID(uuid)
    except ValueError:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail="Object metadata ID is not a valid UUID hex string.",
        )

    obj_meta = crud.obj_meta.get(db=db, id=parsed_uuid)
    if obj_meta is None:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail="Object metadata not found."
        )

    if not scopes.can_read_meta() and obj_meta.created_by != user.user_id:
        raise HTTPException(
            status_code=HTTPStatus.FORBIDDEN,
            detail="You do not have sufficient permissions to read metadata.",
        )
    return obj_meta


@router.post(
    "/",
    status_code=HTTPStatus.CREATED,
    dependencies=[Depends(can_write_meta)],
    response_model=schemas.ObjectMeta
)
def create_obj_meta(
    *,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_user),
    meta_in: schemas.ObjectMetaCreate,
) -> models.ObjectMeta:
    return crud.obj_meta.create(db=db, obj_in=meta_in, user=user)
