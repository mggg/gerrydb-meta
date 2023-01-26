"""Endpoints for generic object metadata."""
from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from cherrydb_meta import crud, schemas
from cherrydb_meta.api.deps import get_db, get_user

router = APIRouter()


@router.get("/{id}")
def get_obj_meta(
    *,
    id: int,
    db: Session = Depends(get_db),
    _user: Session = Depends(get_user),
) -> schemas.ObjectMeta:
    obj_meta = crud.obj_meta.get(db=db, id=id)
    if obj_meta is None:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail="Object metadata not found."
        )

    return schemas.ObjectMeta(
        meta_id=obj_meta.meta_id,
        notes=obj_meta.notes,
        created_at=obj_meta.created_at,
        created_by=obj_meta.user.email,
    )


@router.post("/", status_code=HTTPStatus.CREATED)
def create_obj_meta(
    *,
    db: Session = Depends(get_db),
    user: Session = Depends(get_user),
    loc_in: schemas.ObjectMetaCreate,
) -> schemas.ObjectMeta:
    obj_meta = crud.obj_meta.create(db=db, obj_in=loc_in, user=user)
    return schemas.ObjectMeta(
        meta_id=obj_meta.meta_id,
        notes=obj_meta.notes,
        created_at=obj_meta.created_at,
        created_by=obj_meta.user.email,
    )
