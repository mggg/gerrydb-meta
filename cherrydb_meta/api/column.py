"""Endpoints for column metadata."""
from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from cherrydb_meta import crud, models, schemas
from cherrydb_meta.api.deps import get_db, get_obj_meta

router = APIRouter()


@router.get("/{namespace}", response_model=list[schemas.Column])
def read_columns_in_namespace(
    *, db: Session = Depends(get_db), namespace: str
) -> list[models.DataColumn]:
    return crud.column.all_in_namespace(db=db, namespace=namespace)


@router.get("/{namespace}/{path:path}", response_model=schemas.Column)
def read_column(
    *,
    namespace: str,
    path: str,
    db: Session = Depends(get_db),
) -> models.DataColumn:
    column = crud.column.get(db=db, path=path)
    if column is None:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail="No column found.")


@router.post(
    "/{namespace:path}", response_model=schemas.Column, status_code=HTTPStatus.CREATED
)
def create_column(
    *,
    namespace: str,
    loc_in: schemas.ColumnCreate,
    db: Session = Depends(get_db),
    obj_meta: models.ObjectMeta = Depends(get_obj_meta),
) -> models.DataColumn:
    return crud.column.create(db=db, obj_in=loc_in, obj_meta=obj_meta)
