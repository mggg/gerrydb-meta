"""API operations for manipulating column values."""
from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from cherrydb_meta import crud, models, schemas
from cherrydb_meta.api.base import geos_from_paths, namespace_write_error_msg
from cherrydb_meta.api.deps import get_db, get_obj_meta, get_scopes
from cherrydb_meta.crud.base import normalize_path
from cherrydb_meta.scopes import ScopeManager

router = APIRouter()


@router.put(
    "/{namespace}/{path:path}",
    response_model=None,
    status_code=HTTPStatus.NO_CONTENT,
)
def set_column_values(
    *,
    namespace: str,
    path: str,
    values: list[schemas.ColumnValue],
    db: Session = Depends(get_db),
    obj_meta: models.ObjectMeta = Depends(get_obj_meta),
    scopes: ScopeManager = Depends(get_scopes),
):
    col_path = normalize_path(path)
    col_namespace_obj = crud.namespace.get(db=db, path=namespace)
    if col_namespace_obj is None or not scopes.can_write_in_namespace(
        col_namespace_obj
    ):
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=namespace_write_error_msg("column values"),
        )

    col = crud.column.get(db, path=col_path, namespace=col_namespace_obj)
    if col is None:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail="Column not found."
        )

    geos = geos_from_paths(
        paths=[val.path for val in values], namespace=namespace, db=db, scopes=scopes
    )

    # Pair the geography objects with their values.
    geos_values = [(geo, val.value) for geo, val in zip(geos, values)]
    crud.column.set_values(db, col=col, values=geos_values, obj_meta=obj_meta)
