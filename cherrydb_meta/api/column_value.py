"""API operations for manipulating column values."""
from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from cherrydb_meta import crud, models, schemas
from cherrydb_meta.api.base import namespace_write_error_msg
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
    from time import time

    tic = time()
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

    # Break geography paths into (namespace, path) form.
    #
    # There are few realistic use cases where a user would want to upload values
    # for a column across multiple namespaces at once, but it is often true
    # that the column namespace (where the user needs write access) differs
    # from the geographic namespace(s) (where the user only needs read access),
    # so we might as well parse absolute paths.
    namespaced_values = {}
    for value in values:
        geo_path = normalize_path(value.path)
        if geo_path.startswith("/"):
            parts = normalize_path(geo_path).split("/")
            try:
                namespaced_values[(parts[1], "/".join(parts[2:]))] = value.value
            except IndexError:
                raise HTTPException(
                    status_code=HTTPStatus.BAD_REQUEST,
                    detail=(
                        f'Bad column path "{geo_path}": namespaced paths must '
                        "contain a namespace and a namespace-relative path, i.e. "
                        "/<namespace>/<path>"
                    ),
                )
        else:
            namespaced_values[(namespace, geo_path)] = value.value

    # Verify that the user has read access in all namespaces
    # the geographies are in.
    # TODO: This could be slow when the geographies are spread across
    # a lot of namespaces -- investigate?
    geo_namespaces = {namespace for namespace, _ in namespaced_values}
    for geo_namespace in geo_namespaces:
        geo_namespace_obj = crud.namespace.get(db=db, path=geo_namespace)
        if geo_namespace_obj is None or not scopes.can_read_in_namespace(
            geo_namespace_obj
        ):
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND,
                detail=(
                    f'Namespace "{geo_namespace}" not found, or you do not have '
                    "sufficient permissions to read geographies in this namespace."
                ),
            )

    # Get the geographies in bulk by path; fail if any are unknown.
    geos = crud.geography.get_bulk(db, namespaced_paths=namespaced_values)
    if len(geos) < len(namespaced_values):
        missing = set(namespaced_values) - set(
            (geo.namespace.path, geo.path) for geo in geos
        )
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f"Geographies not found: {', '.join(missing)}",
        )

    # Pair the geography objects with their values.
    geos_values = [
        (geo, namespaced_values[(geo.namespace.path, geo.path)]) for geo in geos
    ]
    crud.column.set_values(db, col=col, values=geos_values, obj_meta=obj_meta)
