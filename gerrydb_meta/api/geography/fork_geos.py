from http import HTTPStatus
from enum import Enum
import hashlib
from shapely import Polygon

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from geoalchemy2 import WKBElement
from typing import Optional

from gerrydb_meta import crud
import gerrydb_meta.models as models
from gerrydb_meta.api.deps import (
    can_read_localities,
    get_db,
    get_scopes,
    get_geo_import,
    get_user,
)
from gerrydb_meta.scopes import ScopeManager
import gerrydb_meta.schemas as schemas
from .list_geos import _get_path_hash_pairs
from sqlalchemy import text
from uvicorn.config import logger as log

fork_router = APIRouter()


def __validate_source_and_target_namespaces(
    source_namespace: str, target_namespace: str, db: Session, scopes: ScopeManager
):
    source_namespace_obj = crud.namespace.get(db=db, path=source_namespace)

    if source_namespace_obj is None or not scopes.can_read_in_namespace(
        source_namespace_obj
    ):
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=(
                f'Namespace "{source_namespace}" not found, or you do not have '
                "sufficient permissions to read geometries in this namespace."
            ),
        )

    target_namespace_obj = crud.namespace.get(db=db, path=target_namespace)

    # We enforce that the target namespace is writeable, because the only
    # reason that you would want to check if you can fork from one namespace
    # to another is to check if you can write new things to the target namespace.
    if target_namespace_obj is None or not scopes.can_write_in_namespace(
        target_namespace_obj
    ):
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=(
                f'Namespace "{target_namespace}" not found, or you do not have '
                "sufficient permissions to write geometries in this namespace."
            ),
        )

    return source_namespace_obj, target_namespace_obj


def __validate_forkability(
    source_namespace: str,
    source_layer: str,
    target_namespace: str,
    target_layer: str,
    source_geo_hash_pairs: set[tuple[str, str]],
    target_geo_hash_pairs: set[tuple[str, str]],
    allow_empty_target: bool = False,
    allow_empty_polys: bool = False,
):
    empty_polygon_wkb = Polygon().wkb
    empty_hash = hashlib.md5(WKBElement(empty_polygon_wkb, srid=4269).data).hexdigest()

    log.debug("Comparing geo path hash pairs")
    if len(source_geo_hash_pairs) == 0 and len(target_geo_hash_pairs) == 0:
        raise HTTPException(
            status_code=HTTPStatus.FORBIDDEN,
            detail=(
                f"Cannot fork column values from layer '{source_layer}' in "
                f"'{source_namespace}' to layer '{target_layer}' in '{target_namespace}' because "
                f"both the source and target layers do not contain any geographies."
            ),
        )

    if not allow_empty_polys and any(
        [pair[1] == empty_hash for pair in source_geo_hash_pairs]
    ):
        raise HTTPException(
            status_code=HTTPStatus.FORBIDDEN,
            detail=(
                f"Cannot fork column values from layer '{source_layer}' in "
                f"'{source_namespace}' to layer '{target_layer}' in '{target_namespace}' because "
                f"some of the source geographies have empty polygons and `allow_empty_polys` "
                f"is False."
            ),
        )

    if source_geo_hash_pairs == target_geo_hash_pairs:
        return

    diff_ts = target_geo_hash_pairs - source_geo_hash_pairs
    diff_st = source_geo_hash_pairs - target_geo_hash_pairs
    if len(diff_st) > 0 and len(diff_ts) > 0:
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail=(
                f"Cannot fork column values from layer '{source_layer}' in "
                f"'{source_namespace}' to layer '{target_layer}' in '{target_namespace}' because "
                f"some geometries in the target namespace/layer are different from the "
                f"geometries in the source namespace/layer."
            ),
        )
    if len(diff_ts) > 0:
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail=(
                f"Cannot fork column values from layer '{source_layer}' in "
                f"'{source_namespace}' to layer '{target_layer}' in '{target_namespace}' because some "
                f"geometries in the target namespace/layer are not present in the "
                f"source namespace/layer."
            ),
        )

    log.debug(len(target_geo_hash_pairs))
    if not (allow_empty_target and len(target_geo_hash_pairs) == 0):
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail=(
                f"Cannot fork column values from layer '{source_layer}' in "
                f"'{source_namespace}' to layer '{target_layer}' in '{target_namespace}' because some "
                f"geometries in the source namespace/layer are not present in the target "
                f"namespace/layer.",
            ),
        )


@fork_router.get(
    "/{target_namespace}/{loc_ref}/{target_layer}",
    response_model=None,
    dependencies=[Depends(can_read_localities)],
)
def check_forkability(
    target_namespace: str,
    loc_ref: str,
    target_layer: str,
    db: Session = Depends(get_db),
    scopes: ScopeManager = Depends(get_scopes),
    source_namespace: Optional[str] = Query(default=None),
    source_layer: Optional[str] = Query(default=None),
    allow_empty_target: bool = Query(default=False),
    allow_empty_polys: bool = Query(default=False),
):
    log.debug("Checking if migration is possible")
    _ = __validate_source_and_target_namespaces(
        source_namespace, target_namespace, db, scopes
    )

    source_geo_hash_pairs = set(
        _get_path_hash_pairs(source_namespace, loc_ref, source_layer, db)
    )
    target_geo_hash_pairs = set(
        _get_path_hash_pairs(target_namespace, loc_ref, target_layer, db)
    )

    __validate_forkability(
        source_namespace=source_namespace,
        source_layer=source_layer,
        target_namespace=target_namespace,
        target_layer=target_layer,
        source_geo_hash_pairs=source_geo_hash_pairs,
        target_geo_hash_pairs=target_geo_hash_pairs,
        allow_empty_target=allow_empty_target,
        allow_empty_polys=allow_empty_polys,
    )

    return source_geo_hash_pairs


@fork_router.post(
    "/{target_namespace}/{loc_ref}/{target_layer}",
    response_model=None,
    dependencies=[Depends(can_read_localities)],
)
def fork_geos_between_namespaces(
    *,
    target_namespace: str,
    loc_ref: str,
    target_layer: str,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_user),
    scopes: ScopeManager = Depends(get_scopes),
    source_namespace: Optional[str] = Query(default=None),
    source_layer: Optional[str] = Query(default=None),
    allow_empty_target: bool = Query(default=False),
    allow_empty_polys: bool = Query(default=False),
    notes: str = Query(default="THERE ARE NO NOTES"),
):
    log.debug("Checking if migration is possible")
    source_namespace_obj, target_namespace_obj = (
        __validate_source_and_target_namespaces(
            source_namespace, target_namespace, db, scopes
        )
    )

    # Now check that you are migrating from a public namespace.
    # At this point, the user has already shown that they have read access to
    # the source namespace, so we can give them information about that namespace
    # NOTE: We do not allow for migration from private namespaces to help prevent
    # leaking protected data to other users of the target namespace.
    if not source_namespace_obj.public:
        raise HTTPException(
            status_code=HTTPStatus.FORBIDDEN,
            detail=(
                f"Namespace '{source_namespace}' is not public, so you cannot fork "
                "values or geographies from it."
            ),
        )

    source_geo_hash_pairs = set(
        _get_path_hash_pairs(source_namespace, loc_ref, source_layer, db)
    )
    target_geo_hash_pairs = set(
        _get_path_hash_pairs(target_namespace, loc_ref, target_layer, db)
    )

    _ = __validate_forkability(
        source_namespace=source_namespace,
        source_layer=source_layer,
        target_namespace=target_namespace,
        target_layer=target_layer,
        source_geo_hash_pairs=source_geo_hash_pairs,
        target_geo_hash_pairs=target_geo_hash_pairs,
        allow_empty_target=allow_empty_target,
        allow_empty_polys=allow_empty_polys,
    )
    # We are now guaranteed that the missing paths do not have a conflicting
    # geography in the target namespace.

    log.debug("Migrating the geos")

    if notes == "THERE ARE NO NOTES":
        notes = (
            f"Forked {len(target_geo_hash_pairs)} geographies from "
            f"{source_namespace}/{loc_ref}/{source_layer} to "
            f"{target_namespace}/{loc_ref}/{target_layer} "
            f"by a direct call to the API."
        )

    schema_meta_obj = schemas.ObjectMetaCreate(notes=notes)
    meta_obj = crud.obj_meta.create(db=db, obj_in=schema_meta_obj, user=user)

    geo_import, _ = crud.geo_import.create(
        db=db, obj_meta=meta_obj, namespace=target_namespace_obj
    )

    return crud.geography.fork_bulk(
        db=db,
        source_namespace=source_namespace_obj,
        target_namespace=target_namespace_obj,
        create_geos_path_hash=source_geo_hash_pairs - target_geo_hash_pairs,
        geo_import=geo_import,
        obj_meta=meta_obj,
    )
