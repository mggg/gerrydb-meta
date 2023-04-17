"""Endpoints for views."""
import gzip
import logging
import os
import subprocess
from datetime import timedelta
from http import HTTPStatus
from pathlib import Path
from typing import Generator

import google.auth
from fastapi import APIRouter, Depends, HTTPException, Response
from fastapi.responses import RedirectResponse, StreamingResponse
from google.cloud import storage
from sqlalchemy.orm import Session

from gerrydb_meta import crud, models, schemas
from gerrydb_meta.api.base import add_etag, namespace_with_read, parse_path
from gerrydb_meta.api.deps import (
    can_read_localities,
    get_db,
    get_obj_meta,
    get_ogr2ogr_db_config,
    get_scopes,
)
from gerrydb_meta.render import view_to_gpkg
from gerrydb_meta.scopes import ScopeManager

log = logging.getLogger()

router = APIRouter()
CHUNK_SIZE = 32 * 1024 * 1024  # for gzipping rendered views
GPKG_MEDIA_TYPE = "application/geopackage+sqlite3"


@router.post(
    "/{namespace}",
    response_model=schemas.ViewMeta,
    status_code=HTTPStatus.CREATED,
    dependencies=[Depends(can_read_localities)],
)
def create_view(
    *,
    response: Response,
    namespace: str,
    obj_in: schemas.ViewCreate,
    db: Session = Depends(get_db),
    obj_meta: models.ObjectMeta = Depends(get_obj_meta),
    scopes: ScopeManager = Depends(get_scopes),
):
    view_namespace_obj = crud.namespace.get(db=db, path=namespace)
    if view_namespace_obj is None or not scopes.can_write_derived_in_namespace(
        view_namespace_obj
    ):
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=(
                f'Namespace "{namespace}" not found, or you do not have '
                "sufficient permissions to write views in this namespace."
            ),
        )

    locality_obj = crud.locality.get_by_ref(db=db, path=obj_in.locality)
    if locality_obj is None:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail="Locality not found."
        )

    layer_namespace, layer_path = parse_path(obj_in.layer)
    template_namespace, template_path = parse_path(obj_in.template)
    if obj_in.graph is None:
        graph_namespace = graph_path = None
    else:
        graph_namespace, graph_path = parse_path(obj_in.graph)

    namespaces = {
        "layer": namespace if layer_namespace is None else layer_namespace,
        "template": namespace if template_namespace is None else template_namespace,
        "graph": namespace if graph_namespace is None else graph_namespace,
    }
    namespace_objs = {}
    for namespace_label, resource_namespace in namespaces.items():
        namespace_objs[namespace_label] = namespace_with_read(
            db=db, scopes=scopes, path=resource_namespace, base_namespace=namespace
        )

    template_obj = crud.view_template.get(
        db, path=template_path, namespace=namespace_objs["template"]
    )
    if template_obj is None:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail="View template not found."
        )

    layer_obj = crud.geo_layer.get(
        db, path=layer_path, namespace=namespace_objs["layer"]
    )
    if layer_obj is None:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail="Geographic layer not found."
        )

    if graph_path is None:
        graph_obj = None
    else:
        graph_obj = crud.graph.get(
            db, path=graph_path, namespace=namespace_objs["graph"]
        )
        if graph_obj is None:
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND, detail="Dual graph not found."
            )

    view_obj, etag = crud.view.create(
        db=db,
        obj_in=obj_in,
        obj_meta=obj_meta,
        namespace=view_namespace_obj,
        template=template_obj,
        locality=locality_obj,
        layer=layer_obj,
        graph=graph_obj,
    )
    add_etag(response, etag)
    return schemas.ViewMeta.from_orm(view_obj)


@router.get(
    "/{namespace}/{path:path}",
    response_model=schemas.ViewMeta,
    dependencies=[Depends(can_read_localities)],
)
def get_view(
    *,
    response: Response,
    namespace: str,
    path: str,
    db: Session = Depends(get_db),
    scopes: ScopeManager = Depends(get_scopes),
):
    view_namespace_obj = crud.namespace.get(db=db, path=namespace)
    if view_namespace_obj is None or not scopes.can_read_in_namespace(
        view_namespace_obj
    ):
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=(
                f'Namespace "{namespace}" not found, or you do not have '
                "sufficient permissions to write views in this namespace."
            ),
        )

    view_obj = crud.view.get(db=db, namespace=view_namespace_obj, path=path)
    if view_obj is None:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f"View not found in namespace.",
        )

    etag = crud.view.etag(db, view_namespace_obj)
    add_etag(response, etag)
    return schemas.ViewMeta.from_orm(view_obj)


@router.post(
    "/{namespace}/{path:path}",
    status_code=HTTPStatus.CREATED,
    dependencies=[Depends(can_read_localities)],
    response_class=StreamingResponse,
)
def render_view(
    *,
    namespace: str,
    path: str,
    db: Session = Depends(get_db),
    db_config: str = Depends(get_ogr2ogr_db_config),
    scopes: ScopeManager = Depends(get_scopes),
):
    view_namespace_obj = crud.namespace.get(db=db, path=namespace)
    if view_namespace_obj is None or not scopes.can_read_in_namespace(
        view_namespace_obj
    ):
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=(
                f'Namespace "{namespace}" not found, or you do not have '
                "sufficient permissions to write views in this namespace."
            ),
        )

    view_obj = crud.view.get(db=db, namespace=view_namespace_obj, path=path)
    if view_obj is None:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f"View not found in namespace.",
        )

    etag = crud.view.etag(db, view_namespace_obj)
    render_ctx = crud.view.render(db=db, view=view_obj)
    render_uuid, gpkg_path = view_to_gpkg(context=render_ctx, db_config=db_config)

    bucket_name = os.getenv("GCS_BUCKET")
    if bucket_name is not None:
        try:
            credentials, project_id = google.auth.default()

            storage_client = storage.Client(project=project_id, credentials=credentials)
            bucket = storage_client.bucket(bucket_name)
            gzipped_path = gpkg_path.with_suffix(".gpkg.gz")
            subprocess.run(["gzip", "-k", str(gpkg_path)], check=True)

            blob = bucket.blob(f"{render_uuid.hex}.gpkg.gz")
            blob.metadata = {
                "cache-control": "public, max-age=604800",  # 1 week
                "content-encoding": "gzip",
                "content-type": GPKG_MEDIA_TYPE,
                "x-gerrydb-view-render-id": render_uuid.hex,
            }

            blob.upload_from_filename(gzipped_path)
            redirect_url = blob.generate_signed_url(
                version="v4",
                expiration=timedelta(minutes=15),
                method="GET",
                # see https://stackoverflow.com/a/64245028
                service_account_email=credentials.service_account_email,
                access_token=credentials.token,
            )
            return RedirectResponse(
                url=redirect_url,
                status_code=HTTPStatus.PERMANENT_REDIRECT,
            )
        except Exception as ex:
            log.exception(
                "Failed to serve rendered view via Google Cloud Storage. "
                "Falling back to direct streaming."
            )
            raise ex

    return StreamingResponse(
        _async_read_and_delete(gpkg_path),
        media_type=GPKG_MEDIA_TYPE,
        headers={
            "ETag": etag.hex,
            "X-GerryDB-View-Render-ID": render_uuid.hex,
        },
    )


async def _async_read_and_delete(path: Path) -> Generator[bytes, None, None]:
    """Asynchronously reads a temporary file, then deletes it."""
    with open(path, "rb") as fp:
        yield fp.read()
    path.unlink()
