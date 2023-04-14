"""Endpoints for views."""
from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException, Response
from fastapi.responses import StreamingResponse
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

router = APIRouter()


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
    render_uuid, gpkg_path, temp_dir = view_to_gpkg(
        context=render_ctx, db_config=db_config
    )

    return StreamingResponse(
        open(gpkg_path, "rb"),
        media_type="application/geopackage+sqlite3",
        headers={
            "ETag": etag.hex,
            "X-GerryDB-View-Render-ID": render_uuid.hex,
        },
    )
