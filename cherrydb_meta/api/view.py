"""Endpoints for views."""
from http import HTTPStatus
from typing import Callable, Optional

from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy.orm import Session

from cherrydb_meta import crud, models, schemas
from cherrydb_meta.api.base import MsgpackResponse, add_etag, parse_path
from cherrydb_meta.api.deps import can_read_localities, get_db, get_obj_meta, get_scopes
from cherrydb_meta.scopes import ScopeManager


def _namespace_with_read(
    db: Session,
    scopes: ScopeManager,
    path: str,
    base_namespace: Optional[str] = None,
) -> models.Namespace:
    """Loads a namespace with read access or raises an HTTP error.

    Also enforces the private join constraint: a view cannot reference
    private namespaces that are not its own. If `base_namespace` is provided,
    private namespaces with paths that do not match `base_namespace` are rejected.
    """
    namespace_obj = crud.namespace.get(db=db, path=path)
    if namespace_obj is None or not scopes.can_read_in_namespace(namespace_obj):
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=(
                f'Namespace "{path}" not found, or you do not have sufficient '
                "permissions to read in this namespace."
            ),
        )
    if (
        base_namespace is not None
        and not namespace_obj.public
        and namespace_obj.path != base_namespace
    ):
        raise HTTPException(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            detail=(
                "Cannot join across private namespaces: "
                f"namespace {namespace_obj.path} is private."
            ),
        )
    return namespace_obj


router = APIRouter()


@router.post(
    "/{namespace}",
    response_model=None,
    response_class=MsgpackResponse,
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
    if view_namespace_obj is None or not scopes.can_write_in_namespace(
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

    namespaces = {
        "layer": namespace if layer_namespace is None else layer_namespace,
        "template": namespace if template_namespace is None else template_namespace,
    }
    namespace_objs = {}
    for namespace_label, resource_namespace in namespaces.items():
        namespace_objs[namespace_label] = _namespace_with_read(
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

    view_obj, etag = crud.view.create(
        db=db,
        obj_in=obj_in,
        obj_meta=obj_meta,
        namespace=view_namespace_obj,
        template=template_obj,
        locality=locality_obj,
        layer=layer_obj,
    )
    geo_versions, col_values = crud.view.instantiate(db=db, view=view_obj)

    add_etag(response, etag)
    return MsgpackResponse(
        schemas.View(
            path=view_obj.path,
            namespace=view_obj.namespace.path,
            template=schemas.ViewTemplate.from_orm(view_obj.template_version),
            locality=view_obj.loc,
            layer=view_obj.layer,
            meta=view_obj.meta,
            valid_at=view_obj.at,
            proj=view_obj.proj,
            geographies=geo_versions,
            values=col_values,
        ).dict()
    )
