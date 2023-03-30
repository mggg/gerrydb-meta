"""Endpoints for districting graphs."""
from http import HTTPStatus
from typing import Callable

from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy.orm import Session

from cherrydb_meta import crud, models, schemas
from cherrydb_meta.api.base import (
    NamespacedObjectApi,
    add_etag,
    geo_set_from_paths,
    geos_from_paths,
)
from cherrydb_meta.api.deps import can_read_localities, get_db, get_obj_meta, get_scopes
from cherrydb_meta.scopes import ScopeManager


class GraphApi(NamespacedObjectApi):
    def _create(self, router: APIRouter) -> Callable:
        @router.post(
            "/{namespace}",
            response_model=self.get_schema,
            name=f"Create {self.obj_name_singular}",
            status_code=HTTPStatus.CREATED,
        )
        def create_route(
            *,
            response: Response,
            namespace: str,
            obj_in: schemas.GraphCreate,
            db: Session = Depends(get_db),
            obj_meta: models.ObjectMeta = Depends(get_obj_meta),
            scopes: ScopeManager = Depends(get_scopes),
        ):
            namespace_obj = self._namespace_with_write(
                db=db, scopes=scopes, path=namespace
            )
            geo_set_version = geo_set_from_paths(
                locality=obj_in.locality,
                layer=obj_in.layer,
                namespace=namespace,
                db=db,
                scopes=scopes,
            )

            # Assemble geographies from edges; verify that they exist
            # and are a subset of the geographies in the `GeoSetVersion`.
            edge_geo_paths = list(
                set(edge[0] for edge in obj_in.edges)
                | set(edge[1] for edge in obj_in.edges)
            )
            edge_geos = geos_from_paths(
                paths=edge_geo_paths, namespace=namespace, db=db, scopes=scopes
            )
            edge_geos_by_path = dict(zip(edge_geo_paths, edge_geos))
            graph, etag = self.crud.create(
                db=db,
                obj_in=obj_in,
                geo_set_version=geo_set_version,
                edge_geos=edge_geos_by_path,
                obj_meta=obj_meta,
                namespace=namespace_obj,
            )
            add_etag(response, etag)
            return schemas.Graph.from_orm(graph)

        return create_route


router = GraphApi(
    crud=crud.graph,
    get_schema=schemas.Graph,
    create_schema=schemas.GraphCreate,
    obj_name_singular="Graph",
    obj_name_plural="Graphs",
).router()
