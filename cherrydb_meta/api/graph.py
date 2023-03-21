"""Endpoints for districting plans."""
from http import HTTPStatus
from typing import Callable

from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy.orm import Session

from cherrydb_meta import crud, models, schemas
from cherrydb_meta.api.base import (
    NamespacedObjectApi,
    add_etag,
    parse_path,
    geo_set_from_paths,
    geos_from_paths,
)
from cherrydb_meta.api.deps import can_read_localities, get_db, get_obj_meta, get_scopes
from cherrydb_meta.scopes import ScopeManager


class PlanApi(NamespacedObjectApi):
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
            obj_in: schemas.PlanCreate,
            db: Session = Depends(get_db),
            obj_meta: models.ObjectMeta = Depends(get_obj_meta),
            scopes: ScopeManager = Depends(get_scopes),
        ):
            plan_namespace_obj = self._namespace_with_write(
                db=db, scopes=scopes, path=namespace
            )
            if plan_namespace_obj is None or not scopes.can_write_in_namespace(
                plan_namespace_obj
            ):
                raise HTTPException(
                    status_code=HTTPStatus.NOT_FOUND,
                    detail=(
                        f'Namespace "{namespace}" not found, or you do not have '
                        "sufficient permissions to write plans in this namespace."
                    ),
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

            set_geos = [member.geo for member in geo_set_version.members]
            not_in_geo_set = set(geo.geo_id for geo in edge_geos) - set(
                geo.geo_id for geo in set_geos
            )
            if not_in_geo_set:
                bad_geo_paths = [
                    geo.full_path for geo in edge_geos if geo.geo_id in not_in_geo_set
                ]
                raise HTTPException(
                    status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
                    detail=(
                        "Geographies not associated with locality and layer: "
                        f"{', '.join(bad_geo_paths)}"
                    ),
                )

        return create_route


router = PlanApi(
    crud=crud.plan,
    get_schema=schemas.Plan,
    create_schema=schemas.PlanCreate,
    obj_name_singular="Plan",
    obj_name_plural="Plans",
).router()
