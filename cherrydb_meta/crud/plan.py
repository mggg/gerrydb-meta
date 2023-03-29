"""CRUD operations and transformations for districting plans."""
import logging
import uuid
from typing import Tuple

from sqlalchemy import exc, insert, select
from sqlalchemy.orm import Session

from cherrydb_meta import models, schemas
from cherrydb_meta.crud.base import NamespacedCRBase, normalize_path
from cherrydb_meta.exceptions import CreateValueError

log = logging.getLogger()


class CRPlan(NamespacedCRBase[models.Plan, schemas.PlanCreate]):
    def create(
        self,
        db: Session,
        *,
        obj_in: schemas.PlanCreate,
        geo_set_version: models.GeoSetVersion,
        assignments: dict[models.Geography, str],
        obj_meta: models.ObjectMeta,
        namespace: models.Namespace,
    ) -> Tuple[models.Plan, uuid.UUID]:
        """Creates a new districting plan."""
        set_geo_ids = set(
            db.scalars(
                select(models.GeoSetMember.geo_id).filter(
                    models.GeoSetMember.set_version_id
                    == geo_set_version.set_version_id,
                )
            )
        )
        assignment_geo_ids = set(geo.geo_id for geo in assignments)
        geo_ids_not_in_set = assignment_geo_ids - set_geo_ids
        if geo_ids_not_in_set:
            geos_not_in_set = [
                geo for geo in assignments if geo.geo_id in geo_ids_not_in_set
            ]
            raise CreateValueError(
                "Geographies not in set defined by locality "
                f'"{geo_set_version.loc.canonical_ref.path}" and geographic layer '
                f'"{geo_set_version.layer.full_path}": '
                f"{', '.join(geo.full_path for geo in geos_not_in_set)}"
            )

        with db.begin(nested=True):
            plan = models.Plan(
                namespace_id=namespace.namespace_id,
                path=normalize_path(obj_in.path),
                set_version_id=geo_set_version.set_version_id,
                num_districts=len(set(assignments.values())),
                complete=(len(assignments) == len(set_geo_ids)),
                description=obj_in.description,
                source_url=obj_in.source_url,
                districtr_id=obj_in.districtr_id,
                daves_id=obj_in.daves_id,
                meta_id=obj_meta.meta_id,
            )
            db.add(plan)

            try:
                db.flush()
            except exc.SQLAlchemyError:
                log.exception("Failed to create new districting plan.")
                raise CreateValueError(
                    "Failed to create canonical path to new districting plan. "
                    "(The path may already exist.)"
                )

            db.refresh(plan)
            db.execute(
                insert(models.PlanAssignment),
                [
                    {
                        "plan_id": plan.plan_id,
                        "geo_id": geo.geo_id,
                        "assignment": assignment,
                    }
                    for geo, assignment in assignments.items()
                ],
            )
            etag = self._update_etag(db, namespace)

        db.refresh(plan)
        return plan, etag

    def get(
        self, db: Session, *, path: str, namespace: models.Namespace
    ) -> models.Plan | None:
        """Retrieves a districting plan by reference path.

        Args:
            path: Path to plan (namespace excluded).
            namespace: Plan's namespace.
        """
        return (
            db.query(models.Plan)
            .filter(
                models.Plan.namespace_id == namespace.namespace_id,
                models.Plan.path == normalize_path(path),
            )
            .first()
        )


plan = CRPlan(models.Plan)
