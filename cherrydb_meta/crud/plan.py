"""CRUD operations and transformations for districting plans."""
import logging
import uuid
from typing import Tuple

from sqlalchemy import exc, insert,
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
        assignments: list[tuple[models.Geography, int]],
        obj_meta: models.ObjectMeta,
        namespace: models.Namespace,
    ) -> Tuple[models.Plan, uuid.UUID]:
        """Creates a new districting plan."""
        with db.begin(nested=True):
            plan = models.Plan(
                namespace_id=namespace.namespace_id,
                path=normalize_path(obj_in.path),
                set_version_id=geo_set_version.set_version_id,
                num_districts=len(set(label for _, label in assignments)),
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
                # TODO: Make this more specific--the primary goal is to capture the case
                # where the reference already exists.
                log.exception("Failed to create new districting plan.")
                raise CreateValueError(
                    "Failed to create canonical path to new districting plan. "
                    "(The path may already exist.)"
                )
               
            db.refresh(plan) 
            db.execute(
                insert(
                    models.PlanAssignment,
                    [
                        {
                            "plan_id": plan.plan_id,
                            "geo_id": geo.geo_id,
                            "assignment": assignment,
                        }
                        for geo, assignment in assignments
                    ]
                )
            )
            
        db.refresh(plan)
        return plan

    def get(
        self, db: Session, *, path: str, namespace: models.Namespace
    ) -> models.View | None:
        """Retrieves a districting plan by reference path.

        Args:
            path: Path to view (namespace excluded).
            namespace: View's namespace.
        """
        return (
            db.query(models.View)
            .filter(
                models.View.namespace_id == namespace.namespace_id,
                models.View.path == normalize_path(path),
            )
            .first()
        )
        
plan = CRPlan(models.Plan)
