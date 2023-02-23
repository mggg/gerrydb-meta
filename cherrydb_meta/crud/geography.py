"""CRUD operations and transformations for geographic imports."""
import logging
import uuid
from collections import defaultdict
from typing import Collection, Tuple

from geoalchemy2.elements import WKBElement
from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session

from cherrydb_meta import models, schemas
from cherrydb_meta.crud.base import NamespacedCRBase
from cherrydb_meta.exceptions import BulkCreateError

log = logging.getLogger()


class CRGeography(NamespacedCRBase[models.Geography, None]):
    def create_bulk(
        self,
        db: Session,
        *,
        objs_in: list[schemas.GeographyCreate],
        obj_meta: models.ObjectMeta,
        geo_import: models.GeoImport,
        namespace: models.Namespace,
    ) -> Tuple[list[models.Geography], uuid.UUID]:
        """Creates new geographies, possibly in bulk."""
        with db.begin(nested=True):
            # TODO: check for existing, raise error.
            existing_geos = (
                db.query(models.Geography.path)
                .filter(
                    models.Geography.path.in_(obj_in.path for obj_in in objs_in),
                    models.Geography.namespace_id == namespace.namespace_id,
                )
                .all()
            )

            geos = []
            for obj_in in objs_in:
                geo = models.Geography(
                    path=obj_in.path,
                    meta_id=obj_meta.meta_id,
                    namespace_id=namespace.namespace_id,
                )
                db.add(geo)
                geos.append(geo)
            db.flush()

            for geo, obj_in in zip(geos, objs_in):
                db.refresh(geo)  # TODO: ouch?
                db.add(
                    models.GeoVersion(
                        import_id=geo_import.import_id,
                        geo_id=geo.geo_id,
                        geography=WKBElement(obj_in.geography, srid=4326),
                    )
                )

            etag = self._update_etag(db, namespace)

        db.flush()
        if existing_geos:
            raise BulkCreateError(
                "Cannot create geographies that already exist.",
                paths=[geo.path for geo in existing_geos],
            )
        return geos, etag

    def patch_bulk(
        self,
        db: Session,
        *,
        objs_in: list[schemas.GeographyCreate],
        obj_meta: models.ObjectMeta,
        geo_import: models.GeoImport,
        namespace: models.Namespace,
    ) -> Tuple[list[models.Geography], uuid.UUID]:
        """Creates a new geographic import."""
        with db.begin(nested=True):
            existing_geos = (
                db.query(models.Geography.path)
                .filter(
                    models.Geography.path.in_(obj_in.path for obj_in in objs_in),
                    models.Geography.namespace_id == namespace.namespace_id,
                )
                .all()
            )
            if existing_geos:
                raise BulkCreateError(
                    "Cannot create geographies that already exist.",
                    paths=[geo.path for geo in existing_geos],
                )

            geos = [
                models.Geography(
                    path=obj_in.path,
                    meta_id=obj_meta.meta_id,
                    namespace_id=namespace.namespace_id,
                )
                for obj_in in objs_in
            ]
            db.flush()

            for geo, obj_in in zip(geos, objs_in):
                db.add(
                    models.GeoVersion(
                        import_id=geo_import.import_id,
                        geo_id=geo.geo_id,
                        geometry=obj_in.geometry,
                    )
                )
            etag = self._update_etag(db, namespace)

        db.flush()
        return geos, etag

    def get(
        self, db: Session, *, path: str, namespace: models.Namespace
    ) -> models.Geography | None:
        """Gets a geography by path."""
        return (
            db.query(models.Geography)
            .filter(
                models.Geography.namespace_id == namespace.namespace_id,
                models.Geography.path == path,
            )
            .first()
        )

    def get_bulk(
        self, db: Session, *, namespaced_paths: Collection[tuple[str]]
    ) -> list[models.Geography]:
        """Gets all geographies referenced by `namespaced_paths`."""
        # Group paths by namespace.
        paths_by_namespace: dict[str, list[str]] = defaultdict(lambda: [])
        for namespace, path in namespaced_paths:
            paths_by_namespace[namespace].append(path)

        namespaces = (
            db.query(models.Namespace.path, models.Namespace.namespace_id)
            .filter(models.Namespace.path.in_(paths_by_namespace))
            .all()
        )
        namespace_ids = {row.path: row.namespace_id for row in namespaces}

        namespace_clauses = [
            and_(
                models.Geography.namespace_id == namespace_ids[namespace],
                models.Geography.path.in_(paths),
            )
            for namespace, paths in paths_by_namespace.items()
        ]

        return db.query(models.Geography).filter(or_(*namespace_clauses)).all()


geography = CRGeography(models.Geography)
