"""CRUD operations and transformations for geographic imports."""
import logging
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Collection

from geoalchemy2.elements import WKBElement
from sqlalchemy import and_, insert, or_, update
from sqlalchemy.orm import Session

from cherrydb_meta import models, schemas
from cherrydb_meta.crud.base import NamespacedCRBase, normalize_path
from cherrydb_meta.exceptions import BulkCreateError, BulkPatchError

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
    ) -> tuple[list[tuple[models.Geography, models.GeoVersion]], uuid.UUID]:
        """Creates new geographies in bulk."""
        now = datetime.now(timezone.utc)
        existing_geos = (
            db.query(models.Geography.path)
            .filter(
                models.Geography.path.in_(
                    normalize_path(obj_in.path) for obj_in in objs_in
                ),
                models.Geography.namespace_id == namespace.namespace_id,
            )
            .all()
        )
        if existing_geos:
            raise BulkCreateError(
                "Cannot create geographies that already exist.",
                paths=[geo.path for geo in existing_geos],
            )

        with db.begin(nested=True):
            geos = list(
                db.scalars(
                    insert(models.Geography).returning(models.Geography),
                    [
                        {
                            "path": normalize_path(obj_in.path),
                            "meta_id": obj_meta.meta_id,
                            "namespace_id": namespace.namespace_id,
                        }
                        for obj_in in objs_in
                    ],
                )
            )
            geo_versions = list(
                db.scalars(
                    insert(models.GeoVersion).returning(models.GeoVersion),
                    [
                        {
                            "import_id": geo_import.import_id,
                            "geo_id": geo.geo_id,
                            "geography": (
                                None
                                if obj_in.geography is None
                                else WKBElement(obj_in.geography, srid=4269)
                            ),
                            "internal_point": (
                                None
                                if obj_in.internal_point is None
                                else WKBElement(obj_in.internal_point, srid=4269)
                            ),
                            "valid_from": now,
                        }
                        for geo, obj_in in zip(geos, objs_in)
                    ],
                )
            )
            etag = self._update_etag(db, namespace)

        db.flush()
        return list(zip(geos, geo_versions)), etag

    def patch_bulk(
        self,
        db: Session,
        *,
        objs_in: list[schemas.GeographyPatch],
        geo_import: models.GeoImport,
        namespace: models.Namespace,
    ) -> tuple[list[tuple[models.Geography, models.GeoVersion]], uuid.UUID]:
        """Updates geographies in bulk."""
        now = datetime.now(timezone.utc)
        existing_geos = (
            db.query(models.Geography)
            .filter(
                models.Geography.path.in_(
                    normalize_path(obj_in.path) for obj_in in objs_in
                ),
                models.Geography.namespace_id == namespace.namespace_id,
            )
            .all()
        )
        if len(existing_geos) < len(objs_in):
            missing = set(normalize_path(geo.path) for geo in objs_in) - set(
                geo.path for geo in existing_geos
            )
            raise BulkPatchError(
                "Cannot update geographies that do not exist.", paths=list(missing)
            )

        geos_by_path = {geo.path: geo for geo in existing_geos}
        geos_ordered = [geos_by_path[normalize_path(geo.path)] for geo in objs_in]

        with db.begin(nested=True):
            geo_versions = db.scalars(
                insert(models.GeoVersion).returning(models.GeoVersion),
                [
                    {
                        "import_id": geo_import.import_id,
                        "geo_id": geo.geo_id,
                        "geography": (
                            None
                            if obj_in.geography is None
                            else WKBElement(obj_in.geography, srid=4269)
                        ),
                        "internal_point": (
                            None
                            if obj_in.internal_point is None
                            else WKBElement(obj_in.internal_point, srid=4269)
                        ),
                        "valid_from": now,
                    }
                    for geo, obj_in in zip(geos_ordered, objs_in)
                ],
            )
            db.execute(
                update(models.GeoVersion)
                .where(
                    models.GeoVersion.geo_id.in_(geo.geo_id for geo in existing_geos),
                    models.GeoVersion.valid_to.is_(None),
                )
                .values(valid_to=now)
            )
            etag = self._update_etag(db, namespace)

        db.flush()
        return list(zip(geos_ordered, geo_versions)), etag

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
        self, db: Session, *, namespaced_paths: Collection[tuple[str, str]]
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
