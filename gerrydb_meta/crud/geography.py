"""CRUD operations and transformations for geographic imports."""

import logging
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Collection

from geoalchemy2.elements import WKBElement
from sqlalchemy import and_, insert, or_, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import StatementError
from sqlalchemy.orm import Session

from gerrydb_meta import models, schemas
from gerrydb_meta.crud.base import NamespacedCRBase, normalize_path
from gerrydb_meta.exceptions import BulkCreateError, BulkPatchError
from uvicorn.config import logger as log


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
                    normalize_path(obj_in.path, case_sensitive_uid=True)
                    for obj_in in objs_in
                ),
                models.Geography.namespace_id == namespace.namespace_id,
            )
            .all()
        )
        log.debug("EXISTING GEOS %s", existing_geos)
        if existing_geos:
            raise BulkCreateError(
                "Cannot create geographies that already exist.",
                paths=[geo.path for geo in existing_geos],
            )

        # Need to check for unique paths since otherwise the db will just
        # insert the first occurrence which could be confusing. (This error
        # should almost never be raised in practice.)
        paths = [
            normalize_path(obj_in.path, case_sensitive_uid=True) for obj_in in objs_in
        ]

        if len(paths) != len(set(paths)):
            raise BulkCreateError(
                "Cannot create geographies with duplicate paths.",
                paths=[path for path in paths if paths.count(path) > 1],
            )

        with db.begin(nested=True):
            geos = list(
                db.scalars(
                    insert(models.Geography).returning(models.Geography),
                    [
                        {
                            "path": normalize_path(
                                obj_in.path, case_sensitive_uid=True
                            ),
                            "meta_id": obj_meta.meta_id,
                            "namespace_id": namespace.namespace_id,
                        }
                        for obj_in in objs_in
                    ],
                )
            )

            try:
                values_list = [
                    {
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
                    }
                    for obj_in in objs_in
                ]

                # Note: We use on_conflict_do_update with the constraint name of the unique index.
                # In the SET clause we “update” with the same values so that if there’s a conflict,
                # nothing really changes.
                # Note: This also produces gaps in the geo_bin_id index sequence, but that should
                # be fine so long as we don't try to upload > 400 copies of the entire census
                # geography table (there is a limit of ~4B rows for this index)
                upsert_stmt = (
                    pg_insert(models.GeoBin)
                    .values(values_list)
                    .on_conflict_do_update(
                        constraint="uq_geo_bin_geometry_hash",
                        set_={
                            "geography": pg_insert(models.GeoBin).excluded.geography,
                            "internal_point": pg_insert(
                                models.GeoBin
                            ).excluded.internal_point,
                        },
                    )
                    .returning(models.GeoBin.geo_bin_id)
                )

                geo_bin_list = list(db.scalars(upsert_stmt))

            except Exception as ex:
                log.exception(
                    "Geography insert failed, likely due to invalid geometries. Full error below: %s",
                    ex,
                )
                raise BulkCreateError(
                    "Failed to insert geometries. This is likely due to invalid Geometries; please"
                    " ensure geometries can be encoded in WKB format."
                ) from ex

            try:
                geo_versions = list(
                    db.scalars(
                        insert(models.GeoVersion).returning(models.GeoVersion),
                        [
                            {
                                "import_id": geo_import.import_id,
                                "geo_id": geo.geo_id,
                                "valid_from": now,
                                "geo_bin_id": bin_id,
                            }
                            for geo, bin_id in zip(geos, geo_bin_list)
                        ],
                    )
                )

            except Exception as ex:
                raise BulkCreateError("Failed at inserting GeoVersions.") from ex

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
        # FIXME: This does not have the new GeoBin table yet.
        """Updates geographies in bulk."""
        now = datetime.now(timezone.utc)
        existing_geos = (
            db.query(models.Geography)
            .filter(
                models.Geography.path.in_(
                    normalize_path(obj_in.path, case_sensitive_uid=True)
                    for obj_in in objs_in
                ),
                models.Geography.namespace_id == namespace.namespace_id,
            )
            .all()
        )

        # This is technically caught by the next error, but this is more
        # informative.
        paths = [
            normalize_path(obj_in.path, case_sensitive_uid=True) for obj_in in objs_in
        ]
        if len(paths) != len(set(paths)):
            raise BulkPatchError(
                "Cannot patch geographies with duplicate paths.",
                paths=[path for path in paths if paths.count(path) > 1],
            )

        if len(existing_geos) < len(objs_in):
            missing = set(
                normalize_path(geo.path, case_sensitive_uid=True) for geo in objs_in
            ) - set(geo.path for geo in existing_geos)
            raise BulkPatchError(
                "Cannot update geographies that do not exist.", paths=list(missing)
            )

        geos_by_path = {geo.path: geo for geo in existing_geos}
        geos_ordered = [
            geos_by_path[normalize_path(geo.path, case_sensitive_uid=True)]
            for geo in objs_in
        ]

        with db.begin(nested=True):
            db.execute(
                update(models.GeoVersion)
                .where(
                    models.GeoVersion.geo_id.in_(geo.geo_id for geo in existing_geos),
                    models.GeoVersion.valid_to.is_(None),
                )
                .values(valid_to=now)
            )

            try:
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
            except StatementError as ex:
                log.exception(
                    "Geography patching failed, likely due to invalid geometries."
                )
                raise BulkPatchError(
                    "Failed to patch geometries. Geometries must be encoded in WKB format."
                ) from ex

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
