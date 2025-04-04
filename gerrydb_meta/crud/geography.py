"""CRUD operations and transformations for geographic imports."""

import logging
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Collection
import hashlib

from geoalchemy2.elements import WKBElement
from sqlalchemy import and_, insert, or_, update, select, func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import StatementError
from sqlalchemy.orm import Session

from gerrydb_meta import models, schemas
from gerrydb_meta.crud.base import NamespacedCRBase, normalize_path
from gerrydb_meta.exceptions import BulkCreateError, BulkPatchError
from uvicorn.config import logger as log


class CRGeography(NamespacedCRBase[models.Geography, None]):
    def _get_existing_geos(
        self,
        db: Session,
        objs_in: list[schemas.GeographyCreate],
        namespace: models.Namespace,
    ) -> list[models.Geography]:
        return (
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

    def _validate_create_geos(
        self,
        db: Session,
        objs_in: list[schemas.GeographyCreate],
        namespace: models.Namespace,
    ) -> None:
        existing_geos = self._get_existing_geos(
            db=db, objs_in=objs_in, namespace=namespace
        )

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

        return

    def _get_missing_geo_bins(
        self, db: Session, hash_dict: dict[str, schemas.GeographyCreate]
    ):
        hash_keys = list(hash_dict.keys())

        # The hashes have a unique constraint in the db, so this will be fine.
        results = db.execute(
            select(
                models.GeoBin,
                func.encode(models.GeoBin.geometry_hash, "hex").label("geom_hex"),
            ).where(func.encode(models.GeoBin.geometry_hash, "hex").in_(hash_keys))
        ).all()

        existing_hsh_to_bin_dict = {
            row.geom_hex: row.GeoBin.geo_bin_id for row in results
        }

        return (
            existing_hsh_to_bin_dict,
            set(hash_keys) - set(existing_hsh_to_bin_dict.keys()),
        )

    def _insert_missing_geo_hashes(
        self,
        *,
        db: Session,
        hash_dict: dict[str, schemas.GeographyCreate],
        existing_hsh_to_bin_dict: dict[str, int],
        missing_hashes: set[str],
    ) -> dict[str, int]:
        try:
            values_list = []
            for h in missing_hashes:
                obj_in = hash_dict[h]

                values_list.append(
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
                )
            result = db.execute(
                insert(models.GeoBin).returning(
                    models.GeoBin.geo_bin_id, models.GeoBin.geometry_hash
                ),
                values_list,
            )
            bin_hash_list = [(bin_id, hsh.hex()) for bin_id, hsh in result.all()]
        except Exception as ex:
            log.exception(
                "Geography insert failed, likely due to invalid geometries. Full error below: %s",
                ex,
            )
            raise BulkCreateError(
                "Failed to insert geometries. This is likely due to invalid Geometries; please"
                " ensure geometries can be encoded in WKB format."
            ) from ex

        for bin_id, hsh in bin_hash_list:
            assert hsh not in existing_hsh_to_bin_dict, "Duplicate hash in db"
            existing_hsh_to_bin_dict[hsh] = bin_id

        return existing_hsh_to_bin_dict

    def _update_geo_hashes(
        self,
        db: Session,
        objs_in: list[schemas.GeographyCreate],
    ) -> tuple[dict[str, int], dict[str, str]]:
        hash_obj_dict = {
            (
                hashlib.md5(WKBElement(obj_in.geography, srid=4269).data).hexdigest()
                if obj_in.geography
                else None
            ): obj_in
            for obj_in in objs_in
        }

        hash_bin_dict, missing_hashes = self._get_missing_geo_bins(
            db=db, hash_dict=hash_obj_dict
        )
        if missing_hashes:
            hash_bin_dict = self._insert_missing_geo_hashes(
                db=db,
                hash_dict=hash_obj_dict,
                existing_hsh_to_bin_dict=hash_bin_dict,
                missing_hashes=missing_hashes,
            )
        path_hash_dict = {obj_in.path: hsh for hsh, obj_in in hash_obj_dict.items()}

        try:
            assert set(hash_bin_dict.keys()) == set(hash_obj_dict.keys())
            assert len(set(hash_bin_dict.keys())) == len(hash_bin_dict.keys())
            assert len(path_hash_dict) == len(objs_in)
        except AssertionError as ex:
            log.exception(ex)
            raise BulkCreateError(
                "Unexpected error when creating geometry hashes."
            ) from ex

        return hash_bin_dict, path_hash_dict

    def _insert_geo_versions(
        self,
        db: Session,
        path_geos_dict: dict[str, models.Geography],
        objs_in: list[schemas.GeographyCreate],
        geo_import: models.GeoImport,
        valid_from: datetime,
    ):
        hash_bin_dict, path_hash_dict = self._update_geo_hashes(db=db, objs_in=objs_in)

        try:
            geo_id_to_version_dict = {
                ver.geo_id: ver
                for ver in list(
                    db.scalars(
                        insert(models.GeoVersion).returning(models.GeoVersion),
                        [
                            {
                                "import_id": geo_import.import_id,
                                "geo_id": geo.geo_id,
                                "valid_from": valid_from,
                                "geo_bin_id": hash_bin_dict[path_hash_dict[path]],
                            }
                            for path, geo in path_geos_dict.items()
                        ],
                    )
                )
            }

        except Exception as ex:
            log.exception(ex)
            raise BulkCreateError("Failed at inserting GeoVersions.") from ex

        return geo_id_to_version_dict

    def _insert_geos(
        self,
        db: Session,
        *,
        objs_in: list[schemas.GeographyCreate],
        obj_meta: models.ObjectMeta,
        namespace: models.Namespace,
    ) -> dict[str, models.Geography]:
        return {
            geo.path: geo
            for geo in list(
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
        }

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
        self._validate_create_geos(db=db, objs_in=objs_in, namespace=namespace)

        with db.begin(nested=True):
            # Need this dict because the order of the returns from the inserts does
            # not have defined behaviour.
            path_geos_dict = self._insert_geos(
                db=db,
                objs_in=objs_in,
                obj_meta=obj_meta,
                namespace=namespace,
            )

            geo_id_to_version_dict = self._insert_geo_versions(
                db=db,
                path_geos_dict=path_geos_dict,
                objs_in=objs_in,
                geo_import=geo_import,
                valid_from=datetime.now(timezone.utc),
            )
            etag = self._update_etag(db, namespace)
        db.flush()

        return [
            (geo, geo_id_to_version_dict[geo.geo_id]) for geo in path_geos_dict.values()
        ], etag

    def _validate_patch_geos(
        self,
        db: Session,
        *,
        objs_in: list[schemas.GeographyPatch],
        namespace: models.Namespace,
    ) -> list[models.Geography]:
        existing_geos = self._get_existing_geos(
            db=db, objs_in=objs_in, namespace=namespace
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

        return existing_geos

    def _get_geoid_to_version_dict(
        self,
        db: Session,
        *,
        geo_id_list: list[int],
    ) -> dict[int, models.GeoVersion]:
        """Gets a mapping from geo_id to GeoVersion."""
        return {
            geo_id: version
            for geo_id, version in (
                db.query(models.GeoVersion.geo_id, models.GeoVersion)
                .filter(
                    models.GeoVersion.geo_id.in_(geo_id_list),
                    models.GeoVersion.valid_to.is_(None),
                )
                .all()
            )
        }

    def _get_paths_to_patch(
        self,
        db: Session,
        *,
        objs_in: list[schemas.GeographyPatch],
    ):
        hash_obj_dict = {
            (
                hashlib.md5(WKBElement(obj_in.geography, srid=4269).data).hexdigest()
                if obj_in.geography
                else None
            ): obj_in
            for obj_in in objs_in
        }

        _, missing_hashes = self._get_missing_geo_bins(db=db, hash_dict=hash_obj_dict)

        return {hash_obj_dict[hsh].path for hsh in missing_hashes}

    def patch_bulk(
        self,
        db: Session,
        *,
        objs_in: list[schemas.GeographyPatch],
        geo_import: models.GeoImport,
        namespace: models.Namespace,
    ) -> tuple[list[tuple[models.Geography, models.GeoVersion]], uuid.UUID]:
        """Updates geographies in bulk."""
        existing_geos = self._validate_patch_geos(
            db=db, objs_in=objs_in, namespace=namespace
        )
        paths_to_patch = self._get_paths_to_patch(db=db, objs_in=objs_in)
        log.debug("BEFORE GETTING GEOID TO VERSION DICT")
        log.debug(paths_to_patch)
        geo_id_to_version_dict = self._get_geoid_to_version_dict(
            db=db, geo_id_list=[geo.geo_id for geo in existing_geos]
        )

        with db.begin(nested=True):
            if paths_to_patch:
                path_update_geo_dict = {
                    geo.path: geo for geo in existing_geos if geo.path in paths_to_patch
                }
                geo_ids_to_update = [
                    geo.geo_id for geo in path_update_geo_dict.values()
                ]

                with db.begin(nested=True):
                    valid_time = datetime.now(timezone.utc)
                    db.execute(
                        update(models.GeoVersion)
                        .where(
                            models.GeoVersion.geo_id.in_(geo_ids_to_update),
                            models.GeoVersion.valid_to.is_(None),
                        )
                        .values(valid_to=valid_time)
                    )

                    geo_id_to_version_dict.update(
                        self._insert_geo_versions(
                            db=db,
                            path_geos_dict=path_update_geo_dict,
                            objs_in=objs_in,
                            geo_import=geo_import,
                            valid_from=valid_time,
                        )
                    )

            etag = self._update_etag(db, namespace)
        db.flush()

        return [
            (geo, geo_id_to_version_dict[geo.geo_id]) for geo in existing_geos
        ], etag

    def upsert_bulk(
        self,
        db: Session,
        *,
        objs_in: list[schemas.GeographyPatch],
        obj_meta: models.ObjectMeta,
        geo_import: models.GeoImport,
        namespace: models.Namespace,
    ) -> tuple[list[tuple[models.Geography, models.GeoVersion]], uuid.UUID]:
        """Updates geographies in bulk."""
        raise NotImplementedError("This method is not finished yet.")

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
