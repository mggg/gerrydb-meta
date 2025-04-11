"""CRUD operations and transformations for geographic imports."""

import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Collection
from shapely.geometry import Point, Polygon
import hashlib
import binascii

from geoalchemy2.elements import WKBElement
from sqlalchemy import and_, insert, or_, update, select, func
from sqlalchemy.orm import Session

from gerrydb_meta import models, schemas
from gerrydb_meta.crud.base import NamespacedCRBase, normalize_path
from gerrydb_meta.exceptions import BulkCreateError, BulkPatchError
from uvicorn.config import logger as log


class CRGeography(NamespacedCRBase[models.Geography, None]):
    def __get_existing_geos(
        self,
        db: Session,
        obj_paths: list[str],
        namespace: models.Namespace,
    ) -> list[models.Geography]:
        return (
            db.query(models.Geography)
            .filter(
                models.Geography.path.in_(
                    normalize_path(path, case_sensitive_uid=True) for path in obj_paths
                ),
                models.Geography.namespace_id == namespace.namespace_id,
            )
            .all()
        )

    def __get_existing_paths(
        self,
        db: Session,
        obj_paths: list[schemas.GeographyBase],
        namespace: models.Namespace,
    ) -> list[models.Geography]:
        return (
            db.query(models.Geography.path)
            .filter(
                models.Geography.path.in_(
                    normalize_path(path, case_sensitive_uid=True) for path in obj_paths
                ),
                models.Geography.namespace_id == namespace.namespace_id,
            )
            .all()
        )

    def __validate_create_geos(
        self,
        db: Session,
        obj_paths: list[str],
        namespace: models.Namespace,
    ) -> None:
        existing_geos = self.__get_existing_geos(
            db=db, obj_paths=obj_paths, namespace=namespace
        )

        if existing_geos:
            raise BulkCreateError(
                "Cannot create geographies that already exist.",
                paths=[geo.path for geo in existing_geos],
            )
        # Need to check for unique paths since otherwise the db will just
        # insert the first occurrence which could be confusing. (This error
        # should almost never be raised in practice.)
        paths = [normalize_path(path, case_sensitive_uid=True) for path in obj_paths]

        if len(paths) != len(set(paths)):
            raise BulkCreateError(
                "Cannot create geographies with duplicate paths.",
                paths=[path for path in paths if paths.count(path) > 1],
            )

        return

    def __get_missing_geo_bins(
        self, db: Session, hash_dict: dict[str, schemas.GeographyBase]
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

    def __insert_missing_geo_hashes(
        self,
        *,
        db: Session,
        hash_dict: dict[str, list[schemas.GeographyBase]],
        existing_hsh_to_bin_dict: dict[str, int],
        missing_hashes: set[str],
    ) -> dict[str, int]:
        try:
            values_list = []
            for h in missing_hashes:
                # Everything with the same hash has the same geography.
                # This is only an issue when there are empty geographies
                # Which are set to empty polygons.
                obj_in = hash_dict[h][0]
                empty_point_wkb = Point().wkb
                empty_polygon_wkb = Polygon().wkb

                values_list.append(
                    {
                        "geography": (
                            WKBElement(empty_polygon_wkb, srid=4269)
                            if obj_in.geography is None
                            else WKBElement(obj_in.geography, srid=4269)
                        ),
                        "internal_point": (
                            WKBElement(empty_point_wkb, srid=4269)
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

    def __update_geo_hashes(
        self,
        db: Session,
        objs_in: list[schemas.GeographyBase],
    ) -> tuple[dict[str, int], dict[str, str]]:
        empty_polygon_wkb = Polygon().wkb
        hash_obj_dict = {}

        for obj_in in objs_in:
            new_hash = (
                hashlib.md5(WKBElement(obj_in.geography, srid=4269).data).hexdigest()
                if obj_in.geography
                else hashlib.md5(
                    WKBElement(empty_polygon_wkb, srid=4269).data
                ).hexdigest()
            )
            if new_hash not in hash_obj_dict:
                hash_obj_dict[new_hash] = [obj_in]
            else:
                hash_obj_dict[new_hash].append(obj_in)

        hash_bin_dict, missing_hashes = self.__get_missing_geo_bins(
            db=db, hash_dict=hash_obj_dict
        )
        if missing_hashes:
            hash_bin_dict = self.__insert_missing_geo_hashes(
                db=db,
                hash_dict=hash_obj_dict,
                existing_hsh_to_bin_dict=hash_bin_dict,
                missing_hashes=missing_hashes,
            )
        path_hash_dict = {
            o.path: hsh for hsh, objs_lst in hash_obj_dict.items() for o in objs_lst
        }

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

    def __insert_geo_versions(
        self,
        db: Session,
        *,
        hash_bin_dict: dict[str, models.GeoBin],
        path_geos_dict: dict[str, models.Geography],
        path_hash_dict: dict[str, str],
        geo_import: models.GeoImport,
        valid_from: datetime,
    ):
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

    def __insert_geos(
        self,
        db: Session,
        *,
        insert_paths: list[str],
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
                            "path": normalize_path(path, case_sensitive_uid=True),
                            "meta_id": obj_meta.meta_id,
                            "namespace_id": namespace.namespace_id,
                        }
                        for path in insert_paths
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
        self.__validate_create_geos(
            db=db, obj_paths=[obj.path for obj in objs_in], namespace=namespace
        )

        valid_from = datetime.now(timezone.utc)

        with db.begin(nested=True):
            # Need this dict because the order of the returns from the inserts does
            # not have defined behaviour.
            path_geos_dict = self.__insert_geos(
                db=db,
                insert_paths=[o.path for o in objs_in],
                obj_meta=obj_meta,
                namespace=namespace,
            )

            hash_bin_dict, path_hash_dict = self.__update_geo_hashes(
                db=db, objs_in=objs_in
            )

            geo_id_to_version_dict = self.__insert_geo_versions(
                db=db,
                hash_bin_dict=hash_bin_dict,
                path_geos_dict=path_geos_dict,
                path_hash_dict=path_hash_dict,
                geo_import=geo_import,
                valid_from=valid_from,
            )
            etag = self._update_etag(db, namespace)
        db.flush()

        return [
            (geo, geo_id_to_version_dict[geo.geo_id]) for geo in path_geos_dict.values()
        ], etag

    def __validate_patch_geos(
        self,
        db: Session,
        *,
        obj_paths: list[str],
        namespace: models.Namespace,
    ) -> list[models.Geography]:
        existing_geos = self.__get_existing_geos(
            db=db, obj_paths=obj_paths, namespace=namespace
        )

        # This is technically caught by the next error, but this is more
        # informative.
        paths = [normalize_path(path, case_sensitive_uid=True) for path in obj_paths]

        if len(paths) != len(set(paths)):
            raise BulkPatchError(
                "Cannot patch geographies with duplicate paths.",
                paths=[path for path in paths if paths.count(path) > 1],
            )

        if len(existing_geos) < len(obj_paths):
            missing = set(
                normalize_path(path, case_sensitive_uid=True) for path in obj_paths
            ) - set(geo.path for geo in existing_geos)
            raise BulkPatchError(
                "Cannot update geographies that do not exist.", paths=list(missing)
            )

        return existing_geos

    def __get_geoid_to_version_dict(
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

    def __get_path_hashes_to_patch(
        self,
        db: Session,
        *,
        objs_in: list[schemas.GeographyPatch],
        namespace: models.Namespace,
        allow_empty_polys: bool,
    ):
        empty_polygon_wkb = Polygon().wkb
        empty_hash = hashlib.md5(
            WKBElement(empty_polygon_wkb, srid=4269).data
        ).hexdigest()

        new_path_hash_set = set({})

        for obj_in in objs_in:
            new_hash = (
                hashlib.md5(WKBElement(obj_in.geography, srid=4269).data).hexdigest()
                if obj_in.geography
                else empty_hash
            )
            new_path_hash_set.add((obj_in.path, new_hash))

        old_path_hash_set = set(
            (pair[0], pair[1].hex())
            for pair in (
                db.query(models.Geography.path, models.GeoBin.geometry_hash)
                .join(
                    models.GeoVersion,
                    models.Geography.geo_id == models.GeoVersion.geo_id,
                )
                .join(
                    models.GeoBin,
                    models.GeoVersion.geo_bin_id == models.GeoBin.geo_bin_id,
                )
                .filter(
                    models.Geography.namespace_id == namespace.namespace_id,
                    models.GeoVersion.valid_to.is_(None),
                    models.Geography.path.in_(
                        normalize_path(obj.path, case_sensitive_uid=True)
                        for obj in objs_in
                    ),
                )
                .all()
            )
        )

        assert len(old_path_hash_set) == len(new_path_hash_set)

        diff_set = new_path_hash_set - old_path_hash_set
        if any([pair[1] == empty_hash for pair in diff_set]) and not allow_empty_polys:
            raise BulkPatchError(
                "When updating geographies, found that some new geographies are empty polygons "
                "when a previous version of the same geography in the target namespace was not "
                "empty. To allow for this, set the `allow_empty_polys` parameter to "
                "`True`."
            )

        path_set = [pair[0] for pair in diff_set]
        assert len(path_set) == len(diff_set)

        return dict(diff_set)

    def patch_bulk(
        self,
        db: Session,
        *,
        objs_in: list[schemas.GeographyPatch],
        geo_import: models.GeoImport,
        namespace: models.Namespace,
        allow_empty_polys: bool = False,
    ) -> tuple[list[tuple[models.Geography, models.GeoVersion]], uuid.UUID]:
        """Updates geographies in bulk."""
        existing_geos = self.__validate_patch_geos(
            db=db, obj_paths=[obj.path for obj in objs_in], namespace=namespace
        )
        path_hash_dict = self.__get_path_hashes_to_patch(
            db=db,
            objs_in=objs_in,
            namespace=namespace,
            allow_empty_polys=allow_empty_polys,
        )
        log.debug("BEFORE GETTING GEOID TO VERSION DICT")
        # This tells me all of the versions in my target namespace
        geo_id_to_version_dict = self.__get_geoid_to_version_dict(
            db=db, geo_id_list=[geo.geo_id for geo in existing_geos]
        )

        with db.begin(nested=True):
            if len(path_hash_dict) > 0:
                path_geos_dict = {
                    geo.path: geo for geo in existing_geos if geo.path in path_hash_dict
                }

                with db.begin(nested=True):
                    valid_time = datetime.now(timezone.utc)
                    db.execute(
                        update(models.GeoVersion)
                        .where(
                            models.GeoVersion.geo_id.in_(
                                [geo.geo_id for geo in path_geos_dict.values()]
                            ),
                            models.GeoVersion.valid_to.is_(None),
                        )
                        .values(valid_to=valid_time)
                    )

                    hash_bin_dict, _path_hash_dict = self.__update_geo_hashes(
                        db=db,
                        objs_in=[obj for obj in objs_in if obj.path in path_hash_dict],
                    )

                    assert path_hash_dict == _path_hash_dict

                    geo_id_to_version_dict.update(
                        self.__insert_geo_versions(
                            db=db,
                            hash_bin_dict=hash_bin_dict,
                            path_geos_dict=path_geos_dict,
                            path_hash_dict=path_hash_dict,
                            geo_import=geo_import,
                            valid_from=valid_time,
                        )
                    )

            etag = self._update_etag(db, namespace)
        db.flush()

        return [
            (geo, geo_id_to_version_dict[geo.geo_id]) for geo in existing_geos
        ], etag

    def __validate_upsert_geos(
        self,
        db: Session,
        objs_in: list[schemas.GeographyUpsert],
        namespace: models.Namespace,
    ) -> None:
        existing_geos_paths = set(
            self.__get_existing_paths(
                db=db, obj_paths=[obj.path for obj in objs_in], namespace=namespace
            )
        )
        # Need to check for unique paths since otherwise the db will just
        # insert the first occurrence which could be confusing. (This error
        # should almost never be raised in practice.)
        paths = [
            normalize_path(obj_in.path, case_sensitive_uid=True) for obj_in in objs_in
        ]
        if len(paths) != len(set(paths)):
            raise BulkPatchError(
                "Cannot create or update geographies with duplicate paths.",
                paths=[path for path in paths if paths.count(path) > 1],
            )

        missing_paths = set(paths) - existing_geos_paths

        objs_to_create = [obj for obj in objs_in if obj.path in missing_paths]
        objs_to_update = [obj for obj in objs_in if obj.path in existing_geos_paths]

        self.__validate_create_geos(
            db=db, obj_paths=[obj.path for obj in objs_to_create], namespace=namespace
        )
        self.__validate_patch_geos(db=db, objs_in=objs_to_update, namespace=namespace)

        return

    def upsert_bulk(
        self,
        db: Session,
        *,
        objs_in: list[schemas.GeographyUpsert],
        obj_meta: models.ObjectMeta,
        geo_import: models.GeoImport,
        namespace: models.Namespace,
    ) -> tuple[list[tuple[models.Geography, models.GeoVersion]], uuid.UUID]:
        """Updates geographies in bulk."""
        _ = self.__validate_upsert_geos(
            db=db,
            objs_in=objs_in,
            namespace=namespace,
        )
        raise NotImplementedError("This method is not finished yet.")

    def fork_bulk(
        self,
        db: Session,
        *,
        source_namespace: models.Namespace,
        target_namespace: models.Namespace,
        create_geos_path_hash: list[tuple[str, str]],
        geo_import: models.GeoImport,
        obj_meta: models.ObjectMeta,
    ) -> tuple[list[tuple[models.Geography, models.GeoVersion]], models.ObjectMeta]:
        """Forks geographies from one namespace to another."""
        # Sanity check to make sure that the paths don't already exist before we start
        self.__validate_create_geos(
            db=db,
            obj_paths=list(create_geos_path_hash.keys()),
            namespace=target_namespace,
        )

        log.debug(
            f"Forking geographies from {source_namespace} to " f"{target_namespace}"
        )
        log.debug(f"Need to create geos: {create_geos_path_hash}")

        valid_from = datetime.now(timezone.utc)

        path_hash_dict = dict(create_geos_path_hash)
        with db.begin(nested=True):
            path_geos_dict = self.__insert_geos(
                db=db,
                insert_paths=list(path_hash_dict.keys()),
                obj_meta=obj_meta,
                namespace=target_namespace,
            )

            hash_bin_dict = {
                k.hex(): v
                for k, v in db.query(
                    models.GeoBin.geometry_hash, models.GeoBin.geo_bin_id
                ).filter(
                    models.GeoBin.geometry_hash.in_(
                        list(
                            map(
                                lambda x: binascii.unhexlify(x), path_hash_dict.values()
                            )
                        )
                    )
                )
            }

            geo_id_to_version_dict = self.__insert_geo_versions(
                db=db,
                hash_bin_dict=hash_bin_dict,
                path_geos_dict=path_geos_dict,
                path_hash_dict=path_hash_dict,
                geo_import=geo_import,
                valid_from=valid_from,
            )

            etag = self._update_etag(db, target_namespace)
        db.flush()

        return [
            (geo, geo_id_to_version_dict[geo.geo_id]) for geo in path_geos_dict.values()
        ], etag

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
