"""CRUD operations and transformations for column metadata."""
import logging
from typing import Collection

from sqlalchemy import exc
from sqlalchemy.orm import Session

from cherrydb_meta import models, schemas
from cherrydb_meta.crud.base import CRBase, normalize_path
from cherrydb_meta.exceptions import CreateValueError

log = logging.getLogger()


class CRColumn(CRBase[models.DataColumn, schemas.ColumnCreate]):
    """CRUD operations and transformations for column metadata."""

    def create(
        self,
        db: Session,
        *,
        obj_in: schemas.ColumnCreate,
        obj_meta: models.ObjectMeta,
        namespace: models.Namespace,
    ) -> models.DataColumn:
        """Creates a new column with a canonical reference."""
        with db.begin(nested=True):
            # Create a path to the column.
            canonical_path = normalize_path(obj_in.canonical_path)
            canonical_ref = models.DataColumnRef(
                path=canonical_path,
                meta_id=obj_meta.meta_id,
                namespace_id=namespace.namespace_id,
            )
            db.add(canonical_ref)
            try:
                db.flush()
            except exc.SQLAlchemyError:
                # TODO: Make this more specific--the primary goal is to capture the case
                # where the reference already exists.
                log.exception(
                    "Failed to create reference '%s' to new column.",
                    obj_in.canonical_path,
                )
                raise CreateValueError(
                    f"Failed to create canonical path '{canonical_path}' to new column. "
                    "(The path may already exist.)"
                )

            # Create the column itself.
            col = models.DataColumn(
                canonical_ref_id=canonical_ref.ref_id,
                namespace_id=namespace.namespace_id,
                meta_id=obj_meta.meta_id,
                description=obj_in.description,
                source_url=obj_in.source_url,
            )
            db.add(col)
            try:
                db.flush()
            except exc.SQLAlchemyError:
                log.exception("Failed to create new column.")
                raise CreateValueError("Failed to create new column.")

            canonical_ref.col_id = col.col_id
            db.flush()

            # Create additional aliases (non-canonical references) to the column.
            if obj_in.aliases:
                self._add_aliases(
                    db=db,
                    alias_paths=obj_in.aliases,
                    col=col,
                    obj_meta=obj_meta,
                    namespace=namespace,
                )
        return col

    def get(
        self, db: Session, *, path: str, namespace: models.Namespace
    ) -> models.DataColumn | None:
        """Retrieves a column by reference path.

        Args:
            path: Path to column (namespace excluded).
            namespace: Column namespace.
        """
        normalized_path = normalize_path(path)

        ref = (
            db.query(models.DataColumnRef)
            .filter(
                (models.DataColumnRef.path == normalized_path)
                & (models.DataColumnRef.namespace_id == namespace.namespace_id)
            )
            .first()
        )
        return None if ref is None else ref.col

    def get_global(
        self, db: Session, *, path: str, namespace: models.Namespace
    ) -> models.DataColumn | None:
        """Retrieves a column by reference path, potentially within a different
        namespace than `namespace`.

        Only public namespaces can be addressed with a global path.

        Args:
            path: Path to column, either global-style (prefixed by `/`)
                or a local-style path.
            namespace: Default column namespace.
        """
        normalized_path = normalize_path(path)
        if normalized_path.startswith("/"):
            namespace_path = normalized_path[1]
            column_path = "/".join(normalized_path[2:])
            alt_namespace = (
                db.query(models.Namespace)
                .filter(
                    models.Namespace.path == namespace_path,
                    models.Namespace.public.is_(True),
                )
                .first()
            )
            return (
                None
                if alt_namespace is None
                else self.get(db, path=column_path, namespace=alt_namespace)
            )

        return self.get(db, path=path, namespace=namespace)

    def patch(
        self,
        db: Session,
        *,
        obj: models.DataColumn,
        obj_meta: models.ObjectMeta,
        patch: schemas.ColumnPatch,
    ) -> models.DataColumn | None:
        """Patches a column (adds new aliases)."""
        new_aliases = set(normalize_path(path) for path in patch.aliases) - set(
            ref.path for ref in obj.refs
        )
        if not new_aliases:
            return obj

        db.flush()
        self._add_aliases(db=db, alias_paths=new_aliases, col=obj, obj_meta=obj_meta)
        db.refresh(obj)
        return obj

    def _add_aliases(
        self,
        *,
        db: Session,
        alias_paths: Collection[str],
        col: models.DataColumn,
        obj_meta: models.ObjectMeta,
    ) -> None:
        """Adds aliases to a column."""
        for alias_path in alias_paths:
            alias_ref = models.DataColumnRef(
                path=normalize_path(alias_path),
                col_id=col.col_id,
                namespace_id=col.namespace_id,
                meta_id=obj_meta.meta_id,
            )
            db.add(alias_ref)

            try:
                db.flush()
            except exc.SQLAlchemyError:
                # TODO: Make this more specific--the primary goal is to capture the case
                # where the reference already exists.
                log.exception(
                    "Failed to create aliases for new column.",
                    col.canonical_path,
                )
                raise CreateValueError(
                    "Failed to create aliases for new column."
                    "(One or more aliases may already exist.)"
                )


column = CRColumn(models.DataColumn)
