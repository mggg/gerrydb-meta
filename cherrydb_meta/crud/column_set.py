"""CRUD operations and transformations for column sets."""
import logging
import uuid
from typing import Tuple

from sqlalchemy import exc
from sqlalchemy.orm import Session

from cherrydb_meta import models, schemas
from cherrydb_meta.crud.base import NamespacedCRBase, normalize_path
from cherrydb_meta.crud.column import column as crud_column
from cherrydb_meta.exceptions import CreateValueError

log = logging.getLogger()


class CRColumnSet(NamespacedCRBase[models.ColumnSet, schemas.ColumnSetCreate]):
    def create(
        self,
        db: Session,
        *,
        obj_in: schemas.ColumnSetCreate,
        obj_meta: models.ObjectMeta,
        namespace: models.Namespace,
    ) -> Tuple[models.ColumnSet, uuid.UUID]:
        """Creates a new column set."""
        with db.begin(nested=True):
            canonical_path = normalize_path(obj_in.path)
            column_set = models.ColumnSet(
                path=canonical_path,
                description=obj_in.description,
                namespace_id=namespace.namespace_id,
                meta_id=obj_meta.meta_id,
            )
            db.add(column_set)

            try:
                db.flush()
            except exc.SQLAlchemyError:
                log.exception(
                    "Failed to create column set '%s'.",
                    canonical_path,
                )
                raise CreateValueError(
                    f"Failed to create column set '{canonical_path}'."
                    "(The path may already exist in the namespace.)"
                )
            db.refresh(column_set)

            for idx, column_path in enumerate(obj_in.columns):
                ref_obj = crud_column.get_global_ref(
                    db, path=column_path, namespace=namespace
                )
                if ref_obj is None:
                    err_suffix = (
                        "" if namespace.public else " other than the current namespace"
                    )
                    raise CreateValueError(
                        f"Failed to resolve column '{column_path}'. "
                        "The column may not exist, or it may be in a "
                        f"private namespace{err_suffix}."
                    )
                db.add(
                    models.ColumnSetMember(
                        set_id=column_set.set_id,
                        ref_id=ref_obj.ref_id,
                        order=idx,
                    )
                )

                etag = self._update_etag(db, namespace)

        db.refresh(column_set)
        return column_set, etag

    def get(
        self, db: Session, *, path: str, namespace: models.Namespace
    ) -> models.ColumnSet | None:
        """Retrieves a column set by reference path.

        Args:
            path: Path to column set (namespace excluded).
            namespace: Column set's namespace.
        """
        return (
            db.query(models.ColumnSet)
            .filter(
                models.ColumnSet.namespace_id == namespace.namespace_id,
                models.ColumnSet.path == normalize_path(path),
            )
            .first()
        )


column_set = CRColumnSet(models.ColumnSet)
