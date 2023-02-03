"""CRUD operations and transformations for column sets."""
import logging

from sqlalchemy import exc
from sqlalchemy.orm import Session

from cherrydb_meta import models, schemas
from cherrydb_meta.crud.base import CRBase, normalize_path
from cherrydb_meta.crud.column import column as crud_column
from cherrydb_meta.exceptions import CreateValueError

log = logging.getLogger()


class CRColumnSet(CRBase[models.ColumnSet, schemas.ColumnSetCreate]):
    def create(
        self,
        db: Session,
        *,
        obj_in: schemas.ColumnSetCreate,
        obj_meta: models.ObjectMeta,
        namespace: models.Namespace,
    ) -> models.ColumnSet:
        """Creates a new column set."""
        with db.begin(nested=True):
            # Create a path to the column.
            canonical_path = normalize_path(obj_in.canonical_path)
            column_set = models.ColumnSet(
                path=canonical_path,
                description=obj_in.description,
                namespace_id=namespace.namespace_id,
                obj_meta=obj_meta.meta_id,
            )

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

            for column_path in obj_in.columns:
                column_obj = crud_column.get_global(
                    db, path=column_path, namespace=namespace
                )
                if column_obj is None:
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
                        set_id=column_set.set_id, col_id=column_obj.col_id
                    )
                )

        db.refresh(column_set)
        return column_set

    def get(
        self, db: Session, *, path: str, namespace: models.Namespace
    ) -> models.ColumnSet | None:
        """Retrieves a column set by reference path.

        Args:
            path: Path to column set (namespace excluded).
            namespace: Column set namespace.
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
