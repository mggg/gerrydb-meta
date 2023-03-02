"""CRUD operations and transformations for view templates."""
import logging
import uuid
from datetime import datetime
from typing import Tuple

from sqlalchemy import exc
from sqlalchemy.orm import Session

from cherrydb_meta import models, schemas
from cherrydb_meta.crud.base import NamespacedCRBase, normalize_path
from cherrydb_meta.crud.column import column as crud_column
from cherrydb_meta.exceptions import CreateValueError

log = logging.getLogger()


class CRViewTemplate(NamespacedCRBase[models.ViewTemplate, schemas.ViewTemplateCreate]):
    def create(
        self,
        db: Session,
        *,
        obj_in: schemas.ViewTemplateCreate,
        resolved_members: list[models.DeclarativeBase],
        obj_meta: models.ObjectMeta,
        namespace: models.Namespace,
    ) -> Tuple[models.ViewTemplate, uuid.UUID]:
        """Creates a new view template."""
        with db.begin(nested=True):
            canonical_path = normalize_path(obj_in.path)
            view_template = models.ViewTemplate(
                path=canonical_path,
                description=obj_in.description,
                namespace_id=namespace.namespace_id,
                meta_id=obj_meta.meta_id,
            )
            db.add(view_template)

            try:
                db.flush()
            except exc.SQLAlchemyError:
                log.exception(
                    "Failed to create view template '%s'.",
                    canonical_path,
                )
                raise CreateValueError(
                    f"Failed to create view template '{canonical_path}'."
                    "(The path may already exist in the namespace.)"
                )
            db.refresh(view_template)

            template_version = models.ViewTemplateVersion(
                template_id=view_template.template_id,
                meta_id=obj_meta.meta_id,
                valid_from=datetime.now(),
            )
            db.add(template_version)
            db.flush()
            db.refresh(template_version)

            for idx, member in enumerate(resolved_members):
                if (
                    member.namespace_id != namespace.namespace_id
                    and not member.namespace.public
                ):
                    raise CreateValueError(
                        "Cannot create cross-namespace reference to an object in "
                        "a private namespace."
                    )

                if isinstance(member, models.ColumnRef):
                    db.add(
                        models.ViewTemplateColumnMember(
                            template_version_id=template_version.template_version_id,
                            ref_id=member.ref_id,
                            order=idx,
                        )
                    )
                else:
                    db.add(
                        models.ViewTemplateColumnSetMember(
                            template_version_id=template_version.template_version_id,
                            set_id=member.set_id,
                            order=idx,
                        )
                    )

            etag = self._update_etag(db, namespace)

        db.refresh(view_template)
        return view_template, etag

    # TODO: patch()

    def get(
        self, db: Session, *, path: str, namespace: models.Namespace
    ) -> models.ViewTemplate | None:
        """Retrieves a view template by reference path.

        Args:
            path: Path to view template (namespace excluded).
            namespace: view template's namespace.
        """
        return (
            db.query(models.ViewTemplate)
            .filter(
                models.ViewTemplate.namespace_id == namespace.namespace_id,
                models.ViewTemplate.path == normalize_path(path),
            )
            .first()
        )


view_template = CRViewTemplate(models.ViewTemplate)
