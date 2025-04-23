"""CRUD operations and transformations for namespace metadata."""

import logging
import uuid
from typing import Tuple

from sqlalchemy import exc
from sqlalchemy.orm import Session
from uvicorn.config import logger as log

from gerrydb_meta import models, schemas
from gerrydb_meta.crud.base import CRBase
from gerrydb_meta.exceptions import CreateValueError


class CRNamespace(CRBase[models.Namespace, schemas.NamespaceCreate]):
    def create(
        self,
        db: Session,
        *,
        obj_in: schemas.NamespaceCreate,
        obj_meta: models.ObjectMeta,
    ) -> Tuple[models.Namespace, uuid.UUID]:
        canonical_path = obj_in.path.lower()
        namespace = models.Namespace(
            path=canonical_path,
            description=obj_in.description,
            public=obj_in.public,
            meta_id=obj_meta.meta_id,
        )
        db.add(namespace)

        try:
            db.flush()
        except exc.SQLAlchemyError:
            log.exception("Failed to create namespace '%s'.", canonical_path)
            raise CreateValueError(
                f"Failed to create namespace '{canonical_path}'. "
                "(The namespace may already exist.)"
            )

        etag = self._update_etag(db)
        db.flush()

        return namespace, etag

    def get(self, db: Session, path: str) -> models.Namespace:
        return db.query(self.model).filter(self.model.path == path.lower()).first()


namespace = CRNamespace(models.Namespace)
