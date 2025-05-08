"""CRUD operations and transformations for namespace metadata."""

import logging
import uuid
from typing import Tuple

from sqlalchemy import exc
from sqlalchemy.orm import Session
from uvicorn.config import logger as log

import gerrydb_meta.admin as admin_module
from gerrydb_meta import models, schemas
from gerrydb_meta.crud.base import CRBase
from gerrydb_meta.exceptions import CreateValueError
from gerrydb_meta.scopes import ScopeManager
from gerrydb_meta.enums import ScopeType


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

        # Now grant the appropriate scopes to the user.
        user = (
            db.query(models.User)
            .filter(models.User.user_id == obj_meta.created_by)
            .one()
        )

        if not ScopeManager(user=user).can_read_in_namespace(namespace):
            admin_module.grant_scope(
                db=db,
                user=user,
                scopes=[
                    ScopeType.NAMESPACE_READ,
                    ScopeType.NAMESPACE_WRITE,
                    ScopeType.NAMESPACE_WRITE_DERIVED,
                ],
                namespace_id=namespace.namespace_id,
                meta=obj_meta,
            )
        return namespace, etag

    def get(self, db: Session, path: str) -> models.Namespace:
        return db.query(self.model).filter(self.model.path == path.lower()).first()


namespace = CRNamespace(models.Namespace)
