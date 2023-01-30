"""CRUD operations and transformations for namespace metadata."""
from sqlalchemy.orm import Session

from cherrydb_meta import models, schemas
from cherrydb_meta.crud.base import CRBase


class CRNamespace(CRBase[models.Namespace, schemas.NamespaceCreate]):
    def create(
        self, db: Session, *, obj_in: schemas.NamespaceCreate, user: models.User
    ) -> models.Namespace:
        namespace = models.Namespace(
            notes=obj_in.notes,
            created_by=user.user_id,
        )
        db.add(namespace)
        db.flush()
        return namespace

    def get(self, db: Session, path: str) -> models.Namespace:
        return db.query(self.model).filter(self.model.path == path).first()


namespace = CRNamespace(models.Namespace)
