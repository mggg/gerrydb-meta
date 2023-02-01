"""CRUD operations and transformations for generic object metadata."""
from uuid import UUID
from sqlalchemy.orm import Session

from cherrydb_meta import models, schemas
from cherrydb_meta.crud.base import CRBase


class CRObjectMeta(CRBase[models.ObjectMeta, schemas.ObjectMetaCreate]):
    def create(
        self, db: Session, *, obj_in: schemas.ObjectMetaCreate, user: models.User
    ) -> models.ObjectMeta:
        obj_meta = models.ObjectMeta(
            notes=obj_in.notes,
            created_by=user.user_id,
        )
        db.add(obj_meta)
        db.flush()
        return obj_meta

    def get(self, db: Session, id: UUID) -> models.ObjectMeta:
        return db.query(self.model).filter(self.model.uuid == id).first()


obj_meta = CRObjectMeta(models.ObjectMeta)
