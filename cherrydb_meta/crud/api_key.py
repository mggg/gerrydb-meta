"""CRUD operations and transformations for location metadata."""
from sqlalchemy.orm import Session

from cherrydb_meta import models
from cherrydb_meta.crud.base import ReadOnlyBase


class ReadOnlyApiKey(ReadOnlyBase[models.ApiKey]):
    def get(self, db: Session, id: bytes) -> models.ApiKey | None:
        return db.query(self.model).filter(self.model.key_hash == id).first()


api_key = ReadOnlyApiKey(models.ApiKey)
