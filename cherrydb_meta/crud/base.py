"""Generic CR(UD) operations."""
# Based on the `full-stack-fastapi-postgresql` template:
#   https://github.com/tiangolo/full-stack-fastapi-postgresql/
#   blob/490c554e23343eec0736b06e59b2108fdd057fdc/
#   %7B%7Bcookiecutter.project_slug%7D%7D/backend/app/app/crud/base.py

from typing import Any, Generic, List, Optional, Type, TypeVar

from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from sqlalchemy.orm import Session

from cherrydb_meta.models import Base


ModelType = TypeVar("ModelType", bound=Base)
CreateSchemaType = TypeVar("CreateSchemaType", bound=BaseModel)

class CRBase(Generic[ModelType, CreateSchemaType]):
    def __init__(self, model: Type[ModelType]):
        """
        CR object with default methods to Create and Read.
        **Parameters**
        * `model`: A SQLAlchemy model class
        """
        self.model = model

    def get(self, db: Session, id: Any) -> Optional[ModelType]:
        return db.query(self.model).filter(self.model.id == id).first()

    def all(
        self, db: Session
    ) -> List[ModelType]:
        return db.query(self.model).all()

    def create(self, db: Session, *, obj_in: CreateSchemaType) -> ModelType:
        obj_in_data = jsonable_encoder(obj_in)
        db_obj = self.model(**obj_in_data)  # type: ignore
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)
        return db_obj
    
    

class ReadOnlyBase(Generic[ModelType]):
    def __init__(self, model: Type[ModelType]):
        """
        Read-only object base.
        **Parameters**
        * `model`: A SQLAlchemy model class
        """
        self.model = model

    def get(self, db: Session, id: Any) -> Optional[ModelType]:
        return db.query(self.model).filter(self.model.id == id).first()

    def all(
        self, db: Session
    ) -> List[ModelType]:
        return db.query(self.model).all()