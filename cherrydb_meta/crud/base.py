"""Generic CR(UD) operations."""
# Based on the `full-stack-fastapi-postgresql` template:
#   https://github.com/tiangolo/full-stack-fastapi-postgresql/
#   blob/490c554e23343eec0736b06e59b2108fdd057fdc/
#   %7B%7Bcookiecutter.project_slug%7D%7D/backend/app/app/crud/base.py

from typing import Any, Generic, List, Optional, Type, TypeVar

from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from sqlalchemy.orm import Session

from cherrydb_meta.models import Base, Namespace, ObjectMeta

ModelType = TypeVar("ModelType", bound=Base)
GetSchemaType = TypeVar("GetSchemaType", bound=BaseModel)
CreateSchemaType = TypeVar("CreateSchemaType", bound=BaseModel)


def normalize_path(path: str) -> str:
    """Normalizes a path (removes leading, trailing, and duplicate slashes)."""
    return "/".join(seg for seg in path.lower().split("/") if seg)


class CRBase(Generic[ModelType, CreateSchemaType]):
    model: Type[ModelType]

    def __init__(self, model: Type[ModelType]):
        """
        CR object with default methods to create and read.

        Args:
            model: A SQLAlchemy model class.
        """
        self.model = model

    def get(self, db: Session, id: Any) -> Optional[ModelType]:
        return db.query(self.model).filter(self.model.id == id).first()

    def all(self, db: Session) -> List[ModelType]:
        return db.query(self.model).all()

    def create(self, db: Session, *, obj_in: CreateSchemaType) -> ModelType:
        obj_in_data = jsonable_encoder(obj_in)
        db_obj = self.model(**obj_in_data)  # type: ignore
        db.add(db_obj)
        db.flush()
        db.refresh(db_obj)
        return db_obj


class NamespacedCRBase(Generic[ModelType, CreateSchemaType]):
    model: Type[ModelType]

    def __init__(self, model: Type[ModelType]):
        """
        Namespaced CR object with default methods to create and read.

        Args:
            model: A SQLAlchemy model class.
        """
        self.model = model

    def get(self, db: Session, namespace: Namespace, path: Any) -> Optional[ModelType]:
        return (
            db.query(self.model)
            .filter(
                self.model.path == path,
                self.model.namespace_id == namespace.namespace_id,
            )
            .first()
        )

    def all_in_namespace(self, db: Session, *, namespace: Namespace) -> List[ModelType]:
        return (
            db.query(self.model)
            .filter(self.model.namespace_id == namespace.namespace_id)
            .all()
        )

    def create(
        self,
        db: Session,
        *,
        obj_in: CreateSchemaType,
        namespace: Namespace,
        obj_meta: ObjectMeta
    ) -> ModelType:
        obj_in_data = jsonable_encoder(obj_in)
        db_obj = self.model(
            namespace_id=namespace.namespace_id, meta_id=obj_meta.meta_id, **obj_in_data
        )  # type: ignore
        db.add(db_obj)
        db.flush()
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

    def all(self, db: Session) -> List[ModelType]:
        return db.query(self.model).all()
