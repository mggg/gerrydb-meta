"""Generic CR(UD) operations."""

# Based on the `full-stack-fastapi-postgresql` template:
#   https://github.com/tiangolo/full-stack-fastapi-postgresql/
#   blob/490c554e23343eec0736b06e59b2108fdd057fdc/
#   %7B%7Bcookiecutter.project_slug%7D%7D/backend/app/app/crud/base.py
import uuid
from typing import Any, Generic, List, Optional, Tuple, Type, TypeVar

from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from gerrydb_meta.models import Base, ETag, Namespace, ObjectMeta
from gerrydb_meta.exceptions import GerryPathError

ModelType = TypeVar("ModelType", bound=Base)
GetSchemaType = TypeVar("GetSchemaType", bound=BaseModel)
CreateSchemaType = TypeVar("CreateSchemaType", bound=BaseModel)
PatchSchemaType = TypeVar("PatchSchemaType", bound=BaseModel)

INVALID_PATH_SUBSTRINGS = set(
    {
        "..",
        " ",
    }
)


def normalize_path(path: str, case_sensitive_uid: bool = False) -> str:
    """Normalizes a path (removes leading, trailing, and duplicate slashes, and
    lowercases the path if `case_sensitive` is `False`).

    Some paths, such as paths containing GEOIDs, are case-sensitive in the last
    segment. In these cases, `case_sensitive` should be set to `True`.
    """
    for item in INVALID_PATH_SUBSTRINGS:
        if item in path:
            raise GerryPathError(
                f"Invalid path: '{path}'. Please remove the following substring: '{item}'"
            )

    if case_sensitive_uid:
        path_list = path.strip().split("/")
        return "/".join(
            seg.lower() if i < len(path_list) - 1 else seg
            for i, seg in enumerate(path_list)
            if seg
        )

    return "/".join(seg for seg in path.strip().lower().split("/") if seg)


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

    def create(
        self, db: Session, *, obj_in: CreateSchemaType
    ) -> Tuple[ModelType, uuid.UUID]:
        obj_in_data = jsonable_encoder(obj_in)
        db_obj = self.model(**obj_in_data)  # type: ignore
        db.add(db_obj)
        etag = self._update_etag(db)
        db.flush()
        db.refresh(db_obj)
        return db_obj, etag

    def etag(self, db: Session) -> uuid.UUID | None:
        """Retrieves the latest UUID-format ETag for the collection."""
        etag = (
            db.query(ETag.etag)
            .filter(ETag.table == self.model.__tablename__, ETag.namespace_id.is_(None))
            .first()
        )
        return None if etag is None else etag[0]

    def _update_etag(self, db: Session) -> uuid.UUID:
        """Refreshes the (object, namespace) ETag."""
        new_etag = uuid.uuid4()
        stmt = (
            insert(ETag)
            .values(table=self.model.__tablename__, namespace_id=None, etag=new_etag)
            .on_conflict_do_update(
                index_elements=["table", "namespace_id"], set_={"etag": new_etag}
            )
        )
        db.execute(stmt)
        return new_etag


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
        obj_meta: ObjectMeta,
    ) -> Tuple[ModelType, uuid.UUID]:
        obj_in_data = jsonable_encoder(obj_in)
        db_obj = self.model(
            namespace_id=namespace.namespace_id, meta_id=obj_meta.meta_id, **obj_in_data
        )  # type: ignore
        db.add(db_obj)
        etag = self._update_etag(db, namespace)
        db.flush()
        db.refresh(db_obj)
        return db_obj, etag

    def etag(self, db: Session, namespace: Namespace) -> uuid.UUID | None:
        """Retrieves the latest UUID-format ETag for the collection."""
        etag = (
            db.query(ETag.etag)
            .filter(
                ETag.table == self.model.__tablename__,
                ETag.namespace_id == namespace.namespace_id,
            )
            .first()
        )
        return None if etag is None else etag[0]

    def _update_etag(self, db: Session, namespace: Namespace) -> uuid.UUID:
        """Refreshes the (object, namespace) ETag."""
        new_etag = uuid.uuid4()
        stmt = (
            insert(ETag)
            .values(
                table=self.model.__tablename__,
                namespace_id=namespace.namespace_id,
                etag=new_etag,
            )
            .on_conflict_do_update(
                index_elements=["table", "namespace_id"], set_={"etag": new_etag}
            )
        )
        db.execute(stmt)
        return new_etag


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
