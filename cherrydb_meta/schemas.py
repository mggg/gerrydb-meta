"""User-facing schemas for CherryDB objects."""
from datetime import datetime

from pydantic import BaseModel, constr

from cherrydb_meta import models

UserEmail = constr(max_length=254)

LocationRef = constr(regex=r"[a-z0-9][a-z0-9-_/]*")


class ObjectMetaBase(BaseModel):
    """Base model for object metadata."""

    notes: str


class ObjectMetaCreate(ObjectMetaBase):
    """Object metadata received on creation."""


class ObjectMeta(ObjectMetaBase):
    """Object metadata returned by the database."""

    meta_id: int
    created_at: datetime
    created_by: UserEmail

    class Config:
        orm_mode = True

    @classmethod
    def from_orm(cls, obj: models.ObjectMeta):
        return cls(
            meta_id=obj.meta_id,
            notes=obj.notes,
            created_at=obj.created_at,
            created_by=obj.user.email,
        )


class LocationBase(BaseModel):
    """Base model for location metadata."""

    canonical_path: LocationRef
    parent_path: LocationRef | None
    name: str


class LocationCreate(LocationBase):
    """Location metadata received on creation."""

    aliases: list[LocationRef] | None


class LocationPatch(BaseModel):
    """Location metadata received on PATCH."""

    aliases: list[LocationRef]


class Location(LocationBase):
    """A location returned by the database."""

    aliases: list[LocationRef]
    meta: ObjectMeta

    class Config:
        orm_mode = True

    @classmethod
    def from_orm(cls, obj: models.Location):
        canonical_path = obj.canonical_ref.path
        return cls(
            canonical_path=canonical_path,
            parent_path=obj.parent.canonical_ref.path if obj.parent else None,
            name=obj.name,
            meta=obj.meta,
            aliases=[ref.path for ref in obj.refs if ref.path != canonical_path],
        )
