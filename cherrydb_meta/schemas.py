"""User-facing schemas for CherryDB objects."""
from datetime import datetime

from pydantic import AnyUrl, BaseModel, constr
from shapely.geometry.base import BaseGeometry

from cherrydb_meta import enums, models

UserEmail = constr(max_length=254)

CherryPath = constr(regex=r"[a-z0-9][a-z0-9-_/]*")
NamespacedCherryPath = constr(regex=r"[a-z0-9/][a-z0-9-_/]*")


class ObjectMetaBase(BaseModel):
    """Base model for object metadata."""

    notes: str | None


class ObjectMetaCreate(ObjectMetaBase):
    """Object metadata received on creation."""


class ObjectMeta(ObjectMetaBase):
    """Object metadata returned by the database."""

    uuid: str
    created_at: datetime
    created_by: UserEmail

    class Config:
        orm_mode = True

    @classmethod
    def from_orm(cls, obj: models.ObjectMeta):
        return cls(
            uuid=str(obj.uuid),
            notes=obj.notes,
            created_at=obj.created_at,
            created_by=obj.user.email,
        )


class LocalityBase(BaseModel):
    """Base model for locality metadata."""

    canonical_path: CherryPath
    parent_path: CherryPath | None
    default_proj: str | None
    name: str


class LocalityCreate(LocalityBase):
    """Locality metadata received on creation."""

    aliases: list[CherryPath] | None


class LocalityPatch(BaseModel):
    """Locality metadata received on PATCH."""

    aliases: list[CherryPath]


class Locality(LocalityBase):
    """A locality returned by the database."""

    aliases: list[CherryPath]
    meta: ObjectMeta

    class Config:
        orm_mode = True

    @classmethod
    def from_orm(cls, obj: models.Locality):
        canonical_path = obj.canonical_ref.path
        return cls(
            canonical_path=canonical_path,
            parent_path=obj.parent.canonical_ref.path if obj.parent else None,
            name=obj.name,
            meta=obj.meta,
            aliases=[ref.path for ref in obj.refs if ref.path != canonical_path],
        )


class NamespaceBase(BaseModel):
    """Base model for namespace metadata."""

    path: constr(regex=r"[a-zA-Z0-9-]+")
    description: str
    public: bool


class NamespaceCreate(NamespaceBase):
    """Namespace metadata received on creation."""


class Namespace(NamespaceBase):
    """A namespace returned by the database."""

    meta: ObjectMeta

    class Config:
        orm_mode = True


class ColumnBase(BaseModel):
    """Base model for locality metadata."""

    canonical_path: CherryPath
    namespace: str
    description: str
    source_url: AnyUrl | None
    kind: enums.ColumnKind
    type: enums.ColumnType


class ColumnCreate(ColumnBase):
    """Column metadata received on creation."""

    aliases: list[CherryPath] | None


class ColumnPatch(BaseModel):
    """Column metadata received on PATCH."""

    aliases: list[CherryPath]


class Column(ColumnBase):
    """A locality returned by the database."""

    aliases: list[CherryPath]
    meta: ObjectMeta

    class Config:
        orm_mode = True

    @classmethod
    def from_orm(cls, obj: models.DataColumn):
        canonical_path = obj.canonical_ref.path
        return cls(
            canonical_path=canonical_path,
            namespace=obj.namespace.path,
            description=obj.description,
            meta=obj.meta,
            aliases=[ref.path for ref in obj.refs if ref.path != canonical_path],
            kind=obj.kind,
            type=obj.type,
        )


class GeoLayerBase(BaseModel):
    """Base model for geographic layer metadata."""

    path: CherryPath
    description: str | None
    source_url: AnyUrl | None


class GeoLayerCreate(GeoLayerBase):
    """Geographic layer metadata received on creation."""


class GeoLayer(GeoLayerBase):
    """Geographic layer metadata returned by the database."""

    meta: ObjectMeta
    namespace: str

    class Config:
        orm_mode = True

    @classmethod
    def from_orm(cls, obj: models.GeoLayer):
        return cls(
            path=obj.path,
            namespace=obj.namespace.path,
            description=obj.description,
            source_url=obj.source_url,
            meta=obj.meta,
        )


class GeoImportBase(BaseModel):
    """Base model for a geographic unit import."""


class GeoImport(GeoImportBase):
    """Geographic unit import metadata returned by the database."""

    uuid: str
    namespace: str
    created_at: datetime
    meta: ObjectMeta

    class Config:
        orm_mode = True

    @classmethod
    def from_orm(cls, obj: models.GeoImport):
        return cls(
            uuid=obj.uuid,
            namespace=obj.namespace.path,
            created_at=obj.created_at,
            meta=obj.meta,
        )


class GeographyBase(BaseModel):
    """Base model for a geographic unit."""

    path: CherryPath
    geography: BaseGeometry

    class Config:
        arbitrary_types_allowed = True


class GeographyCreateRaw(GeographyBase):
    """Geographic unit data received on creation (geography as raw WKB bytes)."""

    path: CherryPath
    geography: bytes


class GeographyCreate(GeographyBase):
    """Geographic unit data received on creation."""


class GeographyPatch(BaseModel):
    """Geographic unit data received on PATCH."""

    class Config:
        arbitrary_types_allowed = True

    geography: BaseGeometry


class Geography(GeographyBase):
    """Geographic unit data returned by the database."""

    meta: ObjectMeta
    namespace: str
    modified_at: datetime

    class Config:
        orm_mode = True

    # TODO: serialization.


class ColumnSetBase(BaseModel):
    """Base model for a logical column grouping."""

    path: CherryPath
    description: str


class ColumnSetCreate(ColumnSetBase):
    """Column grouping data received on creation."""

    columns: list[NamespacedCherryPath]


class ColumnSet(ColumnSetBase):
    """Logical column grouping returned by the database."""

    meta: ObjectMeta
    namespace: str
    columns: list[Column]

    class Config:
        orm_mode = True

    @classmethod
    def from_orm(cls, obj: models.ColumnSet):
        return cls(
            path=obj.path,
            description=obj.description,
            namespace=obj.namespace.path,
            columns=[col.column for col in obj.columns],
            meta=obj.meta,
        )
