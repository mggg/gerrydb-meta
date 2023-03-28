"""User-facing schemas for CherryDB objects."""
from datetime import datetime
from typing import Any

from pydantic import AnyUrl, BaseModel, constr

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
    def from_orm(cls, obj: models.DataColumn | models.ColumnRef):
        root_obj = obj.column if isinstance(obj, models.ColumnRef) else obj
        canonical_path = root_obj.canonical_ref.path
        return cls(
            canonical_path=canonical_path,
            namespace=root_obj.namespace.path,
            description=root_obj.description,
            meta=root_obj.meta,
            aliases=[ref.path for ref in root_obj.refs if ref.path != canonical_path],
            kind=root_obj.kind,
            type=root_obj.type,
        )


class ColumnValue(BaseModel):
    """Value of a column for a geography."""

    path: str  # of geography
    value: Any


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


class GeoImport(BaseModel):
    """Geographic unit import metadata returned by the database."""

    uuid: str
    namespace: str
    created_at: datetime
    created_by: str
    meta: ObjectMeta

    class Config:
        orm_mode = True

    @classmethod
    def from_orm(cls, obj: models.GeoImport):
        return cls(
            uuid=str(obj.uuid),
            namespace=obj.namespace.path,
            created_at=obj.created_at,
            created_by=obj.user.email,
            meta=obj.meta,
        )


class GeographyBase(BaseModel):
    """Base model for a geographic unit."""

    path: CherryPath
    geography: bytes | None
    internal_point: bytes | None


class GeographyCreate(GeographyBase):
    """Geographic unit data received on creation (geography as raw WKB bytes)."""


class GeographyPatch(BaseModel):
    """Geographic unit data received on PATCH."""

    geography: bytes | None


class Geography(GeographyBase):
    """Geographic unit data returned by the database."""

    meta: ObjectMeta
    namespace: str
    valid_from: datetime

    class Config:
        orm_mode = True

    @classmethod
    def from_orm(cls, obj: models.GeoVersion):
        return cls(
            namespace=obj.parent.namespace.path,
            geography=None if obj.geography is None else bytes(obj.geography.data),
            internal_point=(
                None if obj.internal_point is None else bytes(obj.internal_point.data)
            ),
            path=obj.parent.path,
            meta=obj.parent.meta,
            valid_from=obj.valid_from,
        )


class GeoSetCreate(BaseModel):
    """Paths to geographies in a `GeoSet`."""

    paths: list[str]


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
    refs: list[str]

    class Config:
        orm_mode = True

    @classmethod
    def from_orm(cls, obj: models.ColumnSet):
        ordered_cols = sorted(obj.columns, key=lambda v: v.order)
        return cls(
            path=obj.path,
            description=obj.description,
            namespace=obj.namespace.path,
            columns=[col.ref.column for col in ordered_cols],
            refs=[col.ref.path for col in ordered_cols],
            meta=obj.meta,
        )


class ViewTemplateBase(BaseModel):
    """Base model for a view template."""

    path: CherryPath
    description: str


class ViewTemplateCreate(ViewTemplateBase):
    """View template data received on creation."""

    members: list[str]


class ViewTemplatePatch(ViewTemplateBase):
    """View template data received on update."""

    members: list[str]


class ViewTemplate(ViewTemplateBase):
    """View template returned by the database."""

    namespace: str
    members: list[Column | ColumnSet]
    valid_from: datetime
    meta: ObjectMeta

    @classmethod
    def from_orm(cls, obj: models.ViewTemplateVersion):
        members = sorted(obj.columns + obj.column_sets, key=lambda obj: obj.order)
        return cls(
            path=obj.parent.path,
            namespace=obj.parent.namespace.path,
            description=obj.parent.description,
            members=[
                Column.from_orm(member.member.column)
                if isinstance(member.member, models.ColumnRef)
                else ColumnSet.from_orm(member.member)
                for member in members
            ],
            valid_from=obj.valid_from,
            meta=obj.meta,
        )


class ViewBase(BaseModel):
    """Base model for a view."""

    path: CherryPath


class ViewCreate(ViewBase):
    """View definition received on creation."""

    template: NamespacedCherryPath
    locality: NamespacedCherryPath
    layer: NamespacedCherryPath

    valid_at: datetime | None = None
    proj: str | None = None


class View(ViewBase):
    """Rendered view."""

    namespace: str
    template: ViewTemplate
    locality: Locality
    layer: GeoLayer
    meta: ObjectMeta
    valid_at: datetime
    proj: str | None
    geographies: list[Geography]
    values: dict[str, list]  # keys are columns, values are in order of `geographies`


class GraphBase(BaseModel):
    """Base model for a dual graph."""

    path: CherryPath


class GraphCreate(GraphBase):
    """Dual graph definition received on creation."""

    locality: NamespacedCherryPath
    layer: NamespacedCherryPath
    edges: list[tuple[NamespacedCherryPath, NamespacedCherryPath]]


# TODO: rendered dual graph


class PlanBase(BaseModel):
    """Base model for a districting plan."""

    path: CherryPath
    description: str
    source_url: AnyUrl | None = None
    districtr_id: str | None = None
    daves_id: str | None = None


class PlanCreate(PlanBase):
    """Districting plan definition received on creation."""

    locality: NamespacedCherryPath
    layer: NamespacedCherryPath
    assignments: dict[NamespacedCherryPath, str]


class Plan(PlanBase):
    """Rendered districting plan."""

    namespace: str
    locality: Locality
    layer: GeoLayer
    meta: ObjectMeta
    created_at: datetime
    num_districts: int
    complete: bool
    assignments: dict[NamespacedCherryPath, str | None]

    @classmethod
    def from_orm(cls, obj: models.Plan):
        # TODO: there's probably a performance bottleneck around the resolution
        # of geography names for assignments with a lot of geographies.
        base_geos = {member.geo.full_path: None for member in obj.set_version.members}
        assignments = {
            assignment.geo.full_path: assignment.assignment
            for assignment in obj.assignments
        }
        return cls(
            path=obj.path,
            namespace=obj.namespace.path,
            description=obj.description,
            source_url=obj.source_url,
            districtr_id=obj.districtr_id,
            daves_id=obj.daves_id,
            locality=obj.set_version.loc,
            layer=obj.set_version.layer,
            meta=obj.meta,
            created_at=obj.created_at,
            num_districts=obj.num_districts,
            complete=obj.complete,
            assignments={**base_geos, **assignments},
        )
