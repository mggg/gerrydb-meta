"""User-facing schemas for GerryDB objects."""

from datetime import datetime
from typing import Any
from typing import Annotated
from pydantic import AnyUrl, BaseModel, constr, validator

from gerrydb_meta import enums, models

UserEmail = Annotated[str, constr(max_length=254)]

GerryPath = Annotated[str, constr(regex=r"[a-z0-9][a-z0-9-_/]*")]
NamespacedGerryPath = Annotated[str, constr(regex=r"[a-z0-9/][a-z0-9-_/]*")]


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

    canonical_path: GerryPath
    parent_path: GerryPath | None
    default_proj: str | None
    name: str


class LocalityCreate(LocalityBase):
    """Locality metadata received on creation."""

    aliases: list[GerryPath] | None


class LocalityPatch(BaseModel):
    """Locality metadata received on PATCH."""

    aliases: list[GerryPath]


class Locality(LocalityBase):
    """A locality returned by the database."""

    aliases: list[GerryPath]
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
            default_proj=obj.default_proj,
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

    canonical_path: GerryPath
    description: str | None
    source_url: AnyUrl | None
    kind: enums.ColumnKind
    type: enums.ColumnType


class ColumnCreate(ColumnBase):
    """Column metadata received on creation."""

    aliases: list[GerryPath] | None


class ColumnPatch(BaseModel):
    """Column metadata received on PATCH."""

    aliases: list[GerryPath]


class Column(ColumnBase):
    """A locality returned by the database."""

    namespace: str
    aliases: list[GerryPath]
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
            source_url=root_obj.source_url,
        )


class ColumnValue(BaseModel):
    """Value of a column for a geography."""

    path: str  # of geography
    value: Any


class GeoLayerBase(BaseModel):
    """Base model for geographic layer metadata."""

    path: GerryPath
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

    path: GerryPath
    geography: bytes | None
    internal_point: bytes | None

    @validator("geography", "internal_point", pre=True, each_item=False)
    def check_bytes_type(cls, v, field):
        if v is not None and not isinstance(v, bytes):
            raise ValueError(
                f"The {field.name} must be of type bytes, got type {type(v).__name__}"
            )
        return v


class GeographyCreate(GeographyBase):
    """Geographic unit data received on creation (geography as raw WKB bytes)."""


class GeographyPatch(GeographyBase):
    """Geographic unit data received on PATCH."""


class GeographyUpsert(GeographyBase):
    """Geographic unit data received on UPSERT (PUT)."""


class Geography(GeographyBase):
    """Geographic unit data returned by the database."""

    meta: ObjectMeta
    namespace: str
    valid_from: datetime

    class Config:
        orm_mode = True

    @classmethod
    def from_orm(cls, obj: models.GeoVersion):  # pragma: no cover
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


class GeographyMeta(BaseModel):
    """Geographic unit metadata returned by the database."""

    namespace: str
    path: str
    meta: ObjectMeta

    class Config:
        orm_mode = True

    @classmethod
    def from_orm(cls, obj: models.Geography):
        return cls(namespace=obj.namespace.path, path=obj.path, meta=obj.meta)


class GeoSetCreate(BaseModel):
    """Paths to geographies in a `GeoSet`."""

    paths: list[str]


class ColumnSetBase(BaseModel):
    """Base model for a logical column grouping."""

    path: GerryPath
    description: str


class ColumnSetCreate(ColumnSetBase):
    """Column grouping data received on creation.
    The columns must first exist in the before a column set
    can be created with them.
    """

    columns: list[GerryPath]


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

    path: GerryPath
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
    members: list
    valid_from: datetime
    meta: ObjectMeta

    @classmethod
    def from_orm(cls, obj: models.ViewTemplateVersion):
        members = sorted(obj.columns + obj.column_sets, key=lambda obj: obj.order)

        new_members = []

        for member in members:
            if isinstance(member.member, models.ColumnRef):
                new_members.append(Column.from_orm(member.member.column))
            else:
                new_members.append(ColumnSet.from_orm(member.member))

        return cls(
            path=obj.parent.path,
            namespace=obj.parent.namespace.path,
            description=obj.parent.description,
            members=new_members,
            valid_from=obj.valid_from,
            meta=obj.meta,
        )


class GraphBase(BaseModel):
    """Base model for a dual graph."""

    path: GerryPath
    description: str
    proj: str | None = None


WeightedEdge = tuple[NamespacedGerryPath, NamespacedGerryPath, dict | None]


class GraphCreate(GraphBase):
    """Dual graph definition received on creation."""

    locality: NamespacedGerryPath
    layer: NamespacedGerryPath
    edges: list[WeightedEdge]


class GraphMeta(GraphBase):
    """Dual graph metadata."""

    namespace: str
    locality: Locality
    layer: GeoLayer
    meta: ObjectMeta
    created_at: datetime

    @classmethod
    def from_orm(cls, obj: models.Graph):
        return cls(
            path=obj.path,
            namespace=obj.namespace.path,
            description=obj.description,
            locality=obj.set_version.loc,
            layer=obj.set_version.layer,
            meta=obj.meta,
            created_at=obj.created_at,
        )


class Graph(GraphMeta):
    """Rendered dual graph without node attributes."""

    edges: list[WeightedEdge]

    @classmethod
    def from_orm(cls, obj: models.Graph):
        return cls(
            path=obj.path,
            namespace=obj.namespace.path,
            description=obj.description,
            locality=obj.set_version.loc,
            layer=obj.set_version.layer,
            meta=obj.meta,
            created_at=obj.created_at,
            edges=[
                (edge.geo_1.full_path, edge.geo_2.full_path, edge.weights)
                for edge in obj.edges
            ],
        )


class PlanBase(BaseModel):
    """Base model for a districting plan."""

    path: GerryPath
    description: str
    source_url: AnyUrl | None = None
    districtr_id: str | None = None
    daves_id: str | None = None


class PlanCreate(PlanBase):
    """Districting plan definition received on creation."""

    locality: NamespacedGerryPath
    layer: NamespacedGerryPath
    assignments: dict[NamespacedGerryPath, str]


class PlanMeta(PlanBase):
    """Rendered districting plan (metadata only)."""

    namespace: str
    locality: Locality
    layer: GeoLayer
    meta: ObjectMeta
    created_at: datetime
    num_districts: int
    complete: bool

    @classmethod
    def from_orm(cls, obj: models.Plan):  # pragma: no cover
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
        )


class Plan(PlanMeta):
    """Rendered districting plan."""

    assignments: dict[NamespacedGerryPath, str | None]

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


class ViewBase(BaseModel):
    """Base model for a view."""

    path: GerryPath


class ViewCreate(ViewBase):
    """View definition received on creation."""

    template: NamespacedGerryPath
    locality: NamespacedGerryPath
    layer: NamespacedGerryPath
    graph: NamespacedGerryPath | None = None

    valid_at: datetime | None = None
    proj: str | None = None


class ViewMeta(ViewBase):
    """View metadata."""

    namespace: str
    template: ViewTemplate
    locality: Locality
    layer: GeoLayer
    meta: ObjectMeta
    valid_at: datetime
    proj: str | None
    graph: GraphMeta | None
    # TODO: add plans and geography paths?

    @classmethod
    def from_orm(cls, obj: models.View):
        return cls(
            path=obj.path,
            namespace=obj.namespace.path,
            template=ViewTemplate.from_orm(obj.template_version),
            locality=obj.loc,
            layer=obj.layer,
            meta=obj.meta,
            valid_at=obj.at,
            proj=obj.proj,
            graph=None if obj.graph is None else GraphMeta.from_orm(obj.graph),
        )
