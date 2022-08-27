"""SQL table definitions for CherryDB."""
from enum import Enum

from geoalchemy2 import Geometry
from sqlalchemy import Boolean, CheckConstraint, Column, Date, DateTime
from sqlalchemy import Enum as SqlEnum
from sqlalchemy import ForeignKey, Integer, MetaData, String, Text, UniqueConstraint
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql import func

DEFAULT_LENGTH = 256

metadata_obj = MetaData(schema="cherrydb")
Base = declarative_base(metadata=metadata_obj)


class GeoUnit(str, Enum):
    """Mapping level of a geography."""

    BLOCK = "block"
    BG = "bg"  # block group
    TRACT = "tract"
    COUSUB = "cousub"
    COUNTY = "county"
    VTD = "vtd"
    PLACE = "place"
    WARD = "ward"
    PRECINCT = "precinct"
    OTHER_CENSUS = "other_census"
    OTHER_NON_CENSUS = "other_non_census"


class PlanLevel(str, Enum):
    STATE_HOUSE = "state_house"
    STATE_SENATE = "state_senate"
    US_HOUSE = "us_house"
    # TODO: we might add more specific levels for school districts,
    # city wards, etc. as the need arises.
    OTHER = "other"


class ElectedOffice(str, Enum):
    GOVERNOR = "governor"
    PRESIDENT = "president"
    US_HOUSE = "us_house"
    STATE_HOUSE = "state_house"
    STATE_SENATE = "state_senate"
    ATTORNEY_GENERAL = "attorney_general"
    SECRETARY_OF_STATE = "secretary_of_state"
    COMPTROLLER = "comptroller"
    OTHER = "other"


class IncumbentStatus(str, Enum):
    # Based off the `CAND_ICI` column in the FEC candidate master file.
    # see https://www.fec.gov/campaign-finance-data/candidate-master-file-description/
    CHALLENGER = "challenger"
    INCUMBENT = "incumbent"
    OPEN = "open"


class AliasKind(str, Enum):
    FIPS = "fips"
    POSTAL = "postal"
    ASSOCIATED_PRESS = "associated_press"
    CUSTOM = "custom"


class ElectionKind(str, Enum):
    PRIMARY = "primary"
    GENERAL = "general"
    SPECIAL = "special"


class LocationKind(str, Enum):
    COUNTRY = "country"
    STATE = "state"
    TERRITORY = "territory"
    MUNI = "muni"
    OTHER = "other"


class GeoAttrType(str, Enum):
    FLOAT = "float"
    INT = "int"
    BOOL = "bool"
    STR = "str"


class CensusRace(str, Enum):
    # https://www.census.gov/quickfacts/fact/note/US/RHI625221
    WHITE = "white"
    BLACK = "black"
    AMIN = "amin"
    NHPI = "nhpi"
    OTHER = "other"


class User(Base):
    __tablename__ = "user"

    user_id = Column(Integer, primary_key=True)
    name = Column(String(DEFAULT_LENGTH), nullable=False)
    email = Column(String(254), nullable=False, unique=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    api_keys = relationship("ApiKey", back_populates="user")

    def __str__(self):
        return f"User(email={self.email}, name={self.name})"


class ApiKey(Base):
    __tablename__ = "api_key"

    key_hash = Column(postgresql.BYTEA, primary_key=True)
    user_id = Column(Integer, ForeignKey("user.user_id"), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    active = Column(Boolean, default=True)

    user = relationship("User", back_populates="api_keys")


class ObjectMeta(Base):
    __tablename__ = "meta"

    meta_id = Column(Integer, primary_key=True)
    notes = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    created_by = Column(Integer, ForeignKey("user.user_id"), nullable=False)


class Location(Base):
    __tablename__ = "location"
    __table_args__ = (CheckConstraint("parent_id <> loc_id"),)

    loc_id = Column(Integer, primary_key=True)
    name = Column(String(DEFAULT_LENGTH), nullable=False, unique=True)
    kind = Column(SqlEnum(LocationKind), nullable=False)
    parent_id = Column(Integer, ForeignKey("location.loc_id"))
    meta_id = Column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    parent = relationship("Location")
    meta = relationship("ObjectMeta")
    aliases = relationship("LocationAlias")

    def __str__(self):
        return f"Location(loc_id={self.loc_id}, name={self.name}, kind={self.kind})"


class LocationAlias(Base):
    __tablename__ = "location_alias"

    alias_id = Column(Integer, primary_key=True)
    loc_id = Column(Integer, ForeignKey("location.loc_id"), nullable=False)
    name = Column(String(DEFAULT_LENGTH), nullable=False, unique=True)
    kind = Column(SqlEnum(AliasKind), nullable=False)
    meta_id = Column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    loc = relationship("Location", back_populates="aliases")
    meta = relationship("ObjectMeta")


class GeoMeta(Base):
    __tablename__ = "geo_meta"

    geo_meta_id = Column(Integer, primary_key=True)
    unit = Column(SqlEnum(GeoUnit), nullable=False)
    version = Column(String(DEFAULT_LENGTH), nullable=False)
    proj = Column(String(DEFAULT_LENGTH))
    notes = Column(Text)
    source_url = Column(String(2048))
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    created_by = Column(Integer, ForeignKey("user.user_id"), nullable=False)


class Geography(Base):
    __tablename__ = "geography"

    geo_id = Column(Integer, primary_key=True)
    loc_id = Column(Integer, ForeignKey("location.loc_id"), nullable=False)
    geo_meta_id = Column(Integer, ForeignKey("geo_meta.geo_meta_id"), nullable=False)
    meta_id = Column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    geo_meta = relationship("GeoMeta")
    meta = relationship("ObjectMeta")


class GeoNode(Base):
    __tablename__ = "geo_node"

    node_id = Column(Integer, primary_key=True)
    canonical_id = Column(String(DEFAULT_LENGTH), nullable=False)
    geometry = Column(Geometry, nullable=False)


class GeoMember(Base):
    __tablename__ = "geo_member"

    geo_id = Column(Integer, ForeignKey("geography.geo_id"), primary_key=True)
    node_id = Column(Integer, ForeignKey("geo_node.node_id"), primary_key=True)


class GeoNodeHierarchy(Base):
    __tablename__ = "geo_node_hierarchy"
    __table_args__ = (CheckConstraint("parent_id <> child_id"),)

    parent_id = Column(Integer, ForeignKey("geo_node.node_id"), primary_key=True)
    child_id = Column(Integer, ForeignKey("geo_node.node_id"), primary_key=True)


class Ensemble(Base):
    __tablename__ = "ensemble"
    __table_args__ = (UniqueConstraint("graph_id", "name"),)

    ensemble_id = Column(Integer, primary_key=True)
    graph_id = Column(Integer, ForeignKey("dual_graph.graph_id"), nullable=False)
    path = Column(String(4096), nullable=False, unique=True)
    level = Column(SqlEnum(PlanLevel), nullable=False)
    num_districts = Column(Integer, nullable=False)
    params = Column(postgresql.JSONB)
    count = Column(Integer, nullable=False)
    count_accepted = Column(Integer, nullable=False)

    name = Column(String(DEFAULT_LENGTH), nullable=False)
    meta_id = Column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    meta = relationship("ObjectMeta")


class Plan(Base):
    __tablename__ = "plan"
    __table_args__ = (UniqueConstraint("geo_id", "name"),)

    plan_id = Column(Integer, primary_key=True)
    geo_id = Column(Integer, ForeignKey("geography.geo_id"), nullable=False)
    level = Column(SqlEnum(PlanLevel), nullable=False)
    num_districts = Column(Integer, nullable=False)
    assignment = Column(postgresql.ARRAY(Integer, dimensions=1, zero_indexes=True))

    name = Column(String(DEFAULT_LENGTH), nullable=False)
    source_url = Column(String(2048))  # e.g. from Districtr
    districtr_id = Column(Integer)
    daves_id = Column(Integer)
    meta_id = Column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    meta = relationship("ObjectMeta")


class DualGraph(Base):
    __tablename__ = "dual_graph"
    __table_args__ = (UniqueConstraint("geo_id", "variant"),)

    graph_id = Column(Integer, primary_key=True)
    geo_id = Column(Integer, ForeignKey("geography.geo_id"), nullable=False)
    variant = Column(String(DEFAULT_LENGTH), default="default", nullable=False)
    node_ids = Column(postgresql.ARRAY(Integer, dimensions=1, zero_indexes=True))
    edges = Column(postgresql.ARRAY(Integer, dimensions=2, zero_indexes=True))
    meta_id = Column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    meta = relationship("ObjectMeta")


class Official(Base):
    __tablename__ = "official"

    pol_id = Column(Integer, primary_key=True)
    short_name = Column(String(DEFAULT_LENGTH), nullable=False)
    legal_name = Column(String(DEFAULT_LENGTH))
    races = Column(
        postgresql.ARRAY(SqlEnum(CensusRace), dimensions=1, zero_indexes=True)
    )
    hispanic = Column(Boolean)
    wikipedia_url = Column(String(2048))
    ballotpedia_url = Column(String(2048))
    meta_id = Column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    meta = relationship("ObjectMeta")


class Election(Base):
    __tablename__ = "election"

    election_id = Column(Integer, primary_key=True)
    kind = Column(SqlEnum(ElectionKind), nullable=False)
    date = Column(Date, nullable=False)
    meta_id = Column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    meta = relationship("ObjectMeta")
    candidates = relationship("ElectionCandidacy")


class Party(Base):
    __tablename__ = "party"

    party_id = Column(Integer, primary_key=True)
    name = Column(String(DEFAULT_LENGTH), unique=True)
    fec_code = Column(String(3), unique=True)
    meta_id = Column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    meta = relationship("ObjectMeta")


class ElectionCandidacy(Base):
    __tablename__ = "election_candidacy"
    __table_args__ = (UniqueConstraint("election_id", "official_id", "party_id"),)

    candidacy_id = Column(Integer, primary_key=True)
    election_id = Column(Integer, ForeignKey("election.election_id"), nullable=False)
    official_id = Column(Integer, ForeignKey("official.pol_id"))
    party_id = Column(Integer, ForeignKey("party.party_id"), nullable=False)
    district = Column(Integer)
    incumbent_status = Column(SqlEnum(IncumbentStatus))
    fec_candidate_id = Column(String(9))
    meta_id = Column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    official = relationship("Official")
    election = relationship("Election", back_populates="candidates")
    party = relationship("Party")
    meta = relationship("ObjectMeta")


class ElectionCandidacyVotes(Base):
    __tablename__ = "election_candidacy_votes"

    candidacy_id = Column(
        Integer, ForeignKey("election_candidacy.candidacy_id"), primary_key=True
    )
    geo_id = Column(Integer, ForeignKey("geography.geo_id"), primary_key=True)
    votes_attr_id = Column(Integer, ForeignKey("geo_attr.attr_id"))

    candidacy = relationship("ElectionCandidacy")
    geo = relationship("Geography")
    votes_attr = relationship("GeoAttr")


class GeoAttr(Base):
    __tablename__ = "geo_attr"

    attr_id = Column(Integer, primary_key=True)
    geo_id = Column(Integer, nullable=False)
    name = Column(String(DEFAULT_LENGTH), nullable=False)
    version = Column(String(DEFAULT_LENGTH), nullable=False)
    type = Column(SqlEnum(GeoAttrType), nullable=False)
    meta_id = Column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    meta = relationship("ObjectMeta")


class GeoAttrTable(Base):
    __tablename__ = "geo_attr_table"

    table_id = Column(Integer, primary_key=True)
    name = Column(String(DEFAULT_LENGTH), nullable=False, unique=True)
    meta_id = Column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    meta = relationship("ObjectMeta")


class GeoAttrTableColumn(Base):
    __tablename__ = "geo_attr_table_column"

    table_id = Column(Integer, ForeignKey("geo_attr_table.table_id"), primary_key=True)
    col_id = Column(Integer, ForeignKey("geo_attr.attr_id"), primary_key=True)


class GeoAttrFloat(Base):
    __tablename__ = "geo_attr_float"

    node_id = Column(
        Integer, ForeignKey("geo_node.node_id"), nullable=False, primary_key=True
    )
    attr_id = Column(
        Integer, ForeignKey("geo_attr.attr_id"), nullable=False, primary_key=True
    )
    val = Column(postgresql.DOUBLE_PRECISION, nullable=False)


class GeoAttrInt(Base):
    __tablename__ = "geo_attr_int"

    node_id = Column(
        Integer, ForeignKey("geo_node.node_id"), nullable=False, primary_key=True
    )
    attr_id = Column(
        Integer, ForeignKey("geo_attr.attr_id"), nullable=False, primary_key=True
    )
    val = Column(Integer, nullable=False)


class GeoAttrBool(Base):
    __tablename__ = "geo_attr_bool"

    node_id = Column(
        Integer, ForeignKey("geo_node.node_id"), nullable=False, primary_key=True
    )
    attr_id = Column(
        Integer, ForeignKey("geo_attr.attr_id"), nullable=False, primary_key=True
    )
    val = Column(Boolean, nullable=False)


class GeoAttrStr(Base):
    __tablename__ = "geo_attr_str"

    node_id = Column(
        Integer, ForeignKey("geo_node.node_id"), nullable=False, primary_key=True
    )
    attr_id = Column(
        Integer, ForeignKey("geo_attr.attr_id"), nullable=False, primary_key=True
    )
    val = Column(String(65535), nullable=False)
