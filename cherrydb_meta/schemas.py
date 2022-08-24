"""Enums and pydantic schemas for CherryDB."""
from enum import Enum
from datetime import datetime
from pydantic import BaseModel, constr

DEFAULT_LENGTH = 200

class GeoUnit(str, Enum):
    """Mapping level of a geography."""
    block = "block"
    bg = "bg"  # block group
    tract = "tract"
    county = "county"
    vtd = "vtd"
    ward = "ward"
    precinct = "precinct"
    cousub = "cousub"  # county subunit
    # TODO: Add more here from the Census spine.


class User(BaseModel):
    user_id: int
    email: constr(max_length=254)  # type: ignore
    created_at: datetime


class Geography(BaseModel):
    geo_id: int
    name: constr(max_length=100)  # type: ignore
    unit: GeoUnit
    vintage: int
    description: str
