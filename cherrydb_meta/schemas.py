"""Enums and pydantic schemas for CherryDB."""
from enum import Enum
from datetime import datetime
from pydantic import BaseModel, constr

DEFAULT_LENGTH = 200


class User(BaseModel):
    user_id: int
    email: constr(max_length=254)  # type: ignore
    created_at: datetime


class Geography(BaseModel):
    geo_id: int
    name: constr(max_length=100)  # type: ignore
    # unit: GeoUnit
    vintage: int
    description: str
