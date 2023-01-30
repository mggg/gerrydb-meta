"""Base configuration and routing for Cherry API endpoints."""
from fastapi import APIRouter

from cherrydb_meta.api import locality, namespace, obj_meta

api_router = APIRouter()
api_router.include_router(locality.router, prefix="/localities", tags=["localities"])
api_router.include_router(namespace.router, prefix="/namespaces", tags=["namespaces"])
api_router.include_router(obj_meta.router, prefix="/meta", tags=["meta"])
