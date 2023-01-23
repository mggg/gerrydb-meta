"""Base configuration and routing for Cherry API endpoints."""
from cherrydb_meta.api.locations import router as locations_router
from cherrydb_meta.api.obj_meta import router as obj_meta_router

from fastapi import APIRouter
    
api_router = APIRouter()
api_router.include_router(locations_router, prefix="/locations", tags=["locations"])
api_router.include_router(obj_meta_router, prefix="/meta", tags=["meta"])