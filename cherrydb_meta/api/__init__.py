"""Base configuration and routing for Cherry API endpoints."""
from fastapi import APIRouter

from cherrydb_meta.api.localities import router as localities_router
from cherrydb_meta.api.obj_meta import router as obj_meta_router

api_router = APIRouter()
api_router.include_router(localities_router, prefix="/localities", tags=["localities"])
api_router.include_router(obj_meta_router, prefix="/meta", tags=["meta"])
