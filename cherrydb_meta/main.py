"""Entrypoint for Cherry API server."""
from http import HTTPStatus

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from cherrydb_meta.api import api_router
from cherrydb_meta.exceptions import CreateConflictError, CreateValueError

API_PREFIX = "/api/v1"

app = FastAPI(title="cherrydb-meta", openapi_url=f"{API_PREFIX}/openapi.json")


@app.exception_handler(CreateValueError)
def create_error(request: Request, exc: CreateValueError):
    """Handles generic object creation failures."""
    return JSONResponse(
        status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
        content={"detail": f"Object creation failed. Reason: {exc}"},
    )
    
    
@app.exception_handler(CreateConflictError)
def create_conflict_error(request: Request, exc: CreateConflictError):
    """Handles (bulk) creation conflicts."""
    return JSONResponse(
        status_code=HTTPStatus.CONFLICT,
        content={
            "detail": f"Object creation failed. Reason: {exc}",
            "paths": exc.paths,
        }
    )
    

app.include_router(api_router, prefix=API_PREFIX)
