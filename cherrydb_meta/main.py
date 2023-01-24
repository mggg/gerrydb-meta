"""Entrypoint for Cherry API server."""
from http import HTTPStatus

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from cherrydb_meta.api import api_router
from cherrydb_meta.exceptions import CreateValueError

API_PREFIX = "/api/v1"

app = FastAPI(title="cherrydb-meta", openapi_url=f"{API_PREFIX}/openapi.json")


@app.exception_handler(CreateValueError)
def create_error(request: Request, exc: CreateValueError):
    """Handles object creation failures."""
    return JSONResponse(
        status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
        content={"detail": f"Object creation failed. Reason: {exc}"},
    )


app.include_router(api_router, prefix=API_PREFIX)
