"""Entrypoint for Cherry API server."""
from http import HTTPStatus

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from cherrydb_meta.api import api_router
from cherrydb_meta.exceptions import (
    BulkCreateError,
    CreateValueError,
    ColumnValueTypeError,
)

API_PREFIX = "/api/v1"

app = FastAPI(title="cherrydb-meta", openapi_url=f"{API_PREFIX}/openapi.json")


@app.exception_handler(CreateValueError)
def create_value_error(request: Request, exc: CreateValueError):
    """Handles generic object creation failures."""
    return JSONResponse(
        status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
        content={"detail": f"Object creation failed. Reason: {exc}"},
    )


@app.exception_handler(ColumnValueTypeError)
def column_value_type_error(request: Request, exc: ColumnValueTypeError):
    """Handles generic object creation failures."""
    return JSONResponse(
        status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
        content={"detail": "Type errors found in column values.", "errors": exc.errors},
    )


@app.exception_handler(BulkCreateError)
def bulk_create_error(request: Request, exc: BulkCreateError):
    """Handles (bulk) creation conflicts."""
    return JSONResponse(
        status_code=HTTPStatus.CONFLICT,
        content={
            "detail": f"Object creation failed. Reason: {exc}",
            "paths": exc.paths,
        },
    )


app.include_router(api_router, prefix=API_PREFIX)
