"""Entrypoint for Cherry API server."""
from http import HTTPStatus

import sqltap
from fastapi import FastAPI, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse

from cherrydb_meta.api import api_router
from cherrydb_meta.exceptions import (
    BulkCreateError,
    ColumnValueTypeError,
    CreateValueError,
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


# quick'n'dirty inspection:
# https://medium.com/indigoag-eng/profiling-orm-in-fastapi-d8f616b90aa2
@app.middleware("http")
async def tap(request: Request, call_next):
    profiler = sqltap.start()
    response = await call_next(request)
    statistics = profiler.collect()
    sqltap.report(statistics, "report.txt", report_format="text")
    return response


app.add_middleware(GZipMiddleware)
app.include_router(api_router, prefix=API_PREFIX)
