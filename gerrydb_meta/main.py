"""Entrypoint for Gerry API server."""
from http import HTTPStatus

from fastapi import FastAPI, Request, Response
from starlette.responses import StreamingResponse
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse

from gerrydb_meta.api import api_router
from gerrydb_meta.exceptions import (
    BulkCreateError,
    BulkPatchError,
    ColumnValueTypeError,
    CreateValueError,
)

import logging

API_PREFIX = "/api/v1"

app = FastAPI(title="gerrydb-meta", openapi_url=f"{API_PREFIX}/openapi.json")


@app.exception_handler(CreateValueError)
def create_value_error(request: Request, exc: CreateValueError):
    """Handles generic object creation failures."""
    return JSONResponse(
        status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
        content={
            "kind": "Create value error",
            "detail": f"Object creation failed. Reason: {exc}"},
    )


@app.exception_handler(ColumnValueTypeError)
def column_value_type_error(request: Request, exc: ColumnValueTypeError):
    """Handles generic object creation failures."""
    return JSONResponse(
        status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
        content={
            "kind": "Column value type error",
            "detail": "Type errors found in column values.", 
            "errors": exc.errors},
    )


@app.exception_handler(BulkCreateError)
def bulk_create_error(request: Request, exc: BulkCreateError):
    """Handles (bulk) creation conflicts."""
    return JSONResponse(
        status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
        content={
            "kind": "Bulk create error",
            "detail": f"Object creation failed. Reason: {exc}",
            "paths": exc.paths,
        },
    )


@app.exception_handler(BulkPatchError)
def bulk_create_error(request: Request, exc: BulkCreateError):
    """Handles (bulk) creation conflicts."""
    return JSONResponse(
        status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
        content={
            "kind": "BulkPatchError",
            "detail": f"Object patch failed. Reason: {exc}",
            "paths": exc.paths,
        },
    )


app.add_middleware(GZipMiddleware)
app.include_router(api_router, prefix=API_PREFIX)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


MAX_BUFFER_SIZE = 1024

@app.middleware("http")
async def log_422_errors(request: Request, call_next):
    response = await call_next(request)
    
    # Check for the specific status code
    if response.status_code == 422:
        print(response.headers)
        
        # If the response is a StreamingRessponse, you need to iterate over the body
        # and consume it to log the content
        if isinstance(response, StreamingResponse):
            body_content = b''
            async for chunk in response.body_iterator:
                body_content += chunk
                if len(body_content) > MAX_BUFFER_SIZE:
                    body_content = body_content[:MAX_BUFFER_SIZE]
                    break
            logger.info(
                f"\n\t422 for Request: {request.method},\n\tAt: {request.url}\n\tBody: {body_content.decode()}"
            )
            return Response(content=body_content, status_code=422, headers=dict(response.headers))
        else:
            # For non-streaming responses, you can directly access the body
            body_content = b''
            if hasattr(response, 'body'):
                body_content = response.body
            logger.info(
                f"\n\t422 for Request: {request.method},\n\tAt: {request.url}\n\tBody: {body_content.decode()}"
            )
    
    return response

@app.get("/health")
def health_check():
    return {"status": "healthy"}