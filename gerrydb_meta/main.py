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

from uvicorn.config import LOGGING_CONFIG, logger

from io import BytesIO
import json
import gzip

API_PREFIX = "/api/v1"

app = FastAPI(title="gerrydb-meta", openapi_url=f"{API_PREFIX}/openapi.json")


@app.exception_handler(CreateValueError)
def create_value_error(request: Request, exc: CreateValueError):
    """Handles generic object creation failures."""
    return JSONResponse(
        status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
        content={
            "kind": "Create value error",
            "detail": f"Object creation failed. Reason: {exc}",
        },
    )


@app.exception_handler(ColumnValueTypeError)
def column_value_type_error(request: Request, exc: ColumnValueTypeError):
    """Handles generic object creation failures."""
    return JSONResponse(
        status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
        content={
            "kind": "Column value type error",
            "detail": "Type errors found in column values.",
            "errors": exc.errors,
        },
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


@app.middleware("http")
async def log_400_errors(request: Request, call_next):
    response = await call_next(request)

    # Check for the specific status code
    if response.status_code in [400, 422]:

        # If the response is a StreamingResponse, you need to iterate over the body
        # and consume it to log the content
        if isinstance(response, StreamingResponse):
            body_content = b""
            # Reconstruct the response body from the stream
            async for chunk in response.body_iterator:
                body_content += chunk

            # Need this in case the original body is encoded
            original_body = body_content

            # Handle potential gzip encoding
            if response.headers.get("Content-Encoding") == "gzip":
                body_content = gzip.decompress(body_content)

            try:
                json_body = json.loads(body_content.decode())
                logger.error(
                    f"{response.status_code} for Request: {request.method}. At: {request.url}, Detail: {json_body.get('detail', 'No detail available')}"
                )
            except Exception as e:
                # Log the error with the reconstructed body content
                logger.error(
                    f"{response.status_code} for Request: {request.method}. At: {request.url}, Detail: {body_content.decode('utf-8', errors='replace')}"
                )

            # Create a new StreamingResponse with the consumed content
            response = StreamingResponse(
                BytesIO(original_body),
                status_code=response.status_code,
                headers=dict(response.headers),
            )
        else:
            # For non-streaming responses, you can directly access the body
            body_content = b""
            if hasattr(response, "body"):
                body_content = response.body

            # Handle potential gzip encoding
            if response.headers.get("Content-Encoding") == "gzip":
                body_content = gzip.decompress(body_content)

            try:
                json_body = json.loads(body_content.decode())
                logger.error(
                    f"{response.status_code} for Request: {request.method}. At: {request.url}, Detail: {json_body.get('detail', 'No detail available')}"
                )
            except Exception as e:
                logger.error(
                    f"{response.status_code} for Request: {request.method}. At: {request.url}, Detail: {body_content.decode('utf-8', errors='replace')}"
                )

    return response


@app.get("/health")
def health_check():
    return {"status": "healthy"}
