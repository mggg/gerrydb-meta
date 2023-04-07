"""Endpoints for views."""
import io
from collections import deque
from datetime import datetime
from enum import Enum
from http import HTTPStatus
from typing import Any

import msgpack
from fastapi import APIRouter, Depends, HTTPException, Response
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from cherrydb_meta import crud, models, schemas
from cherrydb_meta.api.base import (
    MsgpackResponse,
    add_etag,
    namespace_with_read,
    parse_path,
)
from cherrydb_meta.api.deps import can_read_localities, get_db, get_obj_meta, get_scopes
from cherrydb_meta.scopes import ScopeManager

router = APIRouter()
BUF_SIZE = 32 * 1024**2


@router.post(
    "/{namespace}",
    response_model=None,
    response_class=MsgpackResponse,
    status_code=HTTPStatus.CREATED,
    dependencies=[Depends(can_read_localities)],
)
def create_view(
    *,
    response: Response,
    namespace: str,
    obj_in: schemas.ViewCreate,
    db: Session = Depends(get_db),
    obj_meta: models.ObjectMeta = Depends(get_obj_meta),
    scopes: ScopeManager = Depends(get_scopes),
):
    view_namespace_obj = crud.namespace.get(db=db, path=namespace)
    if view_namespace_obj is None or not scopes.can_write_derived_in_namespace(
        view_namespace_obj
    ):
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=(
                f'Namespace "{namespace}" not found, or you do not have '
                "sufficient permissions to write views in this namespace."
            ),
        )

    locality_obj = crud.locality.get_by_ref(db=db, path=obj_in.locality)
    if locality_obj is None:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail="Locality not found."
        )

    layer_namespace, layer_path = parse_path(obj_in.layer)
    template_namespace, template_path = parse_path(obj_in.template)
    if obj_in.graph is None:
        graph_namespace = graph_path = None
    else:
        graph_namespace, graph_path = parse_path(obj_in.graph)

    namespaces = {
        "layer": namespace if layer_namespace is None else layer_namespace,
        "template": namespace if template_namespace is None else template_namespace,
        "graph": namespace if graph_namespace is None else graph_namespace,
    }
    namespace_objs = {}
    for namespace_label, resource_namespace in namespaces.items():
        namespace_objs[namespace_label] = namespace_with_read(
            db=db, scopes=scopes, path=resource_namespace, base_namespace=namespace
        )

    template_obj = crud.view_template.get(
        db, path=template_path, namespace=namespace_objs["template"]
    )
    if template_obj is None:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail="View template not found."
        )

    layer_obj = crud.geo_layer.get(
        db, path=layer_path, namespace=namespace_objs["layer"]
    )
    if layer_obj is None:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail="Geographic layer not found."
        )

    if graph_path is None:
        graph_obj = None
    else:
        graph_obj = crud.graph.get(
            db, path=graph_path, namespace=namespace_objs["graph"]
        )
        if graph_obj is None:
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND, detail="Dual graph not found."
            )

    view_obj, etag = crud.view.create(
        db=db,
        obj_in=obj_in,
        obj_meta=obj_meta,
        namespace=view_namespace_obj,
        template=template_obj,
        locality=locality_obj,
        layer=layer_obj,
        graph=graph_obj,
    )
    view_stream = crud.view.instantiate(db=db, view=view_obj)
    add_etag(response, etag)
    return StreamingResponse(
        io.BufferedReader(
            ViewMsgpackStream(view=view_obj, stream=view_stream),
            buffer_size=BUF_SIZE,
        ),
        media_type="application/msgpack",
    )


@router.get(
    "/{namespace}/{path:path}",
    response_model=None,
    response_class=MsgpackResponse,
    status_code=HTTPStatus.CREATED,
    dependencies=[Depends(can_read_localities)],
)
def get_view(
    *,
    response: Response,
    namespace: str,
    path: str,
    db: Session = Depends(get_db),
    scopes: ScopeManager = Depends(get_scopes),
):
    view_namespace_obj = crud.namespace.get(db=db, path=namespace)
    if view_namespace_obj is None or not scopes.can_read_in_namespace(
        view_namespace_obj
    ):
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=(
                f'Namespace "{namespace}" not found, or you do not have '
                "sufficient permissions to write views in this namespace."
            ),
        )

    view_obj = crud.view.get(db=db, namespace=view_namespace_obj, path=path)
    if view_obj is None:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f"View not found in namespace.",
        )

    etag = crud.view.etag(db, view_namespace_obj)
    view_stream = crud.view.instantiate(db=db, view=view_obj)
    add_etag(response, etag)
    return StreamingResponse(
        io.BufferedReader(
            ViewMsgpackStream(view=view_obj, stream=view_stream),
            buffer_size=BUF_SIZE,
        ),
        media_type="application/msgpack",
    )


class ViewStreamChunkType(str, Enum):
    """A type of streamed data in `ViewMsgpackStream`."""

    GEO = "geographies"
    COLUMN_VALUE = "values"
    PLAN = "plans"
    GRAPH = "graph"


def _view_encode(obj) -> Any:
    """Custom MessagePack encoder for dates."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj


def _strip_array(dump: bytes) -> bytes:
    """Strips the array prefix from a Msgpack array dump."""
    # Formats:
    # https://github.com/msgpack/msgpack/blob/8aa09e2a6a9180a49fc62ecfefe149f063cc5e4b/spec.md#array-format-family
    if dump[0] & 0b11110000 == 0b10010000:
        return dump[1:]  # fixarray (≤15 elements)
    if dump[0] == 0xDC:
        return dump[3:]  # array 16 (up to 2^16 - 1 elements)
    if dump[0] == 0xDD:
        return dump[5:]  # array 32 (up to 2^32 - 1 elements)
    raise ValueError("Not a Msgpack array dump.")


class ViewMsgpackStream(io.RawIOBase):
    """Renders database-streamed view data as a Msgpack stream."""

    view: models.View
    stream: crud.ViewStream

    # Internal stream state.
    _buf: bytes | None
    _cur_chunk: tuple[ViewStreamChunkType, str | int | None] | None
    _chunks: deque[tuple[ViewStreamChunkType, str | int | None]]

    def __init__(self, view: models.View, stream: crud.ViewStream):
        self.view = view
        self.stream = stream

        # Buffer metadata fields.
        meta_fields = schemas.ViewMeta(
            path=view.path,
            namespace=view.namespace.path,
            template=schemas.ViewTemplate.from_orm(view.template_version),
            locality=view.loc,
            layer=view.layer,
            meta=view.meta,
            valid_at=view.at,
            proj=view.proj,
        ).dict()

        # `geographies`, `values`, `plans`, and `graph` are omitted
        # from this dump, as they will be streamed; we manually increase
        # the key/value count in the Msgpack serialization of these fields
        # to reflect these missing fields.
        #
        # msgpack map format:
        # https://github.com/msgpack/msgpack/blob/8aa09e2a6a9180a49fc62ecfefe149f063cc5e4b/spec.md#map-format-family
        field_count = len(meta_fields) + 4  # `geographies`, `values`, `plans`, `graph`
        self._buf = (
            (0xDE).to_bytes(1, byteorder="big")  # map16 container
            + field_count.to_bytes(2, byteorder="big")
            + msgpack.dumps(meta_fields, default=_view_encode)[1:]
        )

        # Initialize stream state: what will be streamed from the database next?
        self._chunks = deque(
            [
                (ViewStreamChunkType.GEO, None),
                (ViewStreamChunkType.GRAPH, None),
                (ViewStreamChunkType.COLUMN_VALUE, None),  # section markers
            ]
            + [(ViewStreamChunkType.COLUMN_VALUE, col) for col in stream.column_values]
            + [(ViewStreamChunkType.PLAN, None)]  # section marker
            + [(ViewStreamChunkType.PLAN, idx) for idx in range(len(stream.plans))]
        )
        self._cur_chunk = None

    def readable(self):
        return True

    def readinto(self, buf):
        if len(self._buf) >= len(buf):
            chunk_size = len(buf)
            buf[:chunk_size] = self._buf[:chunk_size]
            self._buf = self._buf[chunk_size:]
            return chunk_size

        while self._chunks and len(self._buf) < len(buf):
            # Refresh the buffer by streaming from the next database chunk (if available).
            if self._cur_chunk is None:
                self._cur_chunk = chunk_type, chunk_id = self._chunks.popleft()

                # Add root-level key and container if necessary.
                if chunk_id is None:
                    self._buf += msgpack.packb(chunk_type.value)

                    if chunk_type == ViewStreamChunkType.GEO:
                        # Create an array32 container for geographies.
                        # https://github.com/msgpack/msgpack/blob/8aa09e2a6a9180a49fc62ecfefe149f063cc5e4b/spec.md#array-format-family
                        self._buf += (0xDD).to_bytes(
                            1, byteorder="big"
                        ) + (  # array32 marker
                            self.stream.geo_count
                        ).to_bytes(
                            4, byteorder="big"
                        )  # size
                    elif chunk_type == ViewStreamChunkType.PLAN:
                        # Create an array32 container for plans.
                        self._buf += (0xDD).to_bytes(
                            1, byteorder="big"
                        ) + (  # array32 marker
                            len(self.stream.plans)
                        ).to_bytes(
                            4, byteorder="big"
                        )  # size
                    elif chunk_type == ViewStreamChunkType.COLUMN_VALUE:
                        # Create a map32 container for columns.
                        # https://github.com/msgpack/msgpack/blob/8aa09e2a6a9180a49fc62ecfefe149f063cc5e4b/spec.md#map-format-family
                        self._buf += (0xDF).to_bytes(
                            1, byteorder="big"
                        ) + (  # map32 marker
                            len(self.stream.column_values)
                        ).to_bytes(
                            4, byteorder="big"
                        )

                # Add nested container if necessary.
                if (
                    chunk_type == ViewStreamChunkType.COLUMN_VALUE
                    and chunk_id is not None
                ):
                    self._buf += (
                        msgpack.packb(chunk_id)  # column path
                        + (0xDD).to_bytes(1, byteorder="big")  # array32 marker
                        + (self.stream.geo_count).to_bytes(4, byteorder="big")  # size
                    )
            else:
                chunk_type, chunk_id = self._cur_chunk

            if chunk_type == ViewStreamChunkType.GEO:
                try:
                    geo_chunk = next(self.stream.geographies)
                    self._buf += _strip_array(
                        msgpack.dumps(
                            [
                                schemas.Geography.from_orm(geo).dict()
                                for geo in geo_chunk
                            ],
                            default=_view_encode,
                        )
                    )
                except StopIteration:
                    self._cur_chunk = None
            elif chunk_type == ViewStreamChunkType.GRAPH:
                self._buf += msgpack.dumps(
                    None
                    if self.view.graph is None
                    else schemas.Graph.from_orm(self.view.graph)
                )
                self._cur_chunk = None
            elif chunk_type == ViewStreamChunkType.PLAN:
                if chunk_id is not None:
                    self._buf += msgpack.dumps(
                        schemas.Plan.from_orm(self.stream.plans[chunk_id])
                    )
                self._cur_chunk = None
            elif chunk_type == ViewStreamChunkType.COLUMN_VALUE:
                if chunk_id is None:
                    # Skip over section marker.
                    self._cur_chunk = None
                else:
                    try:
                        value_chunk = next(self.stream.column_values[chunk_id])
                        self._buf += _strip_array(msgpack.dumps(value_chunk))
                    except StopIteration:
                        self._cur_chunk = None

        chunk_size = min(len(buf), len(self._buf))
        buf[:chunk_size] = self._buf[:chunk_size]
        self._buf = self._buf[chunk_size:]
        return chunk_size
