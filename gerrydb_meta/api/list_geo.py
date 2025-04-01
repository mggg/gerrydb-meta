"""Endpoints for listing paths"""

import logging
from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy.orm import Session

from gerrydb_meta import crud
from gerrydb_meta.api.deps import (
    can_read_localities,
    get_db,
    get_scopes,
)
from gerrydb_meta.scopes import ScopeManager
from sqlalchemy import text


log = logging.getLogger()

router = APIRouter()


@router.get(
    "/{namespace}/{fips}/{layer}",
    response_model=list[str],
    dependencies=[Depends(can_read_localities)],
)
def all_paths(
    *,
    namespace: str,
    fips: str,
    layer: str,
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

    query = text(
        """
        SELECT g.path
        FROM gerrydb.geography AS g
        INNER JOIN gerrydb.geo_set_member AS gsm ON g.geo_id = gsm.geo_id
        INNER JOIN gerrydb.geo_set_version AS gsv ON gsm.set_version_id = gsv.set_version_id
        INNER JOIN gerrydb.geo_layer AS gl ON gsv.layer_id = gl.layer_id
        INNER JOIN gerrydb.namespace AS ns ON gl.namespace_id = ns.namespace_id
        INNER JOIN gerrydb.locality AS loc ON gsv.loc_id = loc.loc_id
        INNER JOIN gerrydb.locality AS parent_loc ON loc.parent_id = parent_loc.loc_id
        INNER JOIN gerrydb.locality_ref AS lr ON parent_loc.loc_id = lr.loc_id
        WHERE ns.path = :namespace
        AND gl.path = :layer
        AND lr.path = :fips
        """
    )

    result = db.execute(query, {"namespace": namespace, "layer": layer, "fips": fips})

    geo_objs = [row[0] for row in result]

    return geo_objs
