import pytest
from sqlalchemy.orm import Session
from unittest.mock import MagicMock
from gerrydb_meta.crud.api_key import ReadOnlyApiKey
from gerrydb_meta import models


def test_get_api_key(db_with_user_api_key):
    db, api_key = db_with_user_api_key
    read_api_key = ReadOnlyApiKey(models.ApiKey)

    assert read_api_key.get(db=db, id=api_key.key_hash) == api_key
