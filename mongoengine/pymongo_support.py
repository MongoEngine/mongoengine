"""
Helper functions, constants, and types to aid with PyMongo support.
"""

import pymongo
from bson import binary, json_util

from mongoengine import connection

PYMONGO_VERSION = tuple(pymongo.version_tuple[:2])

LEGACY_JSON_OPTIONS = json_util.LEGACY_JSON_OPTIONS.with_options(
    uuid_representation=binary.UuidRepresentation.PYTHON_LEGACY,
)


def count_documents(
    collection, filter, skip=None, limit=None, hint=None, collation=None
):
    """Count documents, using collection metadata when possible."""
    if limit == 0:
        return 0  # Pymongo raises an OperationFailure if called with limit=0

    kwargs = {}
    if skip is not None:
        kwargs["skip"] = skip
    if limit is not None:
        kwargs["limit"] = limit
    if hint not in (-1, None):
        kwargs["hint"] = hint
    if collation is not None:
        kwargs["collation"] = collation

    session = connection._get_session()
    if not filter and not kwargs and session is None:
        # when no filter is provided, estimated_document_count
        # is a lot faster as it uses the collection metadata
        return collection.estimated_document_count(**kwargs)
    return collection.count_documents(filter=filter, session=session, **kwargs)


def list_collection_names(db, include_system_collections=False):
    """Return collection names, optionally including system collections."""
    collections = db.list_collection_names(session=connection._get_session())

    if not include_system_collections:
        collections = [c for c in collections if not c.startswith("system.")]

    return collections
