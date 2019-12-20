"""
Helper functions, constants, and types to aid with PyMongo v2.7 - v3.x support.
"""
import pymongo

_PYMONGO_37 = (3, 7)

PYMONGO_VERSION = tuple(pymongo.version_tuple[:2])

IS_PYMONGO_GTE_37 = PYMONGO_VERSION >= _PYMONGO_37


def count_documents(collection, filter, skip=None, limit=None, hint=None, collation=None):
    """Pymongo>3.7 deprecates count in favour of count_documents
    """
    if limit == 0:
        return 0  # Pymongo raises an OperationFailure if called with limit=0

    if IS_PYMONGO_GTE_37:
        kwargs = {}
        if skip is not None:
            kwargs["skip"] = skip
        if limit is not None:
            kwargs["limit"] = limit
        if collation is not None:
            kwargs["collation"] = collation
        if hint not in (-1, None):
            kwargs["hint"] = hint
        return collection.count_documents(filter=filter, **kwargs)
    else:
        cursor = collection.find(filter)
        if limit:
            cursor = cursor.limit(limit)
        if skip:
            cursor = cursor.skip(skip)
        if hint != -1:
            cursor = cursor.hint(hint)
        count = cursor.count()
    return count


def list_collection_names(db, include_system_collections=False):
    """Pymongo>3.7 deprecates collection_names in favour of list_collection_names"""
    if IS_PYMONGO_GTE_37:
        collections = db.list_collection_names()
    else:
        collections = db.collection_names()

    if not include_system_collections:
        collections = [c for c in collections if not c.startswith("system.")]

    return collections
