"""
Helper functions, constants, and types to aid with PyMongo v2.7 - v3.x support.
"""
import pymongo
from pymongo.errors import OperationFailure

_PYMONGO_37 = (3, 7)

PYMONGO_VERSION = tuple(pymongo.version_tuple[:2])

IS_PYMONGO_GTE_37 = PYMONGO_VERSION >= _PYMONGO_37


def count_documents(
    collection, filter, skip=None, limit=None, hint=None, collation=None
):
    """Pymongo>3.7 deprecates count in favour of count_documents"""
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

    # count_documents appeared in pymongo 3.7
    if IS_PYMONGO_GTE_37:
        try:
            return collection.count_documents(filter=filter, **kwargs)
        except OperationFailure:
            # OperationFailure - accounts for some operators that used to work
            # with .count but are no longer working with count_documents (i.e $geoNear, $near, and $nearSphere)
            # fallback to deprecated Cursor.count
            # Keeping this should be reevaluated the day pymongo removes .count entirely
            pass

    cursor = collection.find(filter)
    for option, option_value in kwargs.items():
        cursor_method = getattr(cursor, option)
        cursor = cursor_method(option_value)
    with_limit_and_skip = "skip" in kwargs or "limit" in kwargs
    return cursor.count(with_limit_and_skip=with_limit_and_skip)


def list_collection_names(db, include_system_collections=False):
    """Pymongo>3.7 deprecates collection_names in favour of list_collection_names"""
    if IS_PYMONGO_GTE_37:
        collections = db.list_collection_names()
    else:
        collections = db.collection_names()

    if not include_system_collections:
        collections = [c for c in collections if not c.startswith("system.")]

    return collections
