from pymongo.database_shared import _check_name
from pymongo.read_preferences import (
    Secondary,
    Primary,
    PrimaryPreferred,
    SecondaryPreferred,
    Nearest,
)

DEFAULT_CONNECTION_NAME = "default"
DEFAULT_DATABASE_NAME = "test"
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 27017

_class_registry_cache = {}
_field_list_cache = []


def _check_db_name(name):
    """Check if a database name is valid.
    This functionality is copied from pymongo Database class constructor.
    """
    if not isinstance(name, str):
        raise TypeError("name must be an instance of %s" % str)
    elif name != "$external":
        _check_name(name)


def convert_read_preference(
    value: str, tag_sets: list[str] | None = None, max_staleness: int = -1, hedge=None
):
    if not value:
        return Primary()

    value = value.lower()

    mapping = {
        "primary": Primary(),
        "primarypreferred": PrimaryPreferred(
            tag_sets=tag_sets, max_staleness=max_staleness, hedge=hedge
        ),
        "secondary": Secondary(
            tag_sets=tag_sets, max_staleness=max_staleness, hedge=hedge
        ),
        "secondarypreferred": SecondaryPreferred(
            tag_sets=tag_sets, max_staleness=max_staleness, hedge=hedge
        ),
        "nearest": Nearest(tag_sets=tag_sets, max_staleness=max_staleness, hedge=hedge),
    }

    if value not in mapping:
        raise ValueError(f"Invalid readPreference: {value}")

    return mapping[value]


def _import_class(cls_name):
    """Cache mechanism for imports.

    Due to complications of circular imports mongoengine needs to do lots of
    inline imports in functions.  This is inefficient as classes are
    imported repeated throughout the mongoengine code.  This is
    compounded by some recursive functions requiring inline imports.

    :mod:`mongoengine.common` provides a single point to import all these
    classes.  Circular imports aren't an issue as it dynamically imports the
    class when first needed.  Subsequent calls to the
    :func:`~mongoengine.common._import_class` can then directly retrieve the
    class from the :data:`mongoengine.common._class_registry_cache`.
    """
    if cls_name in _class_registry_cache:
        return _class_registry_cache.get(cls_name)

    doc_classes = (
        "Document",
        "DynamicEmbeddedDocument",
        "EmbeddedDocument",
        "MapReduceDocument",
    )

    # Field Classes
    if not _field_list_cache:
        from mongoengine.fields import __all__ as fields

        _field_list_cache.extend(fields)
        from mongoengine.base.fields import __all__ as fields

        _field_list_cache.extend(fields)

    field_classes = _field_list_cache

    if cls_name == "BaseDocument":
        from mongoengine.base import document as module

        import_classes = ["BaseDocument"]
    elif cls_name in doc_classes:
        from mongoengine import document as module

        import_classes = doc_classes
    elif cls_name in field_classes:
        from mongoengine import fields as module

        import_classes = field_classes
    else:
        raise ValueError("No import set for: %s" % cls_name)

    for cls in import_classes:
        _class_registry_cache[cls] = getattr(module, cls)

    return _class_registry_cache.get(cls_name)


async def _async_queryset_to_values(query):
    from mongoengine.asynchronous.queryset import AsyncQuerySet

    if isinstance(query, dict):
        new = {}
        for k, v in query.items():
            new[k] = await _async_queryset_to_values(v)
        return new

    if isinstance(query, list):
        return [await _async_queryset_to_values(x) for x in query]

    # Evaluate AsyncQuerySet here, at the correct event loop!
    if isinstance(query, AsyncQuerySet):
        return [v.pk async for v in query]
    return query


async def _normalize_async_values_document(doc):
    """
    Normalize an entire MongoEngine Document before saving:
    - Converts all AsyncQuerySet values into lists
    - Handles nested embedded docs, ListField, DictField
    - Writes values back into doc._data
    """
    from mongoengine.asynchronous.queryset import AsyncQuerySet
    from mongoengine.document import BaseDocument

    async def normalize(value):
        # AsyncQuerySet → list
        if isinstance(value, AsyncQuerySet):
            return [v async for v in value]

        # EmbeddedDocument → recurse into its _data
        if isinstance(value, BaseDocument) and not value._is_document:
            for k, v in value._data.items():
                value._data[k] = await normalize(v)
            return value

        # List → normalize items
        if isinstance(value, list):
            return [await normalize(v) for v in value]

        # Dict → normalize values
        if isinstance(value, dict):
            return {k: await normalize(v) for k, v in value.items()}

        # Normal primitive values untouched
        return value

    # Apply to top-level doc._data
    for key, value in doc._data.items():
        doc._data[key] = await normalize(value)

    return doc
