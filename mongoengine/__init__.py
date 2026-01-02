"""
MongoEngine top-level public API.

Import submodules and re-export their public symbols so that users can write:

    from mongoengine import connect
    from mongoengine import async_connect
    from mongoengine import Document, StringField
    from mongoengine import QuerySet, AsyncQuerySet

Or simply:

    from mongoengine import *

Instead of importing from internal submodules.

This module exposes both synchronous and asynchronous APIs.
Asynchronous functionality is backed by PyMongo's native async support
(PyMongo >= 4.14).
"""

from mongoengine import document, errors, fields, signals

# ---- private imports (for __all__ only) ----
from mongoengine.synchronous import connection as _sync_connection
from mongoengine.asynchronous import connection as _async_connection
from mongoengine.synchronous import queryset as _sync_queryset
from mongoengine.asynchronous import queryset as _async_queryset

# ---- public re-exports ----
from mongoengine.synchronous.connection import *  # noqa: F401,F403
from mongoengine.asynchronous.connection import *  # noqa: F401,F403
from mongoengine.synchronous.queryset import *  # noqa: F401,F403
from mongoengine.asynchronous.queryset import *  # noqa: F401,F403

from mongoengine.document import *  # noqa: F401,F403
from mongoengine.errors import *  # noqa: F401,F403
from mongoengine.fields import *  # noqa: F401,F403
from mongoengine.signals import *  # noqa: F401,F403

# ---- public API surface ----
__all__ = (
        list(document.__all__)
        + list(fields.__all__)
        + list(_sync_connection.__all__)
        + list(_async_connection.__all__)
        + list(_sync_queryset.__all__)
        + list(_async_queryset.__all__)
        + list(signals.__all__)
        + list(errors.__all__)
)

# ---- hide internals ----
del _sync_connection
del _async_connection
del _sync_queryset
del _async_queryset

VERSION = (0, 30, 0)


def get_version():
    return ".".join(map(str, VERSION))


__version__ = get_version()
