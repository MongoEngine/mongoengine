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
import mongoengine.base.queryset as _queryset_base
import mongoengine.synchronous as _sync_modules
import mongoengine.asynchronous as _async_modules

# ---- public re-exports ----
from mongoengine.base.queryset import *  # noqa: F401,F403
from mongoengine.synchronous import *  # noqa: F401,F403
from mongoengine.asynchronous import *  # noqa: F401,F403

from mongoengine.document import *  # noqa: F401,F403
from mongoengine.errors import *  # noqa: F401,F403
from mongoengine.fields import *  # noqa: F401,F403
from mongoengine.signals import *  # noqa: F401,F403

# ---- public API surface ----
__all__ = (
    list(document.__all__)
    + list(fields.__all__)
    + list(_queryset_base.__all__)
    + list(_sync_modules.__all__)
    + list(_async_modules.__all__)
    + list(signals.__all__)
    + list(errors.__all__)
)

# ---- hide internals ----
del _queryset_base
del _sync_modules
del _async_modules

VERSION = (0, 30, 0)


def get_version():
    return ".".join(map(str, VERSION))


__version__ = get_version()
