from __future__ import absolute_import
from mongoengine import document
from mongoengine.document import *
from mongoengine import fields
from mongoengine.fields import *
from mongoengine import connection
from mongoengine.connection import *
from mongoengine import queryset
from mongoengine.queryset import *
from mongoengine import signals
from mongoengine.signals import *
from mongoengine.errors import *
from mongoengine import connections_manager
from mongoengine import errors
from six import string_types
from six.moves import map

__all__ = (list(document.__all__) + fields.__all__ + connection.__all__ +
           list(queryset.__all__) + signals.__all__ + list(errors.__all__) +
           list(connections_manager.__all__))

VERSION = (0, 10, 6)


def get_version():
    if isinstance(VERSION[-1], string_types):
        return '.'.join(map(str, VERSION[:-1])) + VERSION[-1]
    return '.'.join(map(str, VERSION))

__version__ = get_version()
