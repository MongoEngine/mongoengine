import connection
from connection import *
import document
from document import *
import errors
from errors import *
import fields
from fields import *
import queryset
from queryset import *
import signals
from signals import *

__all__ = (list(document.__all__) + fields.__all__ + connection.__all__ +
           list(queryset.__all__) + signals.__all__ + list(errors.__all__))

VERSION = (0, 10, 7)


def get_version():
    if isinstance(VERSION[-1], basestring):
        return '.'.join(map(str, VERSION[:-1])) + VERSION[-1]
    return '.'.join(map(str, VERSION))


__version__ = get_version()
