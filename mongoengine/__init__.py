import document
from document import *
import fields
from fields import *
import connection
from connection import *
import queryset
from queryset import *
import signals
from signals import *

__all__ = (document.__all__ + fields.__all__ + connection.__all__ +
           queryset.__all__ + signals.__all__)

VERSION = (0, 7, 5)


def get_version():
    if isinstance(VERSION[-1], basestring):
        return '.'.join(map(str, VERSION[:-1])) + VERSION[-1]
    return '.'.join(map(str, VERSION))

__version__ = get_version()
