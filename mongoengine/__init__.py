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

__author__ = 'Harry Marr'

VERSION = (0, 5, 3)


def get_version():
    version = '%s.%s' % (VERSION[0], VERSION[1])
    if VERSION[2]:
        version = '%s.%s' % (version, VERSION[2])
    return version

__version__ = get_version()
