from __future__ import absolute_import
from . import document
from .document import *
from . import fields
from .fields import *
from . import connection
from .connection import *
from . import queryset
from .queryset import *

__all__ = (document.__all__ + fields.__all__ + connection.__all__ +
           queryset.__all__)

__author__ = 'Harry Marr'

VERSION = (0, 4, 2)

def get_version():
    version = '%s.%s' % (VERSION[0], VERSION[1])
    if VERSION[2]:
        version = '%s.%s' % (version, VERSION[2])
    return version

__version__ = get_version()

