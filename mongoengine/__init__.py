from . import document
from .document import *
from . import fields
from .fields import *
from . import connection
from .connection import *
from . import queryset
from .queryset import *
from . import signals
from .signals import *
from .errors import *
from . import errors

__all__ = (list(document.__all__) + fields.__all__ + connection.__all__ +
           list(queryset.__all__) + signals.__all__ + list(errors.__all__))

VERSION = (0, 8, 2)
MALLARD = True


def get_version():
    if isinstance(VERSION[-1], str):
        return '.'.join(map(str, VERSION[:-1])) + VERSION[-1]
    return '.'.join(map(str, VERSION))

__version__ = get_version()
