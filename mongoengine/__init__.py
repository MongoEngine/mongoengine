# Import submodules so that we can expose their __all__
import connection
import document
import errors
import fields
import queryset
import signals

# Import everything from each submodule so that it can be accessed via
# mongoengine, e.g. instead of `from mongoengine.connection import connect`,
# users can simply use `from mongoengine import connect`, or even
# `from mongoengine import *` and then `connect('testdb')`.
from connection import *
from document import *
from errors import *
from fields import *
from queryset import *
from signals import *


__all__ = (list(document.__all__) + list(fields.__all__) +
           list(connection.__all__) + list(queryset.__all__) +
           list(signals.__all__) + list(errors.__all__))


VERSION = (0, 10, 7)


def get_version():
    """Return the VERSION as a string, e.g. for VERSION == (0, 10, 7),
    return '0.10.7'.
    """
    if isinstance(VERSION[-1], basestring):
        return '.'.join(map(str, VERSION[:-1])) + VERSION[-1]
    return '.'.join(map(str, VERSION))


__version__ = get_version()
