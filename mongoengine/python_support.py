"""
Helper functions, constants, and types to aid with Python v2.6 - v3.x and
PyMongo v2.7 - v3.x support.
"""
import sys
import pymongo
import six


if pymongo.version_tuple[0] < 3:
    IS_PYMONGO_3 = False
else:
    IS_PYMONGO_3 = True


PY3 = sys.version_info[0] == 3


# six.BytesIO resolves to StringIO.StringIO in Py2 and io.BytesIO in Py3.
StringIO = six.BytesIO

# Additionally for Py2, try to use the faster cStringIO, if available
if not PY3:
    try:
        from cStringIO import StringIO
    except ImportError:
        pass
