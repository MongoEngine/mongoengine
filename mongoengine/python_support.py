"""
Helper functions, constants, and types to aid with Python v2.7 - v3.x and
PyMongo v2.7 - v3.x support.
"""
import pymongo
import six


IS_PYMONGO_3 = pymongo.version_tuple[0] >= 3

# six.BytesIO resolves to StringIO.StringIO in Py2 and io.BytesIO in Py3.
StringIO = six.BytesIO

# Additionally for Py2, try to use the faster cStringIO, if available
if not six.PY3:
    try:
        import cStringIO
    except ImportError:
        pass
    else:
        StringIO = cStringIO.StringIO
