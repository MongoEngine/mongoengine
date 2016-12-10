"""Helper functions and types to aid with Python 2.6 - 3 support."""

import sys
import warnings

import pymongo


# Show a deprecation warning for people using Python v2.6
# TODO remove in mongoengine v0.11.0
if sys.version_info[0] == 2 and sys.version_info[1] == 6:
    warnings.warn(
        'Python v2.6 support is deprecated and is going to be dropped '
        'entirely in the upcoming v0.11.0 release. Update your Python '
        'version if you want to have access to the latest features and '
        'bug fixes in MongoEngine.',
        DeprecationWarning
    )

if pymongo.version_tuple[0] < 3:
    IS_PYMONGO_3 = False
else:
    IS_PYMONGO_3 = True

PY3 = sys.version_info[0] == 3

if PY3:
    import codecs
    from io import BytesIO as StringIO

    # return s converted to binary.  b('test') should be equivalent to b'test'
    def b(s):
        return codecs.latin_1_encode(s)[0]

    bin_type = bytes
    txt_type = str
else:
    try:
        from cStringIO import StringIO
    except ImportError:
        from StringIO import StringIO

    # Conversion to binary only necessary in Python 3
    def b(s):
        return s

    bin_type = str
    txt_type = unicode

str_types = (bin_type, txt_type)
