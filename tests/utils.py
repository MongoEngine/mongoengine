import unittest

from nose.plugins.skip import SkipTest

from mongoengine import connect
from mongoengine.connection import get_db, get_connection
from mongoengine.python_support import IS_PYMONGO_3


MONGO_TEST_DB = 'mongoenginetest'


class MongoDBTestCase(unittest.TestCase):
    """Base class for tests that need a mongodb connection
    db is being dropped automatically
    """

    @classmethod
    def setUpClass(cls):
        cls._connection = connect(db=MONGO_TEST_DB)
        cls._connection.drop_database(MONGO_TEST_DB)
        cls.db = get_db()

    @classmethod
    def tearDownClass(cls):
        cls._connection.drop_database(MONGO_TEST_DB)


def get_mongodb_version():
    """Return the version tuple of the MongoDB server that the default
    connection is connected to.
    """
    return tuple(get_connection().server_info()['versionArray'])

def _decorated_with_ver_requirement(func, ver_tuple):
    """Return a given function decorated with the version requirement
    for a particular MongoDB version tuple.
    """
    def _inner(*args, **kwargs):
        mongodb_ver = get_mongodb_version()
        if mongodb_ver >= ver_tuple:
            return func(*args, **kwargs)

        raise SkipTest('Needs MongoDB v{}+'.format(
            '.'.join([str(v) for v in ver_tuple])
        ))

    _inner.__name__ = func.__name__
    _inner.__doc__ = func.__doc__

    return _inner

def needs_mongodb_v26(func):
    """Raise a SkipTest exception if we're working with MongoDB version
    lower than v2.6.
    """
    return _decorated_with_ver_requirement(func, (2, 6))

def needs_mongodb_v3(func):
    """Raise a SkipTest exception if we're working with MongoDB version
    lower than v3.0.
    """
    return _decorated_with_ver_requirement(func, (3, 0))

def skip_pymongo3(f):
    """Raise a SkipTest exception if we're running a test against
    PyMongo v3.x.
    """
    def _inner(*args, **kwargs):
        if IS_PYMONGO_3:
            raise SkipTest("Useless with PyMongo 3+")
        return f(*args, **kwargs)

    _inner.__name__ = f.__name__
    _inner.__doc__ = f.__doc__

    return _inner

