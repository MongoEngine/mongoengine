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
    return get_connection().server_info()['versionArray']


def skip_older_mongodb(f):
    """Raise a SkipTest exception with a given message if we're working
    with MongoDB version lower than v2.6.
    """
    def _inner(*args, **kwargs):
        mongodb_ver = get_mongodb_version()
        if mongodb_ver[0] == 2 and mongodb_ver[1] < 6:
            raise SkipTest('Need MongoDB v2.6+')
        return f(*args, **kwargs)

    _inner.__name__ = f.__name__
    _inner.__doc__ = f.__doc__

    return _inner


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

