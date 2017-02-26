import unittest

from nose.plugins.skip import SkipTest

from mongoengine import connect
from mongoengine.connection import get_db, get_connection


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


def skip_in_old_mongodb(msg):
    """Raise a SkipTest exception with a given message if we're working
    with MongoDB version lower than v2.6.
    """
    mongodb_ver = get_mongodb_version()
    if mongodb_ver[0] == 2 and mongodb_ver[1] < 6:
        raise SkipTest(msg)
