import unittest

from mongoengine import connect
from mongoengine.connection import get_db

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
