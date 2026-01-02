import unittest

from pymongo import MongoClient, ReadPreference

import mongoengine
from mongoengine.synchronous.connection import ConnectionFailure
from tests.utils import MONGO_TEST_DB

CONN_CLASS = MongoClient
READ_PREF = ReadPreference.SECONDARY


class ConnectionTest(unittest.TestCase):
    def setUp(self):
        mongoengine.synchronous.connection._connection_settings = {}
        mongoengine.synchronous.connection._connections = {}
        mongoengine.synchronous.connection._dbs = {}

    def tearDown(self):
        mongoengine.synchronous.connection._connection_settings = {}
        mongoengine.synchronous.connection._connections = {}
        mongoengine.synchronous.connection._dbs = {}

    def test_replicaset_uri_passes_read_preference(self):
        """Requires a replica set called "rs" on port 27017"""
        try:
            conn = mongoengine.connect(
                db=MONGO_TEST_DB,
                host=f"mongodb://localhost/{MONGO_TEST_DB}?replicaSet=rs",
                read_preference=READ_PREF,
            )
        except ConnectionFailure:
            return

        if not isinstance(conn, CONN_CLASS):
            # really???
            return

        assert conn.read_preference == READ_PREF


if __name__ == "__main__":
    unittest.main()
