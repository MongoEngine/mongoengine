import unittest

from pymongo import MongoClient, ReadPreference

import mongoengine
from mongoengine.synchronous.connection import ConnectionFailure
from tests.synchronous.utils import reset_connections
from tests.utils import MONGO_TEST_DB

CONN_CLASS = MongoClient
READ_PREF = ReadPreference.SECONDARY


class ConnectionTest(unittest.TestCase):
    def setUp(self):
        reset_connections()

    def tearDown(self):
        reset_connections()

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
            raise TypeError(f"Expected {CONN_CLASS}, got {type(conn)}")

        assert conn.read_preference == READ_PREF


if __name__ == "__main__":
    unittest.main()
