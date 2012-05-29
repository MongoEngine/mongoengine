import unittest
import pymongo
from pymongo import ReadPreference, ReplicaSetConnection

import mongoengine
from mongoengine import *
from mongoengine.connection import get_db, get_connection, ConnectionError


class ConnectionTest(unittest.TestCase):

    def tearDown(self):
        mongoengine.connection._connection_settings = {}
        mongoengine.connection._connections = {}
        mongoengine.connection._dbs = {}

    def test_replicaset_uri_passes_read_preference(self):
        """Requires a replica set called "rs" on port 27017
        """

        try:
            conn = connect(db='mongoenginetest', host="mongodb://localhost/mongoenginetest?replicaSet=rs", read_preference=ReadPreference.SECONDARY_ONLY)
        except ConnectionError, e:
            return

        if not isinstance(conn, ReplicaSetConnection):
            return

        self.assertEquals(conn.read_preference, ReadPreference.SECONDARY_ONLY)

if __name__ == '__main__':
    unittest.main()
