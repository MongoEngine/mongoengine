import sys

sys.path[0:0] = [""]
import unittest

from pymongo import ReadPreference

from mongoengine.python_support import IS_PYMONGO_3

if IS_PYMONGO_3:
    from pymongo import MongoClient
    CONN_CLASS = MongoClient
    READ_PREF = ReadPreference.SECONDARY
else:
    from pymongo import ReplicaSetConnection
    CONN_CLASS = ReplicaSetConnection
    READ_PREF = ReadPreference.SECONDARY_ONLY

import mongoengine
from mongoengine import *
from mongoengine.connection import ConnectionError


class ConnectionTest(unittest.TestCase):

    def setUp(self):
        mongoengine.connection._connection_settings = {}
        mongoengine.connection._connections = {}
        mongoengine.connection._dbs = {}

    def tearDown(self):
        mongoengine.connection._connection_settings = {}
        mongoengine.connection._connections = {}
        mongoengine.connection._dbs = {}

    def test_replicaset_uri_passes_read_preference(self):
        """Requires a replica set called "rs" on port 27017
        """

        try:
            conn = connect(db='mongoenginetest',
                           host="mongodb://localhost/mongoenginetest?replicaSet=rs",
                           read_preference=READ_PREF)
        except ConnectionError as e:
            return

        if not isinstance(conn, CONN_CLASS):
            # really???
            return

        self.assertEqual(conn.read_preference, READ_PREF)

if __name__ == '__main__':
    unittest.main()
