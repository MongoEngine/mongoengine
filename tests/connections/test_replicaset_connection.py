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

import pymongo
import mongoengine
from mongoengine import *
from mongoengine.connection import (MongoEngineConnectionError,
                                    get_db)


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
                           host="mongodb://localhost/mongoenginetest?replicaSet=local-rs",
                           read_preference=READ_PREF)
        except MongoEngineConnectionError as e:
            return

        if not isinstance(conn, CONN_CLASS):
            # really???
            return

        self.assertEqual(conn.read_preference, READ_PREF)

    def test_connect_with_replicaset_via_kwargs(self):
        """Ensure connect() works when specifying a replicaSet via the
        connection kwargs
        """
        if IS_PYMONGO_3:
            c = connect(replicaset='local-rs')
            self.assertEqual(c._MongoClient__options.replica_set_name,
                             'local-rs')
            db = get_db()
            self.assertTrue(isinstance(db, pymongo.database.Database))
            self.assertEqual(db.name, 'test')
        else:
            # PyMongo < v3.x raises an exception:
            # "localhost:27017 is not a member of replica set local-rs"
            with self.assertRaises(MongoEngineConnectionError):
                c = connect(replicaset='local-rs')

    def test_connect_with_replicaset_via_uri(self):
        """Ensure connect() works when specifying a replicaSet via the
        MongoDB URI.
        """
        if IS_PYMONGO_3:
            c = connect(host='mongodb://localhost/test?replicaSet=local-rs')
            db = get_db()
            self.assertTrue(isinstance(db, pymongo.database.Database))
            self.assertEqual(db.name, 'test')
        else:
            # PyMongo < v3.x raises an exception:
            # "localhost:27017 is not a member of replica set local-rs"
            with self.assertRaises(MongoEngineConnectionError):
                c = connect(host='mongodb://localhost/test?replicaSet=local-rs')

if __name__ == '__main__':
    unittest.main()
