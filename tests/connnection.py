import unittest
import datetime
import pymongo

import mongoengine.connection
from mongoengine import *
from mongoengine.connection import _get_db, _get_connection


class ConnectionTest(unittest.TestCase):

    def tearDown(self):
        mongoengine.connection._connection_settings = {}
        mongoengine.connection._connections = {}
        mongoengine.connection._dbs = {}
    
    def test_connect(self):
        """Ensure that the connect() method works properly.
        """
        connect('mongoenginetest')

        conn = _get_connection()
        self.assertTrue(isinstance(conn, pymongo.connection.Connection))

        db = _get_db()
        self.assertTrue(isinstance(db, pymongo.database.Database))
        self.assertEqual(db.name, 'mongoenginetest')

    def test_register_connection(self):
        """Ensure that connections with different aliases may be registered.
        """
        register_connection('testdb', 'mongoenginetest2')

        self.assertRaises(ConnectionError, _get_connection)
        conn = _get_connection('testdb')
        self.assertTrue(isinstance(conn, pymongo.connection.Connection))

        db = _get_db('testdb')
        self.assertTrue(isinstance(db, pymongo.database.Database))
        self.assertEqual(db.name, 'mongoenginetest2')


if __name__ == '__main__':
    unittest.main()
