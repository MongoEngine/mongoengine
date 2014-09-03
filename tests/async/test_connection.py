from mongoengine import *
import motor
import mongoengine.connection
from mongoengine.connection import get_db, get_connection, ConnectionError

try:
    import unittest2 as unittest
except ImportError:
    import unittest


class ConnectionTest(unittest.TestCase):

    def setUp(self):
        mongoengine.connection._connection_settings = {}
        mongoengine.connection._connections = {}
        mongoengine.connection._dbs = {}

    def test_register_connection(self):
        """
        Ensure that the connect() method works properly.
        """
        register_connection('asyncdb', 'mongoengineasynctest', async=True)

        self.assertEqual(
            mongoengine.connection._connection_settings['asyncdb']['name'],
            'mongoengineasynctest')

        self.assertTrue(
            mongoengine.connection._connection_settings['asyncdb']['async'])
        conn = get_connection('asyncdb')
        self.assertTrue(isinstance(conn, motor.MotorClient))

        db = get_db('asyncdb')
        self.assertTrue(isinstance(db, motor.MotorDatabase))
        self.assertEqual(db.name, 'mongoengineasynctest')
