import datetime
from pymongo.errors import OperationFailure

try:
    import unittest2 as unittest
except ImportError:
    import unittest
from nose.plugins.skip import SkipTest

import pymongo
from bson.tz_util import utc

from mongoengine import (
    connect, register_connection,
    Document, DateTimeField
)
from mongoengine.python_support import IS_PYMONGO_3
import mongoengine.connection
from mongoengine.connection import (MongoEngineConnectionError, get_db,
                                    get_connection)


def get_tz_awareness(connection):
    if not IS_PYMONGO_3:
        return connection.tz_aware
    else:
        return connection.codec_options.tz_aware


class ConnectionTest(unittest.TestCase):

    def tearDown(self):
        mongoengine.connection._connection_settings = {}
        mongoengine.connection._connections = {}
        mongoengine.connection._dbs = {}

    def test_connect(self):
        """Ensure that the connect() method works properly."""
        connect('mongoenginetest')

        conn = get_connection()
        self.assertTrue(isinstance(conn, pymongo.mongo_client.MongoClient))

        db = get_db()
        self.assertTrue(isinstance(db, pymongo.database.Database))
        self.assertEqual(db.name, 'mongoenginetest')

        connect('mongoenginetest2', alias='testdb')
        conn = get_connection('testdb')
        self.assertTrue(isinstance(conn, pymongo.mongo_client.MongoClient))

    def test_connect_in_mocking(self):
        """Ensure that the connect() method works properly in mocking.
        """
        try:
            import mongomock
        except ImportError:
            raise SkipTest('you need mongomock installed to run this testcase')

        connect('mongoenginetest', host='mongomock://localhost')
        conn = get_connection()
        self.assertTrue(isinstance(conn, mongomock.MongoClient))

        connect('mongoenginetest2', host='mongomock://localhost', alias='testdb2')
        conn = get_connection('testdb2')
        self.assertTrue(isinstance(conn, mongomock.MongoClient))

        connect('mongoenginetest3', host='mongodb://localhost', is_mock=True, alias='testdb3')
        conn = get_connection('testdb3')
        self.assertTrue(isinstance(conn, mongomock.MongoClient))

        connect('mongoenginetest4', is_mock=True, alias='testdb4')
        conn = get_connection('testdb4')
        self.assertTrue(isinstance(conn, mongomock.MongoClient))

        connect(host='mongodb://localhost:27017/mongoenginetest5', is_mock=True, alias='testdb5')
        conn = get_connection('testdb5')
        self.assertTrue(isinstance(conn, mongomock.MongoClient))

        connect(host='mongomock://localhost:27017/mongoenginetest6', alias='testdb6')
        conn = get_connection('testdb6')
        self.assertTrue(isinstance(conn, mongomock.MongoClient))

        connect(host='mongomock://localhost:27017/mongoenginetest7', is_mock=True, alias='testdb7')
        conn = get_connection('testdb7')
        self.assertTrue(isinstance(conn, mongomock.MongoClient))

    def test_connect_with_host_list(self):
        """Ensure that the connect() method works when host is a list

        Uses mongomock to test w/o needing multiple mongod/mongos processes
        """
        try:
            import mongomock
        except ImportError:
            raise SkipTest('you need mongomock installed to run this testcase')

        connect(host=['mongomock://localhost'])
        conn = get_connection()
        self.assertTrue(isinstance(conn, mongomock.MongoClient))

        connect(host=['mongodb://localhost'], is_mock=True,  alias='testdb2')
        conn = get_connection('testdb2')
        self.assertTrue(isinstance(conn, mongomock.MongoClient))

        connect(host=['localhost'], is_mock=True,  alias='testdb3')
        conn = get_connection('testdb3')
        self.assertTrue(isinstance(conn, mongomock.MongoClient))

        connect(host=['mongomock://localhost:27017', 'mongomock://localhost:27018'], alias='testdb4')
        conn = get_connection('testdb4')
        self.assertTrue(isinstance(conn, mongomock.MongoClient))

        connect(host=['mongodb://localhost:27017', 'mongodb://localhost:27018'], is_mock=True,  alias='testdb5')
        conn = get_connection('testdb5')
        self.assertTrue(isinstance(conn, mongomock.MongoClient))

        connect(host=['localhost:27017', 'localhost:27018'], is_mock=True,  alias='testdb6')
        conn = get_connection('testdb6')
        self.assertTrue(isinstance(conn, mongomock.MongoClient))

    def test_disconnect(self):
        """Ensure that the disconnect() method works properly
        """
        conn1 = connect('mongoenginetest')
        mongoengine.connection.disconnect()
        conn2 = connect('mongoenginetest')
        self.assertTrue(conn1 is not conn2)

    def test_sharing_connections(self):
        """Ensure that connections are shared when the connection settings are exactly the same
        """
        connect('mongoenginetests', alias='testdb1')
        expected_connection = get_connection('testdb1')

        connect('mongoenginetests', alias='testdb2')
        actual_connection = get_connection('testdb2')

        # Handle PyMongo 3+ Async Connection
        if IS_PYMONGO_3:
            # Ensure we are connected, throws ServerSelectionTimeoutError otherwise.
            # Purposely not catching exception to fail test if thrown.
            expected_connection.server_info()

        self.assertEqual(expected_connection, actual_connection)

    def test_connect_uri(self):
        """Ensure that the connect() method works properly with URIs."""
        c = connect(db='mongoenginetest', alias='admin')
        c.admin.system.users.remove({})
        c.mongoenginetest.system.users.remove({})

        c.admin.add_user("admin", "password")
        c.admin.authenticate("admin", "password")
        c.mongoenginetest.add_user("username", "password")

        if not IS_PYMONGO_3:
            self.assertRaises(
                MongoEngineConnectionError, connect, 'testdb_uri_bad',
                host='mongodb://test:password@localhost'
            )

        connect("testdb_uri", host='mongodb://username:password@localhost/mongoenginetest')

        conn = get_connection()
        self.assertTrue(isinstance(conn, pymongo.mongo_client.MongoClient))

        db = get_db()
        self.assertTrue(isinstance(db, pymongo.database.Database))
        self.assertEqual(db.name, 'mongoenginetest')

        c.admin.system.users.remove({})
        c.mongoenginetest.system.users.remove({})

    def test_connect_uri_without_db(self):
        """Ensure connect() method works properly if the URI doesn't
        include a database name.
        """
        connect("mongoenginetest", host='mongodb://localhost/')

        conn = get_connection()
        self.assertTrue(isinstance(conn, pymongo.mongo_client.MongoClient))

        db = get_db()
        self.assertTrue(isinstance(db, pymongo.database.Database))
        self.assertEqual(db.name, 'mongoenginetest')

    def test_connect_uri_default_db(self):
        """Ensure connect() defaults to the right database name if
        the URI and the database_name don't explicitly specify it.
        """
        connect(host='mongodb://localhost/')

        conn = get_connection()
        self.assertTrue(isinstance(conn, pymongo.mongo_client.MongoClient))

        db = get_db()
        self.assertTrue(isinstance(db, pymongo.database.Database))
        self.assertEqual(db.name, 'test')

    def test_uri_without_credentials_doesnt_override_conn_settings(self):
        """Ensure connect() uses the username & password params if the URI
        doesn't explicitly specify them.
        """
        c = connect(host='mongodb://localhost/mongoenginetest',
                    username='user',
                    password='pass')

        # OperationFailure means that mongoengine attempted authentication
        # w/ the provided username/password and failed - that's the desired
        # behavior. If the MongoDB URI would override the credentials
        self.assertRaises(OperationFailure, get_db)

    def test_connect_uri_with_authsource(self):
        """Ensure that the connect() method works well with `authSource`
        option in the URI.
        """
        # Create users
        c = connect('mongoenginetest')
        c.admin.system.users.remove({})
        c.admin.add_user('username2', 'password')

        # Authentication fails without "authSource"
        if IS_PYMONGO_3:
            test_conn = connect(
                'mongoenginetest', alias='test1',
                host='mongodb://username2:password@localhost/mongoenginetest'
            )
            self.assertRaises(OperationFailure, test_conn.server_info)
        else:
            self.assertRaises(
                MongoEngineConnectionError,
                connect, 'mongoenginetest', alias='test1',
                host='mongodb://username2:password@localhost/mongoenginetest'
            )
            self.assertRaises(MongoEngineConnectionError, get_db, 'test1')

        # Authentication succeeds with "authSource"
        authd_conn = connect(
            'mongoenginetest', alias='test2',
            host=('mongodb://username2:password@localhost/'
                  'mongoenginetest?authSource=admin')
        )
        db = get_db('test2')
        self.assertTrue(isinstance(db, pymongo.database.Database))
        self.assertEqual(db.name, 'mongoenginetest')

        # Clear all users
        authd_conn.admin.system.users.remove({})

    def test_register_connection(self):
        """Ensure that connections with different aliases may be registered.
        """
        register_connection('testdb', 'mongoenginetest2')

        self.assertRaises(MongoEngineConnectionError, get_connection)
        conn = get_connection('testdb')
        self.assertTrue(isinstance(conn, pymongo.mongo_client.MongoClient))

        db = get_db('testdb')
        self.assertTrue(isinstance(db, pymongo.database.Database))
        self.assertEqual(db.name, 'mongoenginetest2')

    def test_register_connection_defaults(self):
        """Ensure that defaults are used when the host and port are None.
        """
        register_connection('testdb', 'mongoenginetest', host=None, port=None)

        conn = get_connection('testdb')
        self.assertTrue(isinstance(conn, pymongo.mongo_client.MongoClient))

    def test_connection_kwargs(self):
        """Ensure that connection kwargs get passed to pymongo."""
        connect('mongoenginetest', alias='t1', tz_aware=True)
        conn = get_connection('t1')

        self.assertTrue(get_tz_awareness(conn))

        connect('mongoenginetest2', alias='t2')
        conn = get_connection('t2')
        self.assertFalse(get_tz_awareness(conn))

    def test_connection_pool_via_kwarg(self):
        """Ensure we can specify a max connection pool size using
        a connection kwarg.
        """
        # Use "max_pool_size" or "maxpoolsize" depending on PyMongo version
        # (former was changed to the latter as described in
        # https://jira.mongodb.org/browse/PYTHON-854).
        # TODO remove once PyMongo < 3.0 support is dropped
        if pymongo.version_tuple[0] >= 3:
            pool_size_kwargs = {'maxpoolsize': 100}
        else:
            pool_size_kwargs = {'max_pool_size': 100}

        conn = connect('mongoenginetest', alias='max_pool_size_via_kwarg', **pool_size_kwargs)
        self.assertEqual(conn.max_pool_size, 100)

    def test_connection_pool_via_uri(self):
        """Ensure we can specify a max connection pool size using
        an option in a connection URI.
        """
        if pymongo.version_tuple[0] == 2 and pymongo.version_tuple[1] < 9:
            raise SkipTest('maxpoolsize as a URI option is only supported in PyMongo v2.9+')

        conn = connect(host='mongodb://localhost/test?maxpoolsize=100', alias='max_pool_size_via_uri')
        self.assertEqual(conn.max_pool_size, 100)

    def test_write_concern(self):
        """Ensure write concern can be specified in connect() via
        a kwarg or as part of the connection URI.
        """
        conn1 = connect(alias='conn1', host='mongodb://localhost/testing?w=1&j=true')
        conn2 = connect('testing', alias='conn2', w=1, j=True)
        if IS_PYMONGO_3:
            self.assertEqual(conn1.write_concern.document, {'w': 1, 'j': True})
            self.assertEqual(conn2.write_concern.document, {'w': 1, 'j': True})
        else:
            self.assertEqual(dict(conn1.write_concern), {'w': 1, 'j': True})
            self.assertEqual(dict(conn2.write_concern), {'w': 1, 'j': True})

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

    def test_datetime(self):
        connect('mongoenginetest', tz_aware=True)
        d = datetime.datetime(2010, 5, 5, tzinfo=utc)

        class DateDoc(Document):
            the_date = DateTimeField(required=True)

        DateDoc.drop_collection()
        DateDoc(the_date=d).save()

        date_doc = DateDoc.objects.first()
        self.assertEqual(d, date_doc.the_date)

    def test_multiple_connection_settings(self):
        connect('mongoenginetest', alias='t1', host="localhost")

        connect('mongoenginetest2', alias='t2', host="127.0.0.1")

        mongo_connections = mongoengine.connection._connections
        self.assertEqual(len(mongo_connections.items()), 2)
        self.assertTrue('t1' in mongo_connections.keys())
        self.assertTrue('t2' in mongo_connections.keys())
        if not IS_PYMONGO_3:
            self.assertEqual(mongo_connections['t1'].host, 'localhost')
            self.assertEqual(mongo_connections['t2'].host, '127.0.0.1')
        else:
            # Handle PyMongo 3+ Async Connection
            # Ensure we are connected, throws ServerSelectionTimeoutError otherwise.
            # Purposely not catching exception to fail test if thrown.
            mongo_connections['t1'].server_info()
            mongo_connections['t2'].server_info()
            self.assertEqual(mongo_connections['t1'].address[0], 'localhost')
            self.assertEqual(mongo_connections['t2'].address[0], '127.0.0.1')


if __name__ == '__main__':
    unittest.main()
