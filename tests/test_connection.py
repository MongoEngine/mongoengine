import datetime

from pymongo import MongoClient
from pymongo.errors import OperationFailure, InvalidName

try:
    import unittest2 as unittest
except ImportError:
    import unittest
from nose.plugins.skip import SkipTest

import pymongo
from bson.tz_util import utc

from mongoengine import (
    connect, register_connection,
    Document, DateTimeField,
    disconnect_all, StringField)
from mongoengine.pymongo_support import IS_PYMONGO_3
import mongoengine.connection
from mongoengine.connection import (MongoEngineConnectionError, get_db,
                                    get_connection, disconnect, DEFAULT_DATABASE_NAME)


def get_tz_awareness(connection):
    if not IS_PYMONGO_3:
        return connection.tz_aware
    else:
        return connection.codec_options.tz_aware


class ConnectionTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        disconnect_all()

    @classmethod
    def tearDownClass(cls):
        disconnect_all()

    def tearDown(self):
        mongoengine.connection._connection_settings = {}
        mongoengine.connection._connections = {}
        mongoengine.connection._dbs = {}

    def test_connect(self):
        """Ensure that the connect() method works properly."""
        connect('mongoenginetest')

        conn = get_connection()
        self.assertIsInstance(conn, pymongo.mongo_client.MongoClient)

        db = get_db()
        self.assertIsInstance(db, pymongo.database.Database)
        self.assertEqual(db.name, 'mongoenginetest')

        connect('mongoenginetest2', alias='testdb')
        conn = get_connection('testdb')
        self.assertIsInstance(conn, pymongo.mongo_client.MongoClient)

    def test_connect_disconnect_works_properly(self):
        class History1(Document):
            name = StringField()
            meta = {'db_alias': 'db1'}

        class History2(Document):
            name = StringField()
            meta = {'db_alias': 'db2'}

        connect('db1', alias='db1')
        connect('db2', alias='db2')

        History1.drop_collection()
        History2.drop_collection()

        h = History1(name='default').save()
        h1 = History2(name='db1').save()

        self.assertEqual(list(History1.objects().as_pymongo()),
                         [{'_id': h.id, 'name': 'default'}])
        self.assertEqual(list(History2.objects().as_pymongo()),
                         [{'_id': h1.id, 'name': 'db1'}])

        disconnect('db1')
        disconnect('db2')

        with self.assertRaises(MongoEngineConnectionError):
            list(History1.objects().as_pymongo())

        with self.assertRaises(MongoEngineConnectionError):
            list(History2.objects().as_pymongo())

        connect('db1', alias='db1')
        connect('db2', alias='db2')

        self.assertEqual(list(History1.objects().as_pymongo()),
                         [{'_id': h.id, 'name': 'default'}])
        self.assertEqual(list(History2.objects().as_pymongo()),
                         [{'_id': h1.id, 'name': 'db1'}])

    def test_connect_different_documents_to_different_database(self):
        class History(Document):
            name = StringField()

        class History1(Document):
            name = StringField()
            meta = {'db_alias': 'db1'}

        class History2(Document):
            name = StringField()
            meta = {'db_alias': 'db2'}

        connect()
        connect('db1', alias='db1')
        connect('db2', alias='db2')

        History.drop_collection()
        History1.drop_collection()
        History2.drop_collection()

        h = History(name='default').save()
        h1 = History1(name='db1').save()
        h2 = History2(name='db2').save()

        self.assertEqual(History._collection.database.name, DEFAULT_DATABASE_NAME)
        self.assertEqual(History1._collection.database.name, 'db1')
        self.assertEqual(History2._collection.database.name, 'db2')

        self.assertEqual(list(History.objects().as_pymongo()),
                         [{'_id': h.id, 'name': 'default'}])
        self.assertEqual(list(History1.objects().as_pymongo()),
                         [{'_id': h1.id, 'name': 'db1'}])
        self.assertEqual(list(History2.objects().as_pymongo()),
                         [{'_id': h2.id, 'name': 'db2'}])

    def test_connect_fails_if_connect_2_times_with_default_alias(self):
        connect('mongoenginetest')

        with self.assertRaises(MongoEngineConnectionError) as ctx_err:
            connect('mongoenginetest2')
        self.assertEqual("A different connection with alias `default` was already registered. Use disconnect() first", str(ctx_err.exception))

    def test_connect_fails_if_connect_2_times_with_custom_alias(self):
        connect('mongoenginetest', alias='alias1')

        with self.assertRaises(MongoEngineConnectionError) as ctx_err:
            connect('mongoenginetest2', alias='alias1')

        self.assertEqual("A different connection with alias `alias1` was already registered. Use disconnect() first", str(ctx_err.exception))

    def test_connect_fails_if_similar_connection_settings_arent_defined_the_same_way(self):
        """Intended to keep the detecton function simple but robust"""
        db_name = 'mongoenginetest'
        db_alias = 'alias1'
        connect(db=db_name, alias=db_alias, host='localhost', port=27017)

        with self.assertRaises(MongoEngineConnectionError):
            connect(host='mongodb://localhost:27017/%s' % db_name, alias=db_alias)

    def test_connect_passes_silently_connect_multiple_times_with_same_config(self):
        # test default connection to `test`
        connect()
        connect()
        self.assertEqual(len(mongoengine.connection._connections), 1)
        connect('test01', alias='test01')
        connect('test01', alias='test01')
        self.assertEqual(len(mongoengine.connection._connections), 2)
        connect(host='mongodb://localhost:27017/mongoenginetest02', alias='test02')
        connect(host='mongodb://localhost:27017/mongoenginetest02', alias='test02')
        self.assertEqual(len(mongoengine.connection._connections), 3)

    def test_connect_with_invalid_db_name(self):
        """Ensure that connect() method fails fast if db name is invalid
        """
        with self.assertRaises(InvalidName):
            connect('mongomock://localhost')

    def test_connect_with_db_name_external(self):
        """Ensure that connect() works if db name is $external
        """
        """Ensure that the connect() method works properly."""
        connect('$external')

        conn = get_connection()
        self.assertIsInstance(conn, pymongo.mongo_client.MongoClient)

        db = get_db()
        self.assertIsInstance(db, pymongo.database.Database)
        self.assertEqual(db.name, '$external')

        connect('$external', alias='testdb')
        conn = get_connection('testdb')
        self.assertIsInstance(conn, pymongo.mongo_client.MongoClient)

    def test_connect_with_invalid_db_name_type(self):
        """Ensure that connect() method fails fast if db name has invalid type
        """
        with self.assertRaises(TypeError):
            non_string_db_name = ['e. g. list instead of a string']
            connect(non_string_db_name)

    def test_connect_in_mocking(self):
        """Ensure that the connect() method works properly in mocking.
        """
        try:
            import mongomock
        except ImportError:
            raise SkipTest('you need mongomock installed to run this testcase')

        connect('mongoenginetest', host='mongomock://localhost')
        conn = get_connection()
        self.assertIsInstance(conn, mongomock.MongoClient)

        connect('mongoenginetest2', host='mongomock://localhost', alias='testdb2')
        conn = get_connection('testdb2')
        self.assertIsInstance(conn, mongomock.MongoClient)

        connect('mongoenginetest3', host='mongodb://localhost', is_mock=True, alias='testdb3')
        conn = get_connection('testdb3')
        self.assertIsInstance(conn, mongomock.MongoClient)

        connect('mongoenginetest4', is_mock=True, alias='testdb4')
        conn = get_connection('testdb4')
        self.assertIsInstance(conn, mongomock.MongoClient)

        connect(host='mongodb://localhost:27017/mongoenginetest5', is_mock=True, alias='testdb5')
        conn = get_connection('testdb5')
        self.assertIsInstance(conn, mongomock.MongoClient)

        connect(host='mongomock://localhost:27017/mongoenginetest6', alias='testdb6')
        conn = get_connection('testdb6')
        self.assertIsInstance(conn, mongomock.MongoClient)

        connect(host='mongomock://localhost:27017/mongoenginetest7', is_mock=True, alias='testdb7')
        conn = get_connection('testdb7')
        self.assertIsInstance(conn, mongomock.MongoClient)

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
        self.assertIsInstance(conn, mongomock.MongoClient)

        connect(host=['mongodb://localhost'], is_mock=True, alias='testdb2')
        conn = get_connection('testdb2')
        self.assertIsInstance(conn, mongomock.MongoClient)

        connect(host=['localhost'], is_mock=True, alias='testdb3')
        conn = get_connection('testdb3')
        self.assertIsInstance(conn, mongomock.MongoClient)

        connect(host=['mongomock://localhost:27017', 'mongomock://localhost:27018'], alias='testdb4')
        conn = get_connection('testdb4')
        self.assertIsInstance(conn, mongomock.MongoClient)

        connect(host=['mongodb://localhost:27017', 'mongodb://localhost:27018'], is_mock=True, alias='testdb5')
        conn = get_connection('testdb5')
        self.assertIsInstance(conn, mongomock.MongoClient)

        connect(host=['localhost:27017', 'localhost:27018'], is_mock=True, alias='testdb6')
        conn = get_connection('testdb6')
        self.assertIsInstance(conn, mongomock.MongoClient)

    def test_disconnect_cleans_globals(self):
        """Ensure that the disconnect() method cleans the globals objects"""
        connections = mongoengine.connection._connections
        dbs = mongoengine.connection._dbs
        connection_settings = mongoengine.connection._connection_settings

        connect('mongoenginetest')

        self.assertEqual(len(connections), 1)
        self.assertEqual(len(dbs), 0)
        self.assertEqual(len(connection_settings), 1)

        class TestDoc(Document):
            pass

        TestDoc.drop_collection()  # triggers the db
        self.assertEqual(len(dbs), 1)

        disconnect()
        self.assertEqual(len(connections), 0)
        self.assertEqual(len(dbs), 0)
        self.assertEqual(len(connection_settings), 0)

    def test_disconnect_cleans_cached_collection_attribute_in_document(self):
        """Ensure that the disconnect() method works properly"""
        conn1 = connect('mongoenginetest')

        class History(Document):
            pass

        self.assertIsNone(History._collection)

        History.drop_collection()

        History.objects.first()     # will trigger the caching of _collection attribute
        self.assertIsNotNone(History._collection)

        disconnect()

        self.assertIsNone(History._collection)

        with self.assertRaises(MongoEngineConnectionError) as ctx_err:
            History.objects.first()
        self.assertEqual("You have not defined a default connection", str(ctx_err.exception))

    def test_connect_disconnect_works_on_same_document(self):
        """Ensure that the connect/disconnect works properly with a single Document"""
        db1 = 'db1'
        db2 = 'db2'

        # Ensure freshness of the 2 databases through pymongo
        client = MongoClient('localhost', 27017)
        client.drop_database(db1)
        client.drop_database(db2)

        # Save in db1
        connect(db1)

        class User(Document):
            name = StringField(required=True)

        user1 = User(name='John is in db1').save()
        disconnect()

        # Make sure save doesnt work at this stage
        with self.assertRaises(MongoEngineConnectionError):
            User(name='Wont work').save()

        # Save in db2
        connect(db2)
        user2 = User(name='Bob is in db2').save()
        disconnect()

        db1_users = list(client[db1].user.find())
        self.assertEqual(db1_users, [{'_id': user1.id, 'name': 'John is in db1'}])
        db2_users = list(client[db2].user.find())
        self.assertEqual(db2_users, [{'_id': user2.id, 'name': 'Bob is in db2'}])

    def test_disconnect_silently_pass_if_alias_does_not_exist(self):
        connections = mongoengine.connection._connections
        self.assertEqual(len(connections), 0)
        disconnect(alias='not_exist')

    def test_disconnect_all(self):
        connections = mongoengine.connection._connections
        dbs = mongoengine.connection._dbs
        connection_settings = mongoengine.connection._connection_settings

        connect('mongoenginetest')
        connect('mongoenginetest2', alias='db1')

        class History(Document):
            pass

        class History1(Document):
            name = StringField()
            meta = {'db_alias': 'db1'}

        History.drop_collection()   # will trigger the caching of _collection attribute
        History.objects.first()
        History1.drop_collection()
        History1.objects.first()

        self.assertIsNotNone(History._collection)
        self.assertIsNotNone(History1._collection)

        self.assertEqual(len(connections), 2)
        self.assertEqual(len(dbs), 2)
        self.assertEqual(len(connection_settings), 2)

        disconnect_all()

        self.assertIsNone(History._collection)
        self.assertIsNone(History1._collection)

        self.assertEqual(len(connections), 0)
        self.assertEqual(len(dbs), 0)
        self.assertEqual(len(connection_settings), 0)

        with self.assertRaises(MongoEngineConnectionError):
            History.objects.first()

        with self.assertRaises(MongoEngineConnectionError):
            History1.objects.first()

    def test_disconnect_all_silently_pass_if_no_connection_exist(self):
        disconnect_all()

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
        c.admin.system.users.delete_many({})
        c.mongoenginetest.system.users.delete_many({})

        c.admin.command("createUser", "admin", pwd="password", roles=["root"])
        c.admin.authenticate("admin", "password")
        c.admin.command("createUser", "username", pwd="password", roles=["dbOwner"])

        if not IS_PYMONGO_3:
            self.assertRaises(
                MongoEngineConnectionError, connect, 'testdb_uri_bad',
                host='mongodb://test:password@localhost'
            )

        connect("testdb_uri", host='mongodb://username:password@localhost/mongoenginetest')

        conn = get_connection()
        self.assertIsInstance(conn, pymongo.mongo_client.MongoClient)

        db = get_db()
        self.assertIsInstance(db, pymongo.database.Database)
        self.assertEqual(db.name, 'mongoenginetest')

        c.admin.system.users.delete_many({})
        c.mongoenginetest.system.users.delete_many({})

    def test_connect_uri_without_db(self):
        """Ensure connect() method works properly if the URI doesn't
        include a database name.
        """
        connect("mongoenginetest", host='mongodb://localhost/')

        conn = get_connection()
        self.assertIsInstance(conn, pymongo.mongo_client.MongoClient)

        db = get_db()
        self.assertIsInstance(db, pymongo.database.Database)
        self.assertEqual(db.name, 'mongoenginetest')

    def test_connect_uri_default_db(self):
        """Ensure connect() defaults to the right database name if
        the URI and the database_name don't explicitly specify it.
        """
        connect(host='mongodb://localhost/')

        conn = get_connection()
        self.assertIsInstance(conn, pymongo.mongo_client.MongoClient)

        db = get_db()
        self.assertIsInstance(db, pymongo.database.Database)
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

        c.admin.system.users.delete_many({})
        c.admin.command("createUser", "username2", pwd="password", roles=["dbOwner"])

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
        self.assertIsInstance(db, pymongo.database.Database)
        self.assertEqual(db.name, 'mongoenginetest')

        # Clear all users
        authd_conn.admin.system.users.delete_many({})

    def test_register_connection(self):
        """Ensure that connections with different aliases may be registered.
        """
        register_connection('testdb', 'mongoenginetest2')

        self.assertRaises(MongoEngineConnectionError, get_connection)
        conn = get_connection('testdb')
        self.assertIsInstance(conn, pymongo.mongo_client.MongoClient)

        db = get_db('testdb')
        self.assertIsInstance(db, pymongo.database.Database)
        self.assertEqual(db.name, 'mongoenginetest2')

    def test_register_connection_defaults(self):
        """Ensure that defaults are used when the host and port are None.
        """
        register_connection('testdb', 'mongoenginetest', host=None, port=None)

        conn = get_connection('testdb')
        self.assertIsInstance(conn, pymongo.mongo_client.MongoClient)

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
        pool_size_kwargs = {'maxpoolsize': 100}

        conn = connect('mongoenginetest', alias='max_pool_size_via_kwarg', **pool_size_kwargs)
        self.assertEqual(conn.max_pool_size, 100)

    def test_connection_pool_via_uri(self):
        """Ensure we can specify a max connection pool size using
        an option in a connection URI.
        """
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
            self.assertIsInstance(db, pymongo.database.Database)
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
            self.assertIsInstance(db, pymongo.database.Database)
            self.assertEqual(db.name, 'test')
        else:
            # PyMongo < v3.x raises an exception:
            # "localhost:27017 is not a member of replica set local-rs"
            with self.assertRaises(MongoEngineConnectionError):
                c = connect(replicaset='local-rs')

    def test_connect_tz_aware(self):
        connect('mongoenginetest', tz_aware=True)
        d = datetime.datetime(2010, 5, 5, tzinfo=utc)

        class DateDoc(Document):
            the_date = DateTimeField(required=True)

        DateDoc.drop_collection()
        DateDoc(the_date=d).save()

        date_doc = DateDoc.objects.first()
        self.assertEqual(d, date_doc.the_date)

    def test_read_preference_from_parse(self):
        if IS_PYMONGO_3:
            from pymongo import ReadPreference
            conn = connect(host="mongodb://a1.vpc,a2.vpc,a3.vpc/prod?readPreference=secondaryPreferred")
            self.assertEqual(conn.read_preference, ReadPreference.SECONDARY_PREFERRED)

    def test_multiple_connection_settings(self):
        connect('mongoenginetest', alias='t1', host="localhost")

        connect('mongoenginetest2', alias='t2', host="127.0.0.1")

        mongo_connections = mongoengine.connection._connections
        self.assertEqual(len(mongo_connections.items()), 2)
        self.assertIn('t1', mongo_connections.keys())
        self.assertIn('t2', mongo_connections.keys())
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
