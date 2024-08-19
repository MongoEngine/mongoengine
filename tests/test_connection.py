import datetime
import random
import threading
import time
import unittest
import uuid

import pymongo
import pytest
from bson.tz_util import utc
from pymongo import MongoClient, ReadPreference
from pymongo.errors import (
    InvalidName,
    InvalidOperation,
    OperationFailure,
)
from pymongo.read_preferences import Secondary

import mongoengine.connection
from mongoengine import (
    DateTimeField,
    Document,
    IntField,
    StringField,
    connect,
    disconnect_all,
    register_connection,
    run_in_transaction,
)
from mongoengine.connection import (
    DEFAULT_DATABASE_NAME,
    ConnectionFailure,
    _get_connection_settings,
    _get_session,
    disconnect,
    get_connection,
    get_db,
)
from mongoengine.context_managers import switch_db
from mongoengine.errors import OperationError
from mongoengine.pymongo_support import PYMONGO_VERSION

from .utils import (
    MongoDBTestCase,
    requires_mongodb_gte_40,
    requires_mongodb_gte_44,
)


def random_str():
    return str(uuid.uuid4())


def get_tz_awareness(connection):
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
        connect("mongoenginetest")

        conn = get_connection()
        assert isinstance(conn, pymongo.MongoClient)

        db = get_db()
        assert isinstance(db, pymongo.database.Database)
        assert db.name == "mongoenginetest"

        connect("mongoenginetest2", alias="testdb")
        conn = get_connection("testdb")
        assert isinstance(conn, pymongo.MongoClient)

        connect(
            "mongoenginetest2", alias="testdb3", mongo_client_class=pymongo.MongoClient
        )
        conn = get_connection("testdb")
        assert isinstance(conn, pymongo.MongoClient)

    def test_connect_disconnect_works_properly(self):
        class History1(Document):
            name = StringField()
            meta = {"db_alias": "db1"}

        class History2(Document):
            name = StringField()
            meta = {"db_alias": "db2"}

        connect("db1", alias="db1")
        connect("db2", alias="db2")

        History1.drop_collection()
        History2.drop_collection()

        h = History1(name="default").save()
        h1 = History2(name="db1").save()

        assert list(History1.objects().as_pymongo()) == [
            {"_id": h.id, "name": "default"}
        ]
        assert list(History2.objects().as_pymongo()) == [{"_id": h1.id, "name": "db1"}]

        disconnect("db1")
        disconnect("db2")

        with pytest.raises(ConnectionFailure):
            list(History1.objects().as_pymongo())

        with pytest.raises(ConnectionFailure):
            list(History2.objects().as_pymongo())

        connect("db1", alias="db1")
        connect("db2", alias="db2")

        assert list(History1.objects().as_pymongo()) == [
            {"_id": h.id, "name": "default"}
        ]
        assert list(History2.objects().as_pymongo()) == [{"_id": h1.id, "name": "db1"}]

    def test_connect_different_documents_to_different_database(self):
        class History(Document):
            name = StringField()

        class History1(Document):
            name = StringField()
            meta = {"db_alias": "db1"}

        class History2(Document):
            name = StringField()
            meta = {"db_alias": "db2"}

        connect()
        connect("db1", alias="db1")
        connect("db2", alias="db2")

        History.drop_collection()
        History1.drop_collection()
        History2.drop_collection()

        h = History(name="default").save()
        h1 = History1(name="db1").save()
        h2 = History2(name="db2").save()

        assert History._collection.database.name == DEFAULT_DATABASE_NAME
        assert History1._collection.database.name == "db1"
        assert History2._collection.database.name == "db2"

        assert list(History.objects().as_pymongo()) == [
            {"_id": h.id, "name": "default"}
        ]
        assert list(History1.objects().as_pymongo()) == [{"_id": h1.id, "name": "db1"}]
        assert list(History2.objects().as_pymongo()) == [{"_id": h2.id, "name": "db2"}]

    def test_connect_fails_if_connect_2_times_with_default_alias(self):
        connect("mongoenginetest")

        with pytest.raises(ConnectionFailure) as exc_info:
            connect("mongoenginetest2")
        assert (
            "A different connection with alias `default` was already registered. Use disconnect() first"
            == str(exc_info.value)
        )

    def test_connect_fails_if_connect_2_times_with_custom_alias(self):
        connect("mongoenginetest", alias="alias1")

        with pytest.raises(ConnectionFailure) as exc_info:
            connect("mongoenginetest2", alias="alias1")

        assert (
            "A different connection with alias `alias1` was already registered. Use disconnect() first"
            == str(exc_info.value)
        )

    def test_connect_fails_if_similar_connection_settings_arent_defined_the_same_way(
        self,
    ):
        """Intended to keep the detecton function simple but robust"""
        db_name = "mongoenginetest"
        db_alias = "alias1"
        connect(db=db_name, alias=db_alias, host="localhost", port=27017)

        with pytest.raises(ConnectionFailure):
            connect(host="mongodb://localhost:27017/%s" % db_name, alias=db_alias)

    def test___get_connection_settings(self):
        funky_host = "mongodb://root:12345678@1.1.1.1:27017,2.2.2.2:27017,3.3.3.3:27017/db_api?replicaSet=s0&readPreference=secondary&uuidRepresentation=javaLegacy&readPreferenceTags=region:us-west-2,usage:api"
        settings = _get_connection_settings(host=funky_host)

        if PYMONGO_VERSION < (4,):
            read_pref = Secondary(
                tag_sets=[{"region": "us-west-2", "usage": "api"}],
                max_staleness=-1,
            )
        else:
            read_pref = Secondary(
                tag_sets=[{"region": "us-west-2", "usage": "api"}],
                max_staleness=-1,
                hedge=None,
            )
        assert settings == {
            "authentication_mechanism": None,
            "authentication_source": None,
            "authmechanismproperties": None,
            "host": [funky_host],
            "name": "db_api",
            "password": "12345678",
            "port": 27017,
            "read_preference": read_pref,
            "replicaSet": "s0",
            "username": "root",
            "uuidrepresentation": "javaLegacy",
        }

    def test_connect_passes_silently_connect_multiple_times_with_same_config(self):
        # test default connection to `test`
        connect()
        connect()
        assert len(mongoengine.connection._connections) == 1
        connect("test01", alias="test01")
        connect("test01", alias="test01")
        assert len(mongoengine.connection._connections) == 2
        connect(host="mongodb://localhost:27017/mongoenginetest02", alias="test02")
        connect(host="mongodb://localhost:27017/mongoenginetest02", alias="test02")
        assert len(mongoengine.connection._connections) == 3

    def test_connect_with_invalid_db_name(self):
        """Ensure that connect() method fails fast if db name is invalid"""
        with pytest.raises(InvalidName):
            connect("mongodb://localhost")

    def test_connect_with_db_name_external(self):
        """Ensure that connect() works if db name is $external"""
        """Ensure that the connect() method works properly."""
        connect("$external")

        conn = get_connection()
        assert isinstance(conn, pymongo.mongo_client.MongoClient)

        db = get_db()
        assert isinstance(db, pymongo.database.Database)
        assert db.name == "$external"

        connect("$external", alias="testdb")
        conn = get_connection("testdb")
        assert isinstance(conn, pymongo.mongo_client.MongoClient)

    def test_connect_with_invalid_db_name_type(self):
        """Ensure that connect() method fails fast if db name has invalid type"""
        with pytest.raises(TypeError):
            non_string_db_name = ["e. g. list instead of a string"]
            connect(non_string_db_name)

    def test_disconnect_cleans_globals(self):
        """Ensure that the disconnect() method cleans the globals objects"""
        connections = mongoengine.connection._connections
        dbs = mongoengine.connection._dbs
        connection_settings = mongoengine.connection._connection_settings

        connect("mongoenginetest")

        assert len(connections) == 1
        assert len(dbs) == 0
        assert len(connection_settings) == 1

        class TestDoc(Document):
            pass

        TestDoc.drop_collection()  # triggers the db
        assert len(dbs) == 1

        disconnect()
        assert len(connections) == 0
        assert len(dbs) == 0
        assert len(connection_settings) == 0

    def test_disconnect_cleans_cached_collection_attribute_in_document(self):
        """Ensure that the disconnect() method works properly"""
        connect("mongoenginetest")

        class History(Document):
            pass

        assert History._collection is None

        History.drop_collection()

        History.objects.first()  # will trigger the caching of _collection attribute
        assert History._collection is not None

        disconnect()

        assert History._collection is None

        with pytest.raises(ConnectionFailure) as exc_info:
            History.objects.first()
        assert "You have not defined a default connection" == str(exc_info.value)

    def test_connect_disconnect_works_on_same_document(self):
        """Ensure that the connect/disconnect works properly with a single Document"""
        db1 = "db1"
        db2 = "db2"

        # Ensure freshness of the 2 databases through pymongo
        client = MongoClient("localhost", 27017)
        client.drop_database(db1)
        client.drop_database(db2)

        # Save in db1
        connect(db1)

        class User(Document):
            name = StringField(required=True)

        user1 = User(name="John is in db1").save()
        disconnect()

        # Make sure save doesnt work at this stage
        with pytest.raises(ConnectionFailure):
            User(name="Wont work").save()

        # Save in db2
        connect(db2)
        user2 = User(name="Bob is in db2").save()
        disconnect()

        db1_users = list(client[db1].user.find())
        assert db1_users == [{"_id": user1.id, "name": "John is in db1"}]
        db2_users = list(client[db2].user.find())
        assert db2_users == [{"_id": user2.id, "name": "Bob is in db2"}]

    def test_disconnect_silently_pass_if_alias_does_not_exist(self):
        connections = mongoengine.connection._connections
        assert len(connections) == 0
        disconnect(alias="not_exist")

    def test_disconnect_does_not_close_client_used_by_another_alias(self):
        client1 = connect(alias="disconnect_reused_client_test_1")
        client2 = connect(alias="disconnect_reused_client_test_2")
        client3 = connect(alias="disconnect_reused_client_test_3", maxPoolSize=10)
        assert client1 is client2
        assert client1 is not client3
        client1.admin.command("ping")
        disconnect("disconnect_reused_client_test_1")
        # The client is not closed because the second alias still exists.
        client2.admin.command("ping")
        disconnect("disconnect_reused_client_test_2")
        # The client is now closed:
        if PYMONGO_VERSION >= (4,):
            with pytest.raises(InvalidOperation):
                client2.admin.command("ping")
        # 3rd client connected to the same cluster with different options
        # is not closed either.
        client3.admin.command("ping")
        disconnect("disconnect_reused_client_test_3")
        # 3rd client is now closed:
        if PYMONGO_VERSION >= (4,):
            with pytest.raises(InvalidOperation):
                client3.admin.command("ping")

    def test_disconnect_all(self):
        connections = mongoengine.connection._connections
        dbs = mongoengine.connection._dbs
        connection_settings = mongoengine.connection._connection_settings

        connect("mongoenginetest")
        connect("mongoenginetest2", alias="db1")

        class History(Document):
            pass

        class History1(Document):
            name = StringField()
            meta = {"db_alias": "db1"}

        History.drop_collection()  # will trigger the caching of _collection attribute
        History.objects.first()
        History1.drop_collection()
        History1.objects.first()

        assert History._collection is not None
        assert History1._collection is not None

        assert len(connections) == 2
        assert len(dbs) == 2
        assert len(connection_settings) == 2

        disconnect_all()

        assert History._collection is None
        assert History1._collection is None

        assert len(connections) == 0
        assert len(dbs) == 0
        assert len(connection_settings) == 0

        with pytest.raises(ConnectionFailure):
            History.objects.first()

        with pytest.raises(ConnectionFailure):
            History1.objects.first()

    def test_disconnect_all_silently_pass_if_no_connection_exist(self):
        disconnect_all()

    def test_sharing_connections(self):
        """Ensure that connections are shared when the connection settings are exactly the same"""
        connect("mongoenginetests", alias="testdb1")
        expected_connection = get_connection("testdb1")

        connect("mongoenginetests", alias="testdb2")
        actual_connection = get_connection("testdb2")

        expected_connection.server_info()

        assert expected_connection == actual_connection

    def test_connect_uri(self):
        """Ensure that the connect() method works properly with URIs."""
        c = connect(db="mongoenginetest", alias="admin")
        c.admin.system.users.delete_many({})
        c.mongoenginetest.system.users.delete_many({})

        c.admin.command("createUser", "admin", pwd="password", roles=["root"])

        adminadmin_settings = mongoengine.connection._connection_settings[
            "adminadmin"
        ] = mongoengine.connection._connection_settings["admin"].copy()
        adminadmin_settings["username"] = "admin"
        adminadmin_settings["password"] = "password"
        ca = connect(db="mongoenginetest", alias="adminadmin")
        ca.admin.command("createUser", "username", pwd="password", roles=["dbOwner"])

        connect(
            "testdb_uri", host="mongodb://username:password@localhost/mongoenginetest"
        )

        conn = get_connection()
        assert isinstance(conn, pymongo.mongo_client.MongoClient)

        db = get_db()
        assert isinstance(db, pymongo.database.Database)
        assert db.name == "mongoenginetest"

        c.admin.system.users.delete_many({})
        c.mongoenginetest.system.users.delete_many({})

    def test_connect_uri_without_db(self):
        """Ensure connect() method works properly if the URI doesn't
        include a database name.
        """
        connect("mongoenginetest", host="mongodb://localhost/")

        conn = get_connection()
        assert isinstance(conn, pymongo.mongo_client.MongoClient)

        db = get_db()
        assert isinstance(db, pymongo.database.Database)
        assert db.name == "mongoenginetest"

    def test_connect_uri_default_db(self):
        """Ensure connect() defaults to the right database name if
        the URI and the database_name don't explicitly specify it.
        """
        connect(host="mongodb://localhost/")

        conn = get_connection()
        assert isinstance(conn, pymongo.mongo_client.MongoClient)

        db = get_db()
        assert isinstance(db, pymongo.database.Database)
        assert db.name == "test"

    def test_uri_without_credentials_doesnt_override_conn_settings(self):
        """Ensure connect() uses the username & password params if the URI
        doesn't explicitly specify them.
        """
        connect(
            host="mongodb://localhost/mongoenginetest", username="user", password="pass"
        )

        # OperationFailure means that mongoengine attempted authentication
        # w/ the provided username/password and failed - that's the desired
        # behavior. If the MongoDB URI would override the credentials
        if PYMONGO_VERSION >= (4,):
            with pytest.raises(OperationFailure):
                db = get_db()
                # pymongo 4.x does not call db.authenticate and needs to perform an operation to trigger the failure
                db.list_collection_names()
        else:
            with pytest.raises(OperationFailure):
                get_db()

    def test_connect_uri_with_authsource(self):
        """Ensure that the connect() method works well with `authSource`
        option in the URI.
        """
        # Create users
        c = connect("mongoenginetest")

        c.admin.system.users.delete_many({})
        c.admin.command("createUser", "username2", pwd="password", roles=["dbOwner"])

        # Authentication fails without "authSource"
        test_conn = connect(
            "mongoenginetest",
            alias="test1",
            host="mongodb://username2:password@localhost/mongoenginetest",
        )
        with pytest.raises(OperationFailure):
            test_conn.server_info()

        # Authentication succeeds with "authSource"
        authd_conn = connect(
            "mongoenginetest",
            alias="test2",
            host=(
                "mongodb://username2:password@localhost/"
                "mongoenginetest?authSource=admin"
            ),
        )
        db = get_db("test2")
        assert isinstance(db, pymongo.database.Database)
        assert db.name == "mongoenginetest"

        # Clear all users
        authd_conn.admin.system.users.delete_many({})

    def test_register_connection(self):
        """Ensure that connections with different aliases may be registered."""
        register_connection("testdb", "mongoenginetest2")

        with pytest.raises(ConnectionFailure):
            get_connection()
        conn = get_connection("testdb")
        assert isinstance(conn, pymongo.mongo_client.MongoClient)

        db = get_db("testdb")
        assert isinstance(db, pymongo.database.Database)
        assert db.name == "mongoenginetest2"

    def test_register_connection_defaults(self):
        """Ensure that defaults are used when the host and port are None."""
        register_connection("testdb", "mongoenginetest", host=None, port=None)

        conn = get_connection("testdb")
        assert isinstance(conn, pymongo.mongo_client.MongoClient)

    def test_connection_kwargs(self):
        """Ensure that connection kwargs get passed to pymongo."""
        connect("mongoenginetest", alias="t1", tz_aware=True)
        conn = get_connection("t1")

        assert get_tz_awareness(conn)

        connect("mongoenginetest2", alias="t2")
        conn = get_connection("t2")
        assert not get_tz_awareness(conn)

    def test_connection_pool_via_kwarg(self):
        """Ensure we can specify a max connection pool size using
        a connection kwarg.
        """
        pool_size_kwargs = {"maxpoolsize": 100}

        conn = connect(
            "mongoenginetest", alias="max_pool_size_via_kwarg", **pool_size_kwargs
        )
        if PYMONGO_VERSION >= (4,):
            assert conn.options.pool_options.max_pool_size == 100
        else:
            assert conn.max_pool_size == 100

    def test_connection_pool_via_uri(self):
        """Ensure we can specify a max connection pool size using
        an option in a connection URI.
        """
        conn = connect(
            host="mongodb://localhost/test?maxpoolsize=100",
            alias="max_pool_size_via_uri",
        )
        if PYMONGO_VERSION >= (4,):
            assert conn.options.pool_options.max_pool_size == 100
        else:
            assert conn.max_pool_size == 100

    def test_write_concern(self):
        """Ensure write concern can be specified in connect() via
        a kwarg or as part of the connection URI.
        """
        conn1 = connect(
            alias="conn1", host="mongodb://localhost/testing?w=1&journal=true"
        )
        conn2 = connect("testing", alias="conn2", w=1, journal=True)
        assert conn1.write_concern.document == {"w": 1, "j": True}
        assert conn2.write_concern.document == {"w": 1, "j": True}

    def test_connect_with_replicaset_via_uri(self):
        """Ensure connect() works when specifying a replicaSet via the
        MongoDB URI.
        """
        connect(host="mongodb://localhost/test?replicaSet=local-rs")
        db = get_db()
        assert isinstance(db, pymongo.database.Database)
        assert db.name == "test"

    def test_connect_with_replicaset_via_kwargs(self):
        """Ensure connect() works when specifying a replicaSet via the
        connection kwargs
        """
        c = connect(replicaset="local-rs")
        assert c._MongoClient__options.replica_set_name == "local-rs"
        db = get_db()
        assert isinstance(db, pymongo.database.Database)
        assert db.name == "test"

    def test_connect_tz_aware(self):
        connect("mongoenginetest", tz_aware=True)
        d = datetime.datetime(2010, 5, 5, tzinfo=utc)

        class DateDoc(Document):
            the_date = DateTimeField(required=True)

        DateDoc.drop_collection()
        DateDoc(the_date=d).save()

        date_doc = DateDoc.objects.first()
        assert d == date_doc.the_date

    def test_read_preference_from_parse(self):
        conn = connect(
            host="mongodb://a1.vpc,a2.vpc,a3.vpc/prod?readPreference=secondaryPreferred"
        )
        assert conn.read_preference == ReadPreference.SECONDARY_PREFERRED

    def test_multiple_connection_settings(self):
        connect("mongoenginetest", alias="t1", host="localhost")

        connect("mongoenginetest2", alias="t2", host="127.0.0.1")

        mongo_connections = mongoengine.connection._connections
        assert len(mongo_connections.items()) == 2
        assert "t1" in mongo_connections.keys()
        assert "t2" in mongo_connections.keys()

        # Handle PyMongo 3+ Async Connection
        # Ensure we are connected, throws ServerSelectionTimeoutError otherwise.
        # Purposely not catching exception to fail test if thrown.
        mongo_connections["t1"].server_info()
        mongo_connections["t2"].server_info()
        assert mongo_connections["t1"] is not mongo_connections["t2"]

    def test_connect_2_databases_uses_same_client_if_only_dbname_differs(self):
        c1 = connect(alias="testdb1", db="testdb1")
        c2 = connect(alias="testdb2", db="testdb2")
        assert c1 is c2

    def test_connect_2_databases_uses_different_client_if_different_parameters(self):
        c1 = connect(alias="testdb1", db="testdb1", username="u1", password="pass")
        c2 = connect(alias="testdb2", db="testdb2", username="u2", password="pass")
        assert c1 is not c2

    def test_connect_uri_uuidrepresentation_set_in_uri(self):
        rand = random_str()
        tmp_conn = connect(
            alias=rand,
            host=f"mongodb://localhost:27017/{rand}?uuidRepresentation=csharpLegacy",
        )
        assert (
            tmp_conn.options.codec_options.uuid_representation
            == pymongo.common._UUID_REPRESENTATIONS["csharpLegacy"]
        )
        disconnect(rand)

    def test_connect_uri_uuidrepresentation_set_as_arg(self):
        rand = random_str()
        tmp_conn = connect(alias=rand, db=rand, uuidRepresentation="javaLegacy")
        assert (
            tmp_conn.options.codec_options.uuid_representation
            == pymongo.common._UUID_REPRESENTATIONS["javaLegacy"]
        )
        disconnect(rand)

    def test_connect_uri_uuidrepresentation_set_both_arg_and_uri_arg_prevail(self):
        rand = random_str()
        tmp_conn = connect(
            alias=rand,
            host=f"mongodb://localhost:27017/{rand}?uuidRepresentation=csharpLegacy",
            uuidRepresentation="javaLegacy",
        )
        assert (
            tmp_conn.options.codec_options.uuid_representation
            == pymongo.common._UUID_REPRESENTATIONS["javaLegacy"]
        )
        disconnect(rand)

    def test_connect_uri_uuidrepresentation_default_to_pythonlegacy(self):
        # To be changed soon to unspecified
        rand = random_str()
        tmp_conn = connect(alias=rand, db=rand)
        assert (
            tmp_conn.options.codec_options.uuid_representation
            == pymongo.common._UUID_REPRESENTATIONS["pythonLegacy"]
        )
        disconnect(rand)

    def test_transaction_usage_raises_exception_when_using_pymong_lt_39(self):
        if PYMONGO_VERSION < (3, 9):
            with pytest.raises(OperationError):

                def cb(session):
                    pass

                run_in_transaction(cb)


class TransactionTest(MongoDBTestCase):
    @requires_mongodb_gte_40
    def test_updating_a_document_within_a_transaction(self):
        if PYMONGO_VERSION < (3, 9):
            pytest.skip("pymongo>=3.9 is required to use transactions")

        connect("mongoenginetest")

        class A(Document):
            name = StringField()

        A.drop_collection()

        a_doc = A.objects.create(name="a")

        def cb(session):
            a_doc.update(name="b")

        run_in_transaction(cb)
        assert "b" == A.objects.get(id=a_doc.id).name

    @requires_mongodb_gte_40
    def test_update_a_document_within_a_transaction_with_custom_args(self):
        if PYMONGO_VERSION < (3, 9):
            pytest.skip("pymongo>=3.9 is required to use transactions")

        connect("mongoenginetest")

        class A(Document):
            name = StringField()

        A.drop_collection()

        a_doc = A.objects.create(name="a")

        def cb(session, custom_arg, custom_kwarg=""):
            a_doc.update(name=f"{custom_arg}-{custom_kwarg}")

        run_in_transaction(lambda s: cb(s, "a", custom_kwarg="b"))
        assert "a-b" == A.objects.get(id=a_doc.id).name

    @requires_mongodb_gte_40
    def test_transaction_updates_across_databases(self):
        if PYMONGO_VERSION < (3, 9):
            pytest.skip("pymongo>=3.9 is required to use transactions")

        connect("mongoenginetest")
        connect("test2", "test2")

        class A(Document):
            name = StringField()

        A.objects.all().delete()
        a_doc = A.objects.create(name="a")

        class B(Document):
            meta = {"db_alias": "test2"}
            name = StringField()

        B.objects.all().delete()
        b_doc = B.objects.create(name="b")

        def cb(session):
            a_doc.update(name="a2")
            b_doc.update(name="b2")

        run_in_transaction(cb)

        assert "a2" == A.objects.get(id=a_doc.id).name
        assert "b2" == B.objects.get(id=b_doc.id).name

    @requires_mongodb_gte_44
    def test_collection_creation_via_upserts_across_databases_in_transaction(self):
        if PYMONGO_VERSION < (3, 9):
            pytest.skip("pymongo>=3.9 is required to use transactions")

        connect("mongoenginetest")
        connect("test2", "test2")

        class A(Document):
            name = StringField()

        A.drop_collection()

        a_doc = A.objects.create(name="a")

        class B(Document):
            meta = {"db_alias": "test2"}
            name = StringField()

        B.drop_collection()

        b_doc = B.objects.create(name="b")

        def cb(session):
            a_doc.update(name="a3")
            with switch_db(A, "test2"):
                a_doc.update(name="a4", upsert=True)
                b_doc.update(name="b3")
            b_doc.update(name="b4")

        run_in_transaction(cb)

        assert "a3" == A.objects.get(id=a_doc.id).name
        assert "b4" == B.objects.get(id=b_doc.id).name
        with switch_db(A, "test2"):
            assert "a4" == A.objects.get(id=a_doc.id).name

    @requires_mongodb_gte_40
    def test_an_exception_raised_in_transactions_across_databases_rolls_back_updates(
        self,
    ):
        if PYMONGO_VERSION < (3, 9):
            pytest.skip("pymongo>=3.9 is required to use transactions")

        connect("mongoenginetest")
        connect("test2", "test2")

        class A(Document):
            name = StringField()

        A.drop_collection()
        with switch_db(A, "test2"):
            A.drop_collection()

        a_doc = A.objects.create(name="a")

        class B(Document):
            meta = {"db_alias": "test2"}
            name = StringField()

        B.drop_collection()

        b_doc = B.objects.create(name="b")

        def cb(session):
            a_doc.update(name="a3")
            with switch_db(A, "test2"):
                a_doc.update(name="a4", upsert=True)
                b_doc.update(name="b3")
                b_doc.update(name="b4")
            raise Exception

        try:
            run_in_transaction(cb)
        except Exception:
            pass

        assert "a" == A.objects.get(id=a_doc.id).name
        assert "b" == B.objects.get(id=b_doc.id).name
        with switch_db(A, "test2"):
            assert 0 == A.objects.all().count()

    @requires_mongodb_gte_40
    def test_nested_transactions_create_and_release_sessions_accordingly(self):
        # NOTE: callbacks initiating new transactions goes against pymongo recommended usage!
        # This is simply a sanity check and should not be considered suggested usage.
        if PYMONGO_VERSION < (3, 9):
            pytest.skip("pymongo>=3.9 is required to use transactions")

        connect("mongoenginetest")

        def cb1(s1):
            def cb2(s2):
                assert s1 != s2
                assert s2 == _get_session()

            run_in_transaction(cb2)
            assert s1 == _get_session()

        run_in_transaction(cb1)
        assert _get_session() is None

    @requires_mongodb_gte_40
    def test_thread_safety_of_transactions(self):
        """
        Make sure transactions don't step over each other. Each
        session should be isolated to each thread. If this is the
        case, then no amount of runtime variability should have
        an effect on the output.

        This test sets up 10 records, each with an integer field
        of value 0 - 9.

        We then spin up 10 threads and attempt to update a target
        record by multiplying it's integer value by 10. Then, if
        the target record is even, throw an exception, which
        should then roll the transaction back. The odd rows always
        succeed.

        If the sessions are properly thread safe, we should ALWAYS
        net out with the following sum across the integer fields
        of the 10 records:

        0 + 10 + 2 + 30 + 4 + 50 + 6 + 70 + 8 + 90 = 270
        """
        if PYMONGO_VERSION < (3, 9):
            pytest.skip("pymongo>=3.9 is required to use transactions")

        connect("mongoenginetest")

        class A(Document):
            i = IntField()

        # Ensure the collection is created
        A.objects.create(i=0)

        class TestExc(Exception):
            pass

        def thread_fn(idx):
            # Open the transaction at some unknown interval
            time.sleep(random.uniform(0.01, 0.1))
            try:

                def cb(session):
                    a = A.objects.get(i=idx)
                    a.i = idx * 10
                    # Save at some unknown interval
                    time.sleep(random.uniform(0.01, 0.1))
                    a.save()

                    # Force roll backs for the even runs...
                    if idx % 2 == 0:
                        raise TestExc

                run_in_transaction(cb)
            except TestExc:
                pass

        for r in range(10):
            """
            Threads & randomization are tricky - run it multiple times
            """

            # Clear out the collection for a fresh run
            A.objects.all().delete()

            # Prepopulate the data for reads
            thread_count = 10
            for i in range(thread_count):
                A.objects.create(i=i)

            # Prime the threads
            threads = [
                threading.Thread(target=thread_fn, args=(i,))
                for i in range(thread_count)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            # Check the sum
            expected_sum = 0
            for i in range(thread_count):
                if i % 2 == 0:
                    expected_sum += i
                else:
                    expected_sum += i * 10
            assert expected_sum == 270
            assert expected_sum == sum(a.i for a in A.objects.all())


if __name__ == "__main__":
    unittest.main()
