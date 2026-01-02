import datetime
import unittest
import uuid

import pymongo
import pymongo.database
import pymongo.mongo_client
import pytest
from bson import UuidRepresentation
from bson.tz_util import utc
from pymongo import ReadPreference, AsyncMongoClient
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.errors import (
    InvalidName,
    InvalidOperation,
    OperationFailure,
)

from mongoengine import (
    DateTimeField,
    StringField,
)
from pymongo.errors import ConnectionFailure
from mongoengine.asynchronous import async_connect, async_disconnect, async_disconnect_all, async_get_connection, \
    async_get_db, async_register_connection, connection
from mongoengine.asynchronous.connection import DEFAULT_DATABASE_NAME
from mongoengine.document import Document
from mongoengine.pymongo_support import PYMONGO_VERSION
from mongoengine.registry import _CollectionRegistry
from tests.asynchronous.utils import reset_async_connections
from tests.utils import MONGO_TEST_DB


def random_str():
    return str(uuid.uuid4())


def get_tz_awareness(connection_):
    return connection_.codec_options.tz_aware


class AsyncConnectionTest(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        await async_disconnect_all()

    async def asyncTearDown(self):
        await async_disconnect_all()
        await reset_async_connections()
        _CollectionRegistry.clear()

    @pytest.mark.asyncio
    async def test_async_connect(self):
        """Ensure that the connect() method works properly."""
        await async_connect(MONGO_TEST_DB)

        conn = await async_get_connection()
        assert isinstance(conn, pymongo.AsyncMongoClient)

        db = await async_get_db()
        assert isinstance(db, AsyncDatabase)
        assert db.name == MONGO_TEST_DB

        await async_connect(f"{MONGO_TEST_DB}_2", alias="testdb")
        conn = await async_get_connection("testdb")
        assert isinstance(conn, pymongo.AsyncMongoClient)

        await async_connect(
            f"{MONGO_TEST_DB}_2", alias="testdb3", mongo_client_class=pymongo.AsyncMongoClient
        )
        conn = await async_get_connection("testdb")
        assert isinstance(conn, pymongo.AsyncMongoClient)

    @pytest.mark.asyncio
    async def test_async_connect_disconnect_works_properly(self):
        class History1(Document):
            name = StringField()
            meta = {"db_alias": "db1"}

        class History2(Document):
            name = StringField()
            meta = {"db_alias": "db2"}

        await async_connect(f"{MONGO_TEST_DB}_db1", alias="db1")
        await async_connect(f"{MONGO_TEST_DB}_db2", alias="db2")

        await History1.adrop_collection()
        await History2.adrop_collection()

        h = await History1(name="default").asave()
        h1 = await History2(name="db1").asave()

        assert await History1.aobjects().as_pymongo().to_list() == [
            {"_id": h.id, "name": "default"}
        ]
        assert await History2.aobjects().as_pymongo().to_list() == [{"_id": h1.id, "name": "db1"}]

        await async_disconnect("db1")
        await async_disconnect("db2")

        with pytest.raises(ConnectionFailure):
            await History1.aobjects().as_pymongo().to_list()

        with pytest.raises(ConnectionFailure):
            await History2.aobjects().as_pymongo().to_list()

        await async_connect(f"{MONGO_TEST_DB}_db1", alias="db1")
        await async_connect(f"{MONGO_TEST_DB}_db2", alias="db2")

        assert await History1.aobjects().as_pymongo().to_list() == [
            {"_id": h.id, "name": "default"}
        ]
        assert await History2.aobjects().as_pymongo().to_list() == [{"_id": h1.id, "name": "db1"}]

    @pytest.mark.asyncio
    async def test_async_connect_different_documents_to_different_database(self):
        class History(Document):
            name = StringField()

        class History1(Document):
            name = StringField()
            meta = {"db_alias": "db1"}

        class History2(Document):
            name = StringField()
            meta = {"db_alias": "db2"}

        await async_connect(MONGO_TEST_DB)
        await async_connect(f"{MONGO_TEST_DB}_db1", alias="db1")
        await async_connect(f"{MONGO_TEST_DB}_db2", alias="db2")

        await History.adrop_collection()
        await History1.adrop_collection()
        await History2.adrop_collection()

        h = await History(name="default").asave()
        h1 = await History1(name="db1").asave()
        h2 = await History2(name="db2").asave()

        assert (await History._aget_collection()).database.name == MONGO_TEST_DB
        assert (await History1._aget_collection()).database.name == f"{MONGO_TEST_DB}_db1"
        assert (await History2._aget_collection()).database.name == f"{MONGO_TEST_DB}_db2"

        assert await History.aobjects().as_pymongo().to_list() == [
            {"_id": h.id, "name": "default"}
        ]
        assert await History1.aobjects().as_pymongo().to_list() == [{"_id": h1.id, "name": "db1"}]
        assert await History2.aobjects().as_pymongo().to_list() == [{"_id": h2.id, "name": "db2"}]

    @pytest.mark.asyncio
    async def test_async_connect_fails_if_connect_2_times_with_default_alias(self):
        await async_connect(MONGO_TEST_DB)

        with pytest.raises(ConnectionFailure) as exc_info:
            await async_connect(f"{MONGO_TEST_DB}_2")
        assert (
                "A different connection with alias `default` was already registered. Use async_disconnect() first"
                == str(exc_info.value)
        )

    @pytest.mark.asyncio
    async def test_async_connect_fails_if_async_connect_2_times_with_custom_alias(self):
        await async_connect(MONGO_TEST_DB, alias="alias1")

        with pytest.raises(ConnectionFailure) as exc_info:
            await async_connect(f"{MONGO_TEST_DB}_2", alias="alias1")

            assert (
                    "A different connection with alias `alias1` was already registered. Use async_disconnect() first"
                    == str(exc_info.value)

            )

    @pytest.mark.asyncio
    async def test_async_connect_fails_if_similar_connection_settings_arent_defined_the_same_way(
            self,
    ):
        """Intended to keep the detection function simple but robust"""
        db_name = MONGO_TEST_DB
        db_alias = "alias1"
        await async_connect(db=db_name, alias=db_alias, host="localhost", port=27017)

        with pytest.raises(ConnectionFailure):
            await async_connect(host="mongodb://localhost:27017/%s" % db_name, alias=db_alias)

    @pytest.mark.asyncio
    async def test_async_connect_passes_silently_connect_multiple_times_with_same_config(self):
        # test default async connection to `test`
        await async_connect()
        await async_connect()
        assert len(connection._connections) == 1
        await async_connect(f"{MONGO_TEST_DB}01", alias="test01")
        await async_connect(f"{MONGO_TEST_DB}01", alias="test01")
        assert len(connection._connections) == 2
        await async_connect(host=f"mongodb://localhost:27017/{MONGO_TEST_DB}02", alias="test02")
        await async_connect(host=f"mongodb://localhost:27017/{MONGO_TEST_DB}02", alias="test02")
        assert len(connection._connections) == 3

    @pytest.mark.asyncio
    async def test_async_connect_with_invalid_db_name(self):
        """Ensure that the async_connect() method fails fast if the db name is invalid"""
        with pytest.raises(InvalidName):
            await async_connect("mongodb://localhost")

    @pytest.mark.asyncio
    async def test_async_connect_with_db_name_external(self):
        """Ensure that async_connect() works if the db name is $external"""
        """Ensure that the async_connect() method works properly."""
        await async_connect("$external")

        conn = await async_get_connection()
        assert isinstance(conn, AsyncMongoClient)

        db = await async_get_db()
        assert isinstance(db, AsyncDatabase)
        assert db.name == "$external"

        await async_connect("$external", alias="testdb")
        conn = await async_get_connection("testdb")
        assert isinstance(conn, AsyncMongoClient)

    @pytest.mark.asyncio
    async def test_async_connect_with_invalid_db_name_type(self):
        """Ensure that the async_connect() method fails fast if db name has invalid type"""
        with pytest.raises(TypeError):
            non_string_db_name = ["e. g. list instead of a string"]
            await async_connect(non_string_db_name)

    @pytest.mark.asyncio
    async def test_async_disconnect_cleans_globals(self):
        """Ensure that the async_disconnect() method cleans the globals objects"""
        await reset_async_connections()
        await async_disconnect_all()
        connections = connection
        dbs = connection._dbs
        connection_settings = connection._connection_settings

        await async_connect(MONGO_TEST_DB)

        assert len(connections._connections) == 1
        assert len(dbs) == 0
        assert len(connection_settings) == 1

        class TestDoc(Document):
            pass

        await TestDoc.adrop_collection()  # triggers the db
        assert len(dbs) == 1

        await async_disconnect()
        assert len(connection._connections) == 0
        assert len(dbs) == 0
        assert len(connection_settings) == 0

    @pytest.mark.asyncio
    async def test_async_disconnect_cleans_cached_collection_attribute_in_document(self):
        """Ensure that the async_disconnect() method works properly"""
        await async_connect(MONGO_TEST_DB)

        class History(Document):
            pass

        assert await History._aget_collection() is not None

        await History.adrop_collection()
        await History.aobjects.first()  # will trigger the caching of _collection attribute
        assert await History._aget_collection() is not None

        await async_disconnect()

        with pytest.raises(ConnectionFailure) as exc_info:
            await History._aget_collection()
            await History.aobjects.first()
        assert "You have not defined a default connection" == str(exc_info.value)

    @pytest.mark.asyncio
    async def test_async_connect_disconnect_works_on_same_document(self):
        """Ensure that the async_connect/async_disconnect works properly with a single Document"""
        db1 = f"{MONGO_TEST_DB}_db1"
        db2 = f"{MONGO_TEST_DB}_db2"

        # Ensure freshness of the 2 databases through pymongo
        client = AsyncMongoClient("localhost", 27017)
        await client.drop_database(db1)
        await client.drop_database(db2)

        # Save in db1
        await async_connect(db1)

        class User(Document):
            name = StringField()

        user1 = await User(name="John is in db1").asave()
        await async_disconnect()
        # Make sure save doesn't work at this stage

        with pytest.raises(ConnectionFailure):
            await User(name="Wont work").asave()

        # Save in db2
        await async_connect(db2)
        user2 = await User(name="Bob is in db2").asave()
        await async_disconnect()

        db1_users = await client[db1].user.find().to_list()
        assert db1_users == [{"_id": user1.id, "name": "John is in db1"}]
        db2_users = await client[db2].user.find().to_list()
        assert db2_users == [{"_id": user2.id, "name": "Bob is in db2"}]

    @pytest.mark.asyncio
    async def test_async_disconnect_silently_pass_if_alias_does_not_exist(self):
        assert len(connection._connections) == 0
        await async_disconnect(alias="not_exist")

    @pytest.mark.asyncio
    async def test_async_disconnect_does_not_close_client_used_by_another_alias(self):
        client1 = await async_connect(alias="disconnect_reused_client_test_1")
        client2 = await async_connect(alias="disconnect_reused_client_test_2")
        client3 = await async_connect(alias="disconnect_reused_client_test_3", maxPoolSize=10)
        assert client1 is client2
        assert client1 is not client3
        await client1.admin.command("ping")
        await async_disconnect("disconnect_reused_client_test_1")
        # The client is not closed because the second alias still exists.
        await client2.admin.command("ping")
        await async_disconnect("disconnect_reused_client_test_2")
        # The client is now closed:
        if PYMONGO_VERSION >= (4,):
            with pytest.raises(InvalidOperation):
                await client2.admin.command("ping")
        # 3rd client connected to the same cluster with different options
        # is not closed either.
        await client3.admin.command("ping")
        await async_disconnect("disconnect_reused_client_test_3")
        # 3rd client is now closed:
        if PYMONGO_VERSION >= (4,):
            with pytest.raises(InvalidOperation):
                await client3.admin.command("ping")

    @pytest.mark.asyncio
    async def test_async_disconnect_all(self):
        await reset_async_connections()
        await async_disconnect_all()
        dbs = connection._dbs
        connection_settings = connection._connection_settings

        await async_connect(MONGO_TEST_DB)
        await async_connect(f"{MONGO_TEST_DB}_2", alias="db1")

        class History(Document):
            pass

        class History1(Document):
            name = StringField()
            meta = {"db_alias": "db1"}

        await History.adrop_collection()  # will trigger the caching of _collection attribute
        await History.aobjects.first()
        await History1.adrop_collection()
        await History1.aobjects.first()

        assert (await History._aget_collection()) is not None
        assert (await History1._aget_collection()) is not None

        assert len(connection._connections) == 2
        assert len(dbs) == 2
        assert len(connection_settings) == 2

        await async_disconnect_all()

        with pytest.raises(ConnectionFailure):
            await History._aget_collection()
            await History1._aget_collection()

        assert len(connection._connections) == 0
        assert len(dbs) == 0
        assert len(connection_settings) == 0

        with pytest.raises(ConnectionFailure):
            await History.aobjects.first()

        with pytest.raises(ConnectionFailure):
            await History1.aobjects.first()

    @pytest.mark.asyncio
    async def test_async_disconnect_all_silently_pass_if_no_connection_exist(self):
        await async_disconnect_all()

    @pytest.mark.asyncio
    async def test_sharing_async_connections(self):
        """Ensure that connections are shared when the connection settings are exactly the same"""
        await async_connect(MONGO_TEST_DB, alias="testdb1")
        expected_connection = await async_get_connection("testdb1")

        await async_connect(MONGO_TEST_DB, alias="testdb2")
        actual_connection = await async_get_connection("testdb2")

        await expected_connection.server_info()

        assert expected_connection == actual_connection

    @pytest.mark.asyncio
    async def test_async_connect_uri(self):
        """Ensure that the async_connect() method works properly with URIs."""
        c = await async_connect(db=MONGO_TEST_DB, alias="admin")
        admin_username = f"admin_{uuid.uuid4().hex[:8]}"
        user_username = f"user_{uuid.uuid4().hex[:8]}"

        await c.admin.command("createUser", admin_username, pwd="password", roles=["root"])

        adminadmin_settings = connection._connection_settings[
            "adminadmin"
        ] = connection._connection_settings["admin"].copy()
        adminadmin_settings["username"] = admin_username
        adminadmin_settings["password"] = "password"
        ca = await async_connect(db=MONGO_TEST_DB, alias="adminadmin")
        await ca.admin.command("createUser", user_username, pwd="password", roles=["dbOwner"])

        await async_connect(
            f"{MONGO_TEST_DB}_testdb_uri", host=f"mongodb://username:password@localhost/{MONGO_TEST_DB}"
        )

        conn = await async_get_connection()
        assert isinstance(conn, pymongo.AsyncMongoClient)

        db = await async_get_db()
        assert isinstance(db, AsyncDatabase)
        assert db.name == MONGO_TEST_DB

        await c.admin.command("dropUser", user_username)
        await c.admin.command("dropUser", admin_username)

    @pytest.mark.asyncio
    async def test_async_connect_uri_without_db(self):
        """Ensure the async_connect() method works properly if the URI doesn't
        include a database name.
        """
        await async_connect(MONGO_TEST_DB, host="mongodb://localhost/")

        conn = await async_get_connection()
        assert isinstance(conn, pymongo.AsyncMongoClient)

        db = await async_get_db()
        assert isinstance(db, AsyncDatabase)
        assert db.name == MONGO_TEST_DB

    @pytest.mark.asyncio
    async def test_async_connect_uri_default_db(self):
        """Ensure async_connect() defaults to the right database name if
        the URI and the database_name don't explicitly specify it.
        """
        await async_connect(host="mongodb://localhost/")

        conn = await async_get_connection()
        assert isinstance(conn, pymongo.AsyncMongoClient)

        db = await async_get_db()
        assert isinstance(db, AsyncDatabase)
        assert db.name == "test"

    @pytest.mark.asyncio
    async def test_uri_without_credentials_doesnt_override_async_conn_settings(self):
        """Ensure async_connect() uses the username and password params if the URI
        doesn't explicitly specify them.
        """
        await async_connect(
            host=f"mongodb://localhost/{MONGO_TEST_DB}", username="user", password="pass"
        )

        # OperationFailure means that mongoengine attempted authentication
        # w/ the provided username/password and failed - that's the desired
        # behavior. If the MongoDB URI overrides the credentials
        if PYMONGO_VERSION >= (4,):
            with pytest.raises(OperationFailure):
                db = await async_get_db()
                # pymongo 4.x does not call db.authenticate and needs to perform an operation to trigger the failure
                await db.list_collection_names()
        else:
            with pytest.raises(OperationFailure):
                await async_get_db()

    @pytest.mark.asyncio
    async def test_async_connect_uri_with_authsource(self):
        """Ensure that the async_connect() method works well with the `authSource`
        option in the URI.
        """
        # Create users
        c = await async_connect(MONGO_TEST_DB)

        username = f"user_{uuid.uuid4().hex[:8]}"
        await c.admin.command("createUser", username, pwd="password", roles=["dbOwner"])

        # Authentication fails without "authSource"
        test_conn = await async_connect(
            MONGO_TEST_DB,
            alias="test1",
            host=f"mongodb://{username}:password@localhost/{MONGO_TEST_DB}",
        )
        with pytest.raises(OperationFailure):
            await test_conn.server_info()

        # Authentication succeeds with "authSource"
        authd_conn = await async_connect(
            MONGO_TEST_DB,
            alias="test2",
            host=(
                f"mongodb://{username}:password@localhost/{MONGO_TEST_DB}?authSource=admin"
            ),
        )
        db = await async_get_db("test2")
        assert isinstance(db, AsyncDatabase)
        assert db.name == MONGO_TEST_DB

        # Clear all users
        await authd_conn.admin.command("dropUser", username)

    @pytest.mark.asyncio
    async def test_register_async_connection(self):
        """Ensure that async connections with different aliases may be registered."""
        await async_register_connection("testdb", f"{MONGO_TEST_DB}_2", mongo_client_class=AsyncMongoClient)

        with pytest.raises(ConnectionFailure):
            await async_get_connection()
        conn = await async_get_connection("testdb")
        assert isinstance(conn, pymongo.AsyncMongoClient)

        db = await async_get_db("testdb")
        assert isinstance(db, AsyncDatabase)
        assert db.name == f"{MONGO_TEST_DB}_2"

    @pytest.mark.asyncio
    async def test_register_async_connection_defaults(self):
        """Ensure that defaults are used when the host and port are None."""
        await async_register_connection("testdb", MONGO_TEST_DB, host=None, port=None,
                                        mongo_client_class=AsyncMongoClient)

        conn = await async_get_connection("testdb")
        assert isinstance(conn, pymongo.AsyncMongoClient)

    @pytest.mark.asyncio
    async def test_async_connection_kwargs(self):
        """Ensure that async connection kwargs get passed to pymongo."""
        await async_connect(MONGO_TEST_DB, alias="t1", tz_aware=True)
        conn = await async_get_connection("t1")

        assert get_tz_awareness(conn)

        await async_connect(f"{MONGO_TEST_DB}_2", alias="t2")
        conn = await async_get_connection("t2")
        assert not get_tz_awareness(conn)

    @pytest.mark.asyncio
    async def test_async_connection_pool_via_kwarg(self):
        """Ensure we can specify a max connection pool size using
        an async connection kwarg.
        """
        pool_size_kwargs = {"maxpoolsize": 100}

        conn = await async_connect(
            MONGO_TEST_DB, alias="max_pool_size_via_kwarg", **pool_size_kwargs
        )
        if PYMONGO_VERSION >= (4,):
            assert conn.options.pool_options.max_pool_size == 100
        else:
            assert conn.max_pool_size == 100

    @pytest.mark.asyncio
    async def test_async_connection_pool_via_uri(self):
        """Ensure we can specify a max connection pool size using
        an option in an async connection URI.
        """
        conn = await async_connect(
            host="mongodb://localhost/test?maxpoolsize=100",
            alias="max_pool_size_via_uri"
        )
        if PYMONGO_VERSION >= (4,):
            assert conn.options.pool_options.max_pool_size == 100
        else:
            assert conn.max_pool_size == 100

    @pytest.mark.asyncio
    async def test_async_write_concern(self):
        """Ensure write concern can be specified in connect() via
        a kwarg or as part of the connection URI.
        """
        conn1 = await async_connect(
            alias="conn1", host="mongodb://localhost/testing?w=1&journal=true"
        )
        conn2 = await async_connect("testing", alias="conn2", w=1, journal=True)
        assert conn1.write_concern.document == {"w": 1, "j": True}
        assert conn2.write_concern.document == {"w": 1, "j": True}

    @pytest.mark.asyncio
    async def test_async_connect_with_replicaset_via_uri(self):
        """Ensure connect() works when specifying a replicaSet via the
        MongoDB URI.
        """
        await async_connect(host="mongodb://localhost/test?replicaSet=local-rs")
        db = await async_get_db()
        assert isinstance(db, AsyncDatabase)
        assert db.name == "test"

    @pytest.mark.asyncio
    async def test_async_connect_with_replicaset_via_kwargs(self):
        """Ensure async_connect() works when specifying a replicaSet via the
        connection kwargs
        """
        c = await async_connect(replicaset="local-rs")
        if hasattr(c, "_AsyncMongoClient__options"):
            assert c._AsyncMongoClient__options.replica_set_name == "local-rs"
        else:  # pymongo >= 4.9
            assert c._options.replica_set_name == "local-rs"
        db = await async_get_db()
        assert isinstance(db, AsyncDatabase)
        assert db.name == "test"

    @pytest.mark.asyncio
    async def test_async_connect_tz_aware(self):
        await async_connect(MONGO_TEST_DB, tz_aware=True)
        d = datetime.datetime(2010, 5, 5, tzinfo=utc)

        class DateDoc(Document):
            the_date = DateTimeField(required=True)

        await DateDoc.adrop_collection()
        await DateDoc(the_date=d).asave()

        date_doc = await DateDoc.aobjects.first()
        assert d == date_doc.the_date

    @pytest.mark.asyncio
    async def test_async_read_preference_from_parse(self):
        conn = await async_connect(
            host="mongodb://a1.vpc,a2.vpc,a3.vpc/prod?readPreference=secondaryPreferred"
        )
        assert conn.read_preference == ReadPreference.SECONDARY_PREFERRED

    @pytest.mark.asyncio
    async def test_multiple_async_connection_settings(self):
        await async_connect(
            MONGO_TEST_DB,
            alias="t1",
            host="localhost",
            read_preference=ReadPreference.PRIMARY
        )
        await async_connect(
            f"{MONGO_TEST_DB}_2",
            alias="t2",
            host="127.0.0.1",
            read_preference=ReadPreference.PRIMARY_PREFERRED
        )
        mongo_connections = connection._connections
        assert len(mongo_connections.items()) == 2
        assert "t1" in mongo_connections.keys()
        assert "t2" in mongo_connections.keys()

        # Handle PyMongo 3+ Async Connection (lazily established)
        # Ensure we are connected, throws ServerSelectionTimeoutError otherwise.
        # Purposely not catching exception to fail the test if thrown.
        mongo_connections["t1"].server_info()
        mongo_connections["t2"].server_info()
        assert (await mongo_connections["t1"].address)[0] == "localhost"
        assert (await mongo_connections["t2"].address)[0] in (
            "localhost",
            "127.0.0.1",
        )  # weird but there is a discrepancy in the address in replicaset setup
        assert mongo_connections["t1"].read_preference == ReadPreference.PRIMARY
        assert (
                mongo_connections["t2"].read_preference == ReadPreference.PRIMARY_PREFERRED
        )
        assert mongo_connections["t1"] is not mongo_connections["t2"]

    @pytest.mark.asyncio
    async def test_async_connect_2_databases_uses_same_client_if_only_dbname_differs(self):
        c1 = await async_connect(alias="testdb1", db="testdb1")
        c2 = await async_connect(alias="testdb2", db="testdb2")
        assert c1 is c2

    @pytest.mark.asyncio
    async def test_async_connect_2_databases_uses_different_client_if_different_parameters(self):
        c1 = await async_connect(alias="testdb1", db="testdb1", username="u1", password="pass")
        c2 = await async_connect(alias="testdb2", db="testdb2", username="u2", password="pass")
        assert c1 is not c2

    @pytest.mark.asyncio
    async def test_async_connect_uri_uuidrepresentation_set_in_uri(self):
        rand = random_str()
        tmp_conn = await async_connect(
            alias=rand,
            host=f"mongodb://localhost:27017/{rand}?uuidRepresentation=csharpLegacy"
        )
        assert (
                tmp_conn.options.codec_options.uuid_representation
                == pymongo.common._UUID_REPRESENTATIONS["csharpLegacy"]
        )
        await async_disconnect(rand)

    @pytest.mark.asyncio
    async def test_async_connect_uri_uuidrepresentation_set_as_arg(self):
        rand = random_str()
        tmp_conn = await async_connect(alias=rand, db=rand, uuidRepresentation="javaLegacy")
        assert (
                tmp_conn.options.codec_options.uuid_representation
                == pymongo.common._UUID_REPRESENTATIONS["javaLegacy"]
        )
        await async_disconnect(rand)

    @pytest.mark.asyncio
    async def test_async_connect_uri_uuidrepresentation_set_both_arg_and_uri_arg_prevail(self):
        rand = random_str()
        tmp_conn = await async_connect(
            alias=rand,
            host=f"mongodb://localhost:27017/{rand}?uuidRepresentation=csharpLegacy",
            uuidRepresentation="javaLegacy",
        )
        assert (
                tmp_conn.options.codec_options.uuid_representation
                == pymongo.common._UUID_REPRESENTATIONS["javaLegacy"]
        )
        await async_disconnect(rand)

    @pytest.mark.asyncio
    async def test_async_connect_uuid_representation_defaults_to_unspecified(self):
        """
        PyMongo >= 4 defaults uuidRepresentation to UNSPECIFIED.
        Old behavior ('pythonLegacy') is deprecated and removed.
        """
        rand = random_str()
        tmp_conn = await async_connect(alias=rand, db=rand)

        # Assert new PyMongo 4.x behavior
        assert (
                tmp_conn.options.codec_options.uuid_representation
                == UuidRepresentation.UNSPECIFIED
        )

        await async_disconnect(rand)
