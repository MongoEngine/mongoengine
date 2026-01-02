import unittest

import pytest

import mongoengine.synchronous.connection
from mongoengine import Document, StringField, connect, disconnect_all
from mongoengine.synchronous.connection import get_connection
from tests.utils import MONGO_TEST_DB

try:
    import mongomock

    MONGOMOCK_INSTALLED = True
except ImportError:
    MONGOMOCK_INSTALLED = False

require_mongomock = pytest.mark.skipif(
    not MONGOMOCK_INSTALLED, reason="you need mongomock installed to run this testcase"
)


class MongoMockConnectionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        disconnect_all()

    @classmethod
    def tearDownClass(cls):
        disconnect_all()

    def tearDown(self):
        mongoengine.synchronous.connection._connection_settings = {}
        mongoengine.synchronous.connection._connections = {}
        mongoengine.synchronous.connection._dbs = {}

    @require_mongomock
    def test_connect_in_mocking(self):
        """Ensure that the connect() method works properly in mocking."""
        connect(
            MONGO_TEST_DB,
            host="mongodb://localhost",
            mongo_client_class=mongomock.MongoClient,
        )
        conn = get_connection()
        assert isinstance(conn, mongomock.MongoClient)

        connect(
            f"{MONGO_TEST_DB}_2",
            host="mongodb://localhost",
            mongo_client_class=mongomock.MongoClient,
            alias="testdb2",
        )
        conn = get_connection("testdb2")
        assert isinstance(conn, mongomock.MongoClient)

        connect(
            f"{MONGO_TEST_DB}_3",
            host="mongodb://localhost",
            mongo_client_class=mongomock.MongoClient,
            alias="testdb3",
        )
        conn = get_connection("testdb3")
        assert isinstance(conn, mongomock.MongoClient)

        connect(
            f"{MONGO_TEST_DB}_4",
            mongo_client_class=mongomock.MongoClient,
            alias="testdb4",
        )
        conn = get_connection("testdb4")
        assert isinstance(conn, mongomock.MongoClient)

        connect(
            host=f"mongodb://localhost:27017/{MONGO_TEST_DB}_5",
            mongo_client_class=mongomock.MongoClient,
            alias="testdb5",
        )
        conn = get_connection("testdb5")
        assert isinstance(conn, mongomock.MongoClient)

        connect(
            host=f"mongodb://localhost:27017/{MONGO_TEST_DB}_6",
            mongo_client_class=mongomock.MongoClient,
            alias="testdb6",
        )
        conn = get_connection("testdb6")
        assert isinstance(conn, mongomock.MongoClient)

        connect(
            host=f"mongodb://localhost:27017/{MONGO_TEST_DB}_7",
            mongo_client_class=mongomock.MongoClient,
            alias="testdb7",
        )
        conn = get_connection("testdb7")
        assert isinstance(conn, mongomock.MongoClient)

    @require_mongomock
    def test_default_database_with_mocking(self):
        """Ensure that the default database is correctly set when using mongomock."""
        disconnect_all()

        class SomeDocument(Document):
            pass

        conn = connect(
            host=f"mongodb://localhost:27017/{MONGO_TEST_DB}",
            mongo_client_class=mongomock.MongoClient,
        )
        some_document = SomeDocument()
        # database won't exist until we save a document
        some_document.save()
        assert SomeDocument.objects.count() == 1
        assert conn.get_default_database().name == MONGO_TEST_DB
        assert conn.list_database_names()[0] == MONGO_TEST_DB

    @require_mongomock
    def test_basic_queries_against_mongomock(self):
        disconnect_all()

        connect(
            host=f"mongodb://localhost:27017/{MONGO_TEST_DB}",
            mongo_client_class=mongomock.MongoClient,
        )

        class Person(Document):
            name = StringField()

        Person.drop_collection()
        assert Person.objects.limit(0).count(with_limit_and_skip=True) == 0

        bob = Person(name="Bob").save()
        john = Person(name="John").save()
        assert Person.objects.count() == 2

        qs = Person.objects(name="Bob")
        assert qs.count() == 1
        assert qs[0] == bob
        assert list(qs.as_pymongo()) == [{"_id": bob.id, "name": "Bob"}]

        pipeline = [{"$project": {"name": {"$toUpper": "$name"}}}]
        data = Person.objects.order_by("name").aggregate(pipeline)
        assert list(data) == [
            {"_id": bob.id, "name": "BOB"},
            {"_id": john.id, "name": "JOHN"},
        ]

        Person.drop_collection()
        assert Person.objects.count() == 0

    @require_mongomock
    def test_connect_with_host_list(self):
        """Ensure that the connect() method works when host is a list

        Uses mongomock to test w/o needing multiple mongod/mongos processes
        """
        connect(host=["mongodb://localhost"], mongo_client_class=mongomock.MongoClient)
        conn = get_connection()
        assert isinstance(conn, mongomock.MongoClient)

        connect(
            host=["localhost"],
            mongo_client_class=mongomock.MongoClient,
            alias="testdb3",
        )
        conn = get_connection("testdb3")
        assert isinstance(conn, mongomock.MongoClient)

        connect(
            host=["mongodb://localhost:27017", "mongodb://localhost:27018"],
            alias="testdb4",
            mongo_client_class=mongomock.MongoClient,
        )
        conn = get_connection("testdb4")
        assert isinstance(conn, mongomock.MongoClient)

        connect(
            host=["mongodb://localhost:27017", "mongodb://localhost:27018"],
            mongo_client_class=mongomock.MongoClient,
            alias="testdb5",
        )
        conn = get_connection("testdb5")
        assert isinstance(conn, mongomock.MongoClient)

        connect(
            host=["localhost:27017", "localhost:27018"],
            mongo_client_class=mongomock.MongoClient,
            alias="testdb6",
        )
        conn = get_connection("testdb6")
        assert isinstance(conn, mongomock.MongoClient)


if __name__ == "__main__":
    unittest.main()
