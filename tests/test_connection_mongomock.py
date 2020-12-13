import unittest

import pytest

import mongoengine.connection
from mongoengine import (
    Document,
    StringField,
    connect,
    disconnect_all,
)
from mongoengine.connection import get_connection


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
        mongoengine.connection._connection_settings = {}
        mongoengine.connection._connections = {}
        mongoengine.connection._dbs = {}

    @require_mongomock
    def test_connect_in_mocking(self):
        """Ensure that the connect() method works properly in mocking."""
        connect("mongoenginetest", host="mongomock://localhost")
        conn = get_connection()
        assert isinstance(conn, mongomock.MongoClient)

        connect("mongoenginetest2", host="mongomock://localhost", alias="testdb2")
        conn = get_connection("testdb2")
        assert isinstance(conn, mongomock.MongoClient)

        connect(
            "mongoenginetest3",
            host="mongodb://localhost",
            is_mock=True,
            alias="testdb3",
        )
        conn = get_connection("testdb3")
        assert isinstance(conn, mongomock.MongoClient)

        connect("mongoenginetest4", is_mock=True, alias="testdb4")
        conn = get_connection("testdb4")
        assert isinstance(conn, mongomock.MongoClient)

        connect(
            host="mongodb://localhost:27017/mongoenginetest5",
            is_mock=True,
            alias="testdb5",
        )
        conn = get_connection("testdb5")
        assert isinstance(conn, mongomock.MongoClient)

        connect(host="mongomock://localhost:27017/mongoenginetest6", alias="testdb6")
        conn = get_connection("testdb6")
        assert isinstance(conn, mongomock.MongoClient)

        connect(
            host="mongomock://localhost:27017/mongoenginetest7",
            is_mock=True,
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

        conn = connect(host="mongomock://localhost:27017/mongoenginetest")
        some_document = SomeDocument()
        # database won't exist until we save a document
        some_document.save()
        assert SomeDocument.objects.count() == 1
        assert conn.get_default_database().name == "mongoenginetest"
        assert conn.list_database_names()[0] == "mongoenginetest"

    @require_mongomock
    def test_basic_queries_against_mongomock(self):
        disconnect_all()

        connect(host="mongomock://localhost:27017/mongoenginetest")

        class Person(Document):
            name = StringField()

        Person.drop_collection()
        assert Person.objects.count() == 0

        bob = Person(name="Bob").save()
        john = Person(name="John").save()
        assert Person.objects.count() == 2

        qs = Person.objects(name="Bob")
        assert qs.count() == 1
        assert qs.first() == bob
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
        connect(host=["mongomock://localhost"])
        conn = get_connection()
        assert isinstance(conn, mongomock.MongoClient)

        connect(host=["mongodb://localhost"], is_mock=True, alias="testdb2")
        conn = get_connection("testdb2")
        assert isinstance(conn, mongomock.MongoClient)

        connect(host=["localhost"], is_mock=True, alias="testdb3")
        conn = get_connection("testdb3")
        assert isinstance(conn, mongomock.MongoClient)

        connect(
            host=["mongomock://localhost:27017", "mongomock://localhost:27018"],
            alias="testdb4",
        )
        conn = get_connection("testdb4")
        assert isinstance(conn, mongomock.MongoClient)

        connect(
            host=["mongodb://localhost:27017", "mongodb://localhost:27018"],
            is_mock=True,
            alias="testdb5",
        )
        conn = get_connection("testdb5")
        assert isinstance(conn, mongomock.MongoClient)

        connect(
            host=["localhost:27017", "localhost:27018"], is_mock=True, alias="testdb6"
        )
        conn = get_connection("testdb6")
        assert isinstance(conn, mongomock.MongoClient)


if __name__ == "__main__":
    unittest.main()
