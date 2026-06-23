import unittest

from pymongo import AsyncMongoClient, ReadPreference

from mongoengine.asynchronous.connection import ConnectionFailure, async_connect
from tests.asynchronous.utils import reset_async_connections
from tests.utils import MONGO_TEST_DB

CONN_CLASS = AsyncMongoClient
READ_PREF = ReadPreference.SECONDARY


class ConnectionTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await reset_async_connections()

    async def asyncTearDown(self):
        await reset_async_connections()

    async def test_replicaset_uri_passes_read_preference(self):
        """Requires a replica set called "rs" on port 27017"""
        try:
            conn = async_connect(
                db=MONGO_TEST_DB,
                host=f"mongodb://localhost/{MONGO_TEST_DB}?replicaSet=rs",
                read_preference=READ_PREF,
            )
        except ConnectionFailure:
            return

        if not isinstance(conn, CONN_CLASS):
            raise TypeError(f"Expected {CONN_CLASS}, got {type(conn)}")

        assert conn.read_preference == READ_PREF
