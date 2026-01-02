import unittest

from pymongo import MongoClient, ReadPreference

import mongoengine
from mongoengine.asynchronous.connection import ConnectionFailure, async_connect
from tests.utils import MONGO_TEST_DB

CONN_CLASS = MongoClient
READ_PREF = ReadPreference.SECONDARY


class ConnectionTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        mongoengine.asynchronous.connection._connection_settings = {}
        mongoengine.asynchronous.connection._connections = {}
        mongoengine.asynchronous.connection._dbs = {}

    async def asyncTearDown(self):
        mongoengine.asynchronous.connection._connection_settings = {}
        mongoengine.asynchronous.connection._connections = {}
        mongoengine.asynchronous.connection._dbs = {}

    async def test_replicaset_uri_passes_read_preference(self):
        """Requires a replica set called "rs" on port 27017"""
        try:
            conn = await async_connect(
                db=MONGO_TEST_DB,
                host=f"mongodb://localhost/{MONGO_TEST_DB}?replicaSet=rs",
                read_preference=READ_PREF,
            )
        except ConnectionFailure:
            return

        if not isinstance(conn, CONN_CLASS):
            # really???
            return

        assert conn.read_preference == READ_PREF
