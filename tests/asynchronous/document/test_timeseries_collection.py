import asyncio
import unittest
from datetime import datetime, timedelta, UTC

from mongoengine import (
    DateTimeField,
    Document,
    FloatField,
    StringField,
)
from mongoengine.asynchronous import async_connect, async_get_db, async_disconnect
from mongoengine.registry import _CollectionRegistry
from tests.asynchronous.utils import requires_mongodb_gte_50


class TestTimeSeriesCollections(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await async_connect(db="mongoenginetest")
        self.db = await async_get_db()

        class SensorData(Document):
            timestamp = DateTimeField(required=True)
            temperature = FloatField()

            meta = {
                "timeseries": {
                    "timeField": "timestamp",
                    "metaField": "temperature",
                    "granularity": "seconds",
                    "expireAfterSeconds": 5,
                },
                "collection": "sensor_data",
            }

        self.SensorData = SensorData

    async def asyncTearDown(self):
        await super().asyncTearDown()
        _CollectionRegistry.clear()

    async def test_get_db(self):
        """Ensure that get_db returns the expected db."""
        db = await self.SensorData._async_get_db()
        assert self.db == db

    async def asyncTearDown(self):
        for collection_name in await self.db.list_collection_names():
            if not collection_name.startswith("system."):
                await self.db.drop_collection(collection_name)
        await async_disconnect()

    async def test_definition(self):
        """Ensure that document may be defined using fields."""
        assert ["id", "temperature", "timestamp"] == sorted(
            self.SensorData._fields.keys()
        )
        assert ["DateTimeField", "FloatField", "ObjectIdField"] == sorted(
            x.__class__.__name__ for x in self.SensorData._fields.values()
        )

    @requires_mongodb_gte_50
    async def test_get_collection(self):
        """Ensure that get_collection returns the expected collection."""
        collection_name = "sensor_data"
        collection = await self.SensorData._aget_collection()
        assert self.db[collection_name] == collection

    @requires_mongodb_gte_50
    async def test_create_timeseries_collection(self):
        """Ensure that a time-series collection can be created."""
        collection_name = self.SensorData._get_collection_name()
        collection = await self.SensorData._aget_collection()

        assert collection_name in await self.db.list_collection_names()

        options = await collection.options()

        assert options.get("timeseries") is not None
        assert options["timeseries"]["timeField"] == "timestamp"
        assert options["timeseries"]["granularity"] == "seconds"

    @requires_mongodb_gte_50
    async def test_insert_document_into_timeseries_collection(self):
        """Ensure that a document can be inserted into a time-series collection."""
        collection_name = self.SensorData._get_collection_name()
        collection = await self.SensorData._aget_collection()
        assert collection_name in await self.db.list_collection_names()

        # Insert a document and ensure it was inserted
        await self.SensorData(timestamp=datetime.now(UTC), temperature=23.4).asave()
        assert await collection.count_documents({}) == 1

    @requires_mongodb_gte_50
    async def test_timeseries_expiration(self):
        """Ensure that documents in a time-series collection expire after the specified time."""

        self.SensorData._meta["timeseries"]["expireAfterSeconds"] = 1
        self.SensorData._get_collection_name()
        collection = await self.SensorData._aget_collection()
        options = await collection.options()
        assert options.get("timeseries", {}) is not None
        assert options["expireAfterSeconds"] == 1

        await self.SensorData(timestamp=datetime.now(UTC), temperature=23.4).asave()

        assert await collection.count_documents({}) == 1

        # Wait for more than the expiration time
        await asyncio.sleep(2)
        assert await collection.count_documents({}) > 0

    @requires_mongodb_gte_50
    async def test_index_creation(self):
        """Test if the index defined in the meta dictionary is created properly."""

        # Define the Document with indexes
        class SensorDataWithIndex(Document):
            timestamp = DateTimeField(required=True)
            temperature = FloatField()
            location = StringField()  # Field to be indexed

            meta = {
                "timeseries": {
                    "timeField": "timestamp",
                    "metaField": "temperature",
                    "granularity": "seconds",
                    "expireAfterSeconds": 5,
                },
                "collection": "sensor_data",
                "indexes": [
                    {"fields": ["timestamp"], "name": "timestamp_index"},
                    {"fields": ["temperature"], "name": "temperature_index"},
                ],
            }

        collection = await SensorDataWithIndex._aget_collection()

        indexes = await collection.index_information()

        assert "timestamp_index" in indexes
        assert "temperature_index" in indexes

    @requires_mongodb_gte_50
    async def test_timeseries_data_insertion_order(self):
        """Ensure that data in the time-series collection is inserted and queried in the correct time order."""
        self.SensorData._get_collection_name()
        await self.SensorData._aget_collection()

        # Insert documents out of order
        now = datetime.now(UTC)
        await self.SensorData(timestamp=now, temperature=23.4).asave()
        await self.SensorData(timestamp=now - timedelta(seconds=5), temperature=22.0).asave()
        await self.SensorData(timestamp=now + timedelta(seconds=5), temperature=24.0).asave()

        documents = await self.SensorData.aobjects.order_by("timestamp").to_list()

        # Check the insertion order
        assert len(documents) == 3
        assert documents[0].temperature == 22.0
        assert documents[1].temperature == 23.4
        assert documents[2].temperature == 24.0

    @requires_mongodb_gte_50
    async def test_timeseries_query_by_time_range(self):
        """Ensure that data can be queried by a specific time range in the time-series collection."""

        self.SensorData._get_collection_name()
        await self.SensorData._aget_collection()

        now = datetime.now(UTC)
        await self.SensorData(timestamp=now - timedelta(seconds=10), temperature=22.0).asave()
        await self.SensorData(timestamp=now - timedelta(seconds=5), temperature=23.0).asave()
        await self.SensorData(timestamp=now, temperature=24.0).asave()

        # Query documents within the last 6 seconds
        start_time = now - timedelta(seconds=6)
        documents = await self.SensorData.aobjects(timestamp__gte=start_time).to_list()

        assert len(documents) == 2
        assert documents[0].temperature == 23.0
        assert documents[1].temperature == 24.0