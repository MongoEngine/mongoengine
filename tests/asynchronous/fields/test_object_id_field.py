import pytest
from bson import ObjectId

from mongoengine import Document, ObjectIdField, ValidationError
from tests.asynchronous.utils import MongoDBAsyncTestCase, async_get_as_pymongo


class TestObjectIdField(MongoDBAsyncTestCase):
    async def test_storage(self):
        class MyDoc(Document):
            oid = ObjectIdField()

        doc = MyDoc(oid=ObjectId())
        await doc.asave()
        assert await async_get_as_pymongo(doc) == {"_id": doc.id, "oid": doc.oid}

    async def test_constructor_converts_str_to_ObjectId(self):
        class MyDoc(Document):
            oid = ObjectIdField()

        doc = MyDoc(oid=str(ObjectId()))
        assert isinstance(doc.oid, ObjectId)

    async def test_validation_works(self):
        class MyDoc(Document):
            oid = ObjectIdField()

        doc = MyDoc(oid="not-an-oid!")
        with pytest.raises(ValidationError, match="Invalid ObjectID"):
            await doc.asave()

    async def test_query_none_value_dont_raise(self):
        # cf issue #2681
        class MyDoc(Document):
            oid = ObjectIdField(null=True)

        _ = await MyDoc.aobjects(oid=None).to_list()
