import pytest
from bson import Int64

from mongoengine import *
from tests.asynchronous.utils import MongoDBAsyncTestCase


class TestIntField(MongoDBAsyncTestCase):
    def test_int_validation(self):
        """Ensure that invalid values cannot be assigned to int fields."""

        class Person(Document):
            age = IntField(min_value=0, max_value=110)

        person = Person()
        person.age = 0
        person.validate()

        person.age = 50
        person.validate()

        person.age = 110
        person.validate()

        person.age = -1
        with pytest.raises(ValidationError):
            person.validate()
        person.age = 120
        with pytest.raises(ValidationError):
            person.validate()
        person.age = "ten"
        with pytest.raises(ValidationError):
            person.validate()

    async def test_ne_operator(self):
        class TestDocument(Document):
            int_fld = IntField()

        await TestDocument.adrop_collection()

        await TestDocument(int_fld=None).asave()
        await TestDocument(int_fld=1).asave()

        assert 1 == await TestDocument.aobjects(int_fld__ne=None).count()
        assert 1 == await TestDocument.aobjects(int_fld__ne=1).count()

    async def test_int_field_long_field_migration(self):
        class DeprecatedLongField(IntField):
            """64-bit integer field. (Equivalent to IntField since the support to Python2 was dropped)"""

            def to_mongo(self, value):
                return Int64(value)

        class TestDocument(Document):
            long = DeprecatedLongField()

        await TestDocument.adrop_collection()
        await TestDocument(long=10).asave()

        v = (await TestDocument.aobjects().first()).long

        # simulate a migration to IntField
        class TestDocument(Document):
            long = IntField()

        assert await TestDocument.aobjects(long=10).count() == 1
        assert (await TestDocument.aobjects().first()).long == v
