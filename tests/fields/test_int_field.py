import pytest
from bson import Int64

from mongoengine import *
from tests.utils import MongoDBTestCase


class TestIntField(MongoDBTestCase):
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

    def test_ne_operator(self):
        class TestDocument(Document):
            int_fld = IntField()

        TestDocument.drop_collection()

        TestDocument(int_fld=None).save()
        TestDocument(int_fld=1).save()

        assert 1 == TestDocument.objects(int_fld__ne=None).count()
        assert 1 == TestDocument.objects(int_fld__ne=1).count()

    def test_int_field_long_field_migration(self):
        class DeprecatedLongField(IntField):
            """64-bit integer field. (Equivalent to IntField since the support to Python2 was dropped)"""

            def to_mongo(self, value):
                return Int64(value)

        class TestDocument(Document):
            long = DeprecatedLongField()

        TestDocument.drop_collection()
        TestDocument(long=10).save()

        v = TestDocument.objects().first().long

        # simulate a migration to IntField
        class TestDocument(Document):
            long = IntField()

        assert TestDocument.objects(long=10).count() == 1
        assert TestDocument.objects().first().long == v
