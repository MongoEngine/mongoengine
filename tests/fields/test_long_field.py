from bson.int64 import Int64
import pytest

from mongoengine import *
from mongoengine.connection import get_db

from tests.utils import MongoDBTestCase, get_as_pymongo


class TestLongField(MongoDBTestCase):
    def test_storage(self):
        class Person(Document):
            value = LongField()

        Person.drop_collection()
        person = Person(value=5000)
        person.save()
        assert get_as_pymongo(person) == {"_id": person.id, "value": 5000}

    def test_construction_does_not_fail_with_invalid_value(self):
        class Person(Document):
            value = LongField()

        person = Person(value="not_an_int")
        assert person.value == "not_an_int"

    def test_long_field_is_considered_as_int64(self):
        """
        Tests that long fields are stored as long in mongo, even if long
        value is small enough to be an int.
        """

        class TestLongFieldConsideredAsInt64(Document):
            some_long = LongField()

        doc = TestLongFieldConsideredAsInt64(some_long=42).save()
        db = get_db()
        assert isinstance(
            db.test_long_field_considered_as_int64.find()[0]["some_long"], Int64
        )
        assert isinstance(doc.some_long, int)

    def test_long_validation(self):
        """Ensure that invalid values cannot be assigned to long fields."""

        class TestDocument(Document):
            value = LongField(min_value=0, max_value=110)

        TestDocument(value=50).validate()

        with pytest.raises(ValidationError):
            TestDocument(value=-1).validate()

        with pytest.raises(ValidationError):
            TestDocument(value=120).validate()

        with pytest.raises(ValidationError):
            TestDocument(value="ten").validate()

    def test_long_ne_operator(self):
        class TestDocument(Document):
            long_fld = LongField()

        TestDocument.drop_collection()

        TestDocument(long_fld=None).save()
        TestDocument(long_fld=1).save()

        assert TestDocument.objects(long_fld__ne=None).count() == 1
        assert TestDocument.objects(long_fld__ne=1).count() == 1
