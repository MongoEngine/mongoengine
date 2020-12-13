from bson.int64 import Int64
import pytest

from mongoengine import *
from mongoengine.connection import get_db

from tests.utils import MongoDBTestCase


class TestLongField(MongoDBTestCase):
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

        doc = TestDocument()
        doc.value = 50
        doc.validate()

        doc.value = -1
        with pytest.raises(ValidationError):
            doc.validate()
        doc.value = 120
        with pytest.raises(ValidationError):
            doc.validate()
        doc.value = "ten"
        with pytest.raises(ValidationError):
            doc.validate()

    def test_long_ne_operator(self):
        class TestDocument(Document):
            long_fld = LongField()

        TestDocument.drop_collection()

        TestDocument(long_fld=None).save()
        TestDocument(long_fld=1).save()

        assert 1 == TestDocument.objects(long_fld__ne=None).count()
