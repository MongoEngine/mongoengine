# -*- coding: utf-8 -*-
import six

try:
    from bson.int64 import Int64
except ImportError:
    Int64 = long

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
        self.assertIsInstance(db.test_long_field_considered_as_int64.find()[0]['some_long'], Int64)
        self.assertIsInstance(doc.some_long, six.integer_types)

    def test_long_validation(self):
        """Ensure that invalid values cannot be assigned to long fields.
        """
        class TestDocument(Document):
            value = LongField(min_value=0, max_value=110)

        doc = TestDocument()
        doc.value = 50
        doc.validate()

        doc.value = -1
        self.assertRaises(ValidationError, doc.validate)
        doc.age = 120
        self.assertRaises(ValidationError, doc.validate)
        doc.age = 'ten'
        self.assertRaises(ValidationError, doc.validate)

    def test_long_ne_operator(self):
        class TestDocument(Document):
            long_fld = LongField()

        TestDocument.drop_collection()

        TestDocument(long_fld=None).save()
        TestDocument(long_fld=1).save()

        self.assertEqual(1, TestDocument.objects(long_fld__ne=None).count())
