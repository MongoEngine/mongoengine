import unittest
import uuid

from nose.plugins.skip import SkipTest
from datetime import datetime
from bson import ObjectId

import pymongo

from mongoengine import *

__all__ = ("TestJson",)


class TestJson(unittest.TestCase):

    def setUp(self):
        connect(db='mongoenginetest')

    def test_json_names(self):
        """
        Going to test reported issue:
            https://github.com/MongoEngine/mongoengine/issues/654
        where the reporter asks for the availability to perform
        a to_json with the original class names and not the abreviated
        mongodb document keys
        """
        class Embedded(EmbeddedDocument):
            string = StringField(db_field='s')

        class Doc(Document):
            string = StringField(db_field='s')
            embedded = EmbeddedDocumentField(Embedded, db_field='e')

        doc = Doc( string="Hello", embedded=Embedded(string="Inner Hello"))
        doc_json = doc.to_json(sort_keys=True, use_db_field=False,separators=(',', ':'))

        expected_json = """{"embedded":{"string":"Inner Hello"},"string":"Hello"}"""

        self.assertEqual( doc_json, expected_json)

    def test_json_simple(self):

        class Embedded(EmbeddedDocument):
            string = StringField()

        class Doc(Document):
            string = StringField()
            embedded_field = EmbeddedDocumentField(Embedded)

            def __eq__(self, other):
                return (self.string == other.string and
                        self.embedded_field == other.embedded_field)

        doc = Doc(string="Hi", embedded_field=Embedded(string="Hi"))

        doc_json = doc.to_json(sort_keys=True, separators=(',', ':'))
        expected_json = """{"embedded_field":{"string":"Hi"},"string":"Hi"}"""
        self.assertEqual(doc_json, expected_json)

        self.assertEqual(doc, Doc.from_json(doc.to_json()))

    def test_json_complex(self):

        if pymongo.version_tuple[0] <= 2 and pymongo.version_tuple[1] <= 3:
            raise SkipTest("Need pymongo 2.4 as has a fix for DBRefs")

        class EmbeddedDoc(EmbeddedDocument):
            pass

        class Simple(Document):
            pass

        class Doc(Document):
            string_field = StringField(default='1')
            int_field = IntField(default=1)
            float_field = FloatField(default=1.1)
            boolean_field = BooleanField(default=True)
            datetime_field = DateTimeField(default=datetime.now)
            embedded_document_field = EmbeddedDocumentField(EmbeddedDoc,
                                        default=lambda: EmbeddedDoc())
            list_field = ListField(default=lambda: [1, 2, 3])
            dict_field = DictField(default=lambda: {"hello": "world"})
            objectid_field = ObjectIdField(default=ObjectId)
            reference_field = ReferenceField(Simple, default=lambda:
                                                        Simple().save())
            map_field = MapField(IntField(), default=lambda: {"simple": 1})
            decimal_field = DecimalField(default=1.0)
            complex_datetime_field = ComplexDateTimeField(default=datetime.now)
            url_field = URLField(default="http://mongoengine.org")
            dynamic_field = DynamicField(default=1)
            generic_reference_field = GenericReferenceField(
                                            default=lambda: Simple().save())
            sorted_list_field = SortedListField(IntField(),
                                                default=lambda: [1, 2, 3])
            email_field = EmailField(default="ross@example.com")
            geo_point_field = GeoPointField(default=lambda: [1, 2])
            sequence_field = SequenceField()
            uuid_field = UUIDField(default=uuid.uuid4)
            generic_embedded_document_field = GenericEmbeddedDocumentField(
                                        default=lambda: EmbeddedDoc())

            def __eq__(self, other):
                import json
                return json.loads(self.to_json()) == json.loads(other.to_json())

        doc = Doc()
        self.assertEqual(doc, Doc.from_json(doc.to_json()))


if __name__ == '__main__':
    unittest.main()
