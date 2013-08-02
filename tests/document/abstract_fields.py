# -*- coding: utf-8 -*-
import sys
sys.path[0:0] = [""]
import unittest

from mongoengine import Document, EmbeddedDocument, connect
from mongoengine.connection import get_db
from mongoengine.fields import IntField, StringField, EmbeddedDocumentField, \
    ListField, DictField, MapField

__all__ = ('AbstractFieldsTest', )


class AbstractFieldsTest(unittest.TestCase):

    def setUp(self):
        connect(db='mongoenginetest')
        self.db = get_db()

    def tearDown(self):
        for collection in self.db.collection_names():
            if 'system.' in collection:
                continue
            self.db.drop_collection(collection)

    def test_simple(self):
        "Index on a simple abstract field"
        class Animal(Document):
            meta = {
                'abstract_fields': {
                    'wings': IntField()
                },
                'indexes': [('wings',)]
            }

        expected_specs = [{'fields': [('wings', 1)]}]
        self.assertEqual(expected_specs, Animal._meta['index_specs'])

    def test_compound(self):
        "Compound index on a non-abstract and an abstract field"
        class Animal(Document):
            name = StringField()

            meta = {
                'abstract_fields': {
                    'wings': IntField()
                },
                'indexes': [('name', 'wings')]
            }

        expected_specs = [{'fields': [('name', 1), ('wings', 1)]}]
        self.assertEqual(expected_specs, Animal._meta['index_specs'])

    def test_embedded(self):
        "Index on an abstract EmbeddedDocumentField and one of its fields"
        class Species(EmbeddedDocument):
            name = StringField()

        class Animal(Document):
            meta = {
                'abstract_fields': {
                    'species': EmbeddedDocumentField(Species)
                },
                'indexes': [('species',), ('species.name',)]
            }

        expected_specs = [
            {'fields': [('species', 1)]},
            {'fields': [('species.name', 1)]}
        ]
        self.assertEqual(expected_specs, Animal._meta['index_specs'])

    def test_embedded_abstract(self):
        "Index on a abstract field of an EmbeddedDocumentField"
        class Species(EmbeddedDocument):
            meta = {
                'abstract_fields': {
                    'genus': StringField()
                }
            }

        class Animal(Document):
            species = EmbeddedDocumentField(Species)

            meta = {
                'indexes': [('species.genus',)]
            }

        expected_specs = [{'fields': [('species.genus', 1)]}]
        self.assertEqual(expected_specs, Animal._meta['index_specs'])

    def test_embedded_abstract_embedded(self):
        "Index on a abstract field of an abstract EmbeddedDocumentField"
        class Species(EmbeddedDocument):
            meta = {
                'abstract_fields': {
                    'genus': StringField()
                }
            }

        class Animal(Document):
            meta = {
                'abstract_fields': {
                    'species': EmbeddedDocumentField(Species)
                },
                'indexes': [('species.genus',)]
            }

        expected_specs = [{'fields': [('species.genus', 1)]}]
        self.assertEqual(expected_specs, Animal._meta['index_specs'])

    def test_list(self):
        "Index on an abstract ListField"
        class Animal(Document):
            meta = {
                'abstract_fields': {'habitats': ListField(StringField())},
                'indexes': ['habitats']
            }

        expected_specs = [{'fields': [('habitats', 1)]}]
        self.assertEqual(expected_specs, Animal._meta['index_specs'])

    def test_list_embedded(self):
        "Index on a field of an EmbeddedDocumentField in an abstract ListField"
        class Habitat(EmbeddedDocument):
            name = StringField()

        class Animal(Document):
            meta = {
                'abstract_fields': {'habitats': ListField(EmbeddedDocumentField(Habitat))},
                'indexes': ['habitats.name']
            }

        expected_specs = [{'fields': [('habitats.name', 1)]}]
        self.assertEqual(expected_specs, Animal._meta['index_specs'])

    def test_list_embedded_abstract(self):
        """
        Index on an abstract field of an EmbeddedDocumentField in an
        abstract ListField
        """
        class Habitat(EmbeddedDocument):
            meta = {
                'abstract_fields': {'name': StringField()}
            }

        class Animal(Document):
            meta = {
                'abstract_fields': {'habitats': ListField(EmbeddedDocumentField(Habitat))},
                'indexes': ['habitats.name']
            }

        expected_specs = [{'fields': [('habitats.name', 1)]}]
        self.assertEqual(expected_specs, Animal._meta['index_specs'])

    def test_dict(self):
        "Index on an abstract DictField, and on a sub-field"
        class Animal(Document):
            meta = {
                'abstract_fields': {'properties': DictField()},
                'indexes': [('properties', 'properties.species')]
            }

        expected_specs = [{'fields': [('properties', 1), ('properties.species', 1)]}]
        self.assertEqual(expected_specs, Animal._meta['index_specs'])

    def test_abstract_dict(self):
        "Index on a field of an EmbeddedDocumentField in an abstract DictField"
        class Value(EmbeddedDocument):
            name = StringField()

        class Animal(Document):
            meta = {
                'abstract_fields': {'properties': DictField(field=EmbeddedDocumentField(Value))},
                'indexes': [('properties.name')]
            }

        expected_specs = [{'fields': [('properties.name', 1)]}]
        self.assertEqual(expected_specs, Animal._meta['index_specs'])

    def test_dict_abstract(self):
        "Index on an abstract field in an EmbeddedDocumentField in a DictField"
        class Value(EmbeddedDocument):
            meta = {
                'abstract_fields': {'name': StringField()}
            }

        class Animal(Document):
            properties = DictField(field=EmbeddedDocumentField(Value))

            meta = {
                'indexes': [('properties.name')]
            }

        expected_specs = [{'fields': [('properties.name', 1)]}]
        self.assertEqual(expected_specs, Animal._meta['index_specs'])

    def test_abstract_dict_abstract(self):
        """
        Index on an abstract field in an EmbeddedDocumentField in an
        abstract DictField
        """
        class Value(EmbeddedDocument):
            meta = {
                'abstract_fields': {'name': StringField()}
            }

        class Animal(Document):
            meta = {
                'abstract_fields': {'properties': DictField(field=EmbeddedDocumentField(Value))},
                'indexes': [('properties.name')]
            }

        expected_specs = [{'fields': [('properties.name', 1)]}]
        self.assertEqual(expected_specs, Animal._meta['index_specs'])


if __name__ == '__main__':
    unittest.main()
