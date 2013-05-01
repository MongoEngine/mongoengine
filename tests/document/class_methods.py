# -*- coding: utf-8 -*-
import sys
sys.path[0:0] = [""]
import unittest

from mongoengine import *

from mongoengine.queryset import NULLIFY
from mongoengine.connection import get_db

__all__ = ("ClassMethodsTest", )


class ClassMethodsTest(unittest.TestCase):

    def setUp(self):
        connect(db='mongoenginetest')
        self.db = get_db()

        class Person(Document):
            name = StringField()
            age = IntField()

            non_field = True

            meta = {"allow_inheritance": True}

        self.Person = Person

    def tearDown(self):
        for collection in self.db.collection_names():
            if 'system.' in collection:
                continue
            self.db.drop_collection(collection)

    def test_definition(self):
        """Ensure that document may be defined using fields.
        """
        self.assertEqual(['age', 'id', 'name'],
                         sorted(self.Person._fields.keys()))
        self.assertEqual(["IntField", "ObjectIdField", "StringField"],
                        sorted([x.__class__.__name__ for x in
                                self.Person._fields.values()]))

    def test_get_db(self):
        """Ensure that get_db returns the expected db.
        """
        db = self.Person._get_db()
        self.assertEqual(self.db, db)

    def test_get_collection_name(self):
        """Ensure that get_collection_name returns the expected collection
        name.
        """
        collection_name = 'person'
        self.assertEqual(collection_name, self.Person._get_collection_name())

    def test_get_collection(self):
        """Ensure that get_collection returns the expected collection.
        """
        collection_name = 'person'
        collection = self.Person._get_collection()
        self.assertEqual(self.db[collection_name], collection)

    def test_drop_collection(self):
        """Ensure that the collection may be dropped from the database.
        """
        collection_name = 'person'
        self.Person(name='Test').save()
        self.assertTrue(collection_name in self.db.collection_names())

        self.Person.drop_collection()
        self.assertFalse(collection_name in self.db.collection_names())

    def test_register_delete_rule(self):
        """Ensure that register delete rule adds a delete rule to the document
        meta.
        """
        class Job(Document):
            employee = ReferenceField(self.Person)

        self.assertEqual(self.Person._meta.get('delete_rules'), None)

        self.Person.register_delete_rule(Job, 'employee', NULLIFY)
        self.assertEqual(self.Person._meta['delete_rules'],
                         {(Job, 'employee'): NULLIFY})

    def test_collection_naming(self):
        """Ensure that a collection with a specified name may be used.
        """

        class DefaultNamingTest(Document):
            pass
        self.assertEqual('default_naming_test',
                         DefaultNamingTest._get_collection_name())

        class CustomNamingTest(Document):
            meta = {'collection': 'pimp_my_collection'}

        self.assertEqual('pimp_my_collection',
                         CustomNamingTest._get_collection_name())

        class DynamicNamingTest(Document):
            meta = {'collection': lambda c: "DYNAMO"}
        self.assertEqual('DYNAMO', DynamicNamingTest._get_collection_name())

        # Use Abstract class to handle backwards compatibility
        class BaseDocument(Document):
            meta = {
                'abstract': True,
                'collection': lambda c: c.__name__.lower()
            }

        class OldNamingConvention(BaseDocument):
            pass
        self.assertEqual('oldnamingconvention',
                         OldNamingConvention._get_collection_name())

        class InheritedAbstractNamingTest(BaseDocument):
            meta = {'collection': 'wibble'}
        self.assertEqual('wibble',
                         InheritedAbstractNamingTest._get_collection_name())

        # Mixin tests
        class BaseMixin(object):
            meta = {
                'collection': lambda c: c.__name__.lower()
            }

        class OldMixinNamingConvention(Document, BaseMixin):
            pass
        self.assertEqual('oldmixinnamingconvention',
                          OldMixinNamingConvention._get_collection_name())

        class BaseMixin(object):
            meta = {
                'collection': lambda c: c.__name__.lower()
            }

        class BaseDocument(Document, BaseMixin):
            meta = {'allow_inheritance': True}

        class MyDocument(BaseDocument):
            pass

        self.assertEqual('basedocument', MyDocument._get_collection_name())

    def test_custom_collection_name_operations(self):
        """Ensure that a collection with a specified name is used as expected.
        """
        collection_name = 'personCollTest'

        class Person(Document):
            name = StringField()
            meta = {'collection': collection_name}

        Person(name="Test User").save()
        self.assertTrue(collection_name in self.db.collection_names())

        user_obj = self.db[collection_name].find_one()
        self.assertEqual(user_obj['name'], "Test User")

        user_obj = Person.objects[0]
        self.assertEqual(user_obj.name, "Test User")

        Person.drop_collection()
        self.assertFalse(collection_name in self.db.collection_names())

    def test_collection_name_and_primary(self):
        """Ensure that a collection with a specified name may be used.
        """

        class Person(Document):
            name = StringField(primary_key=True)
            meta = {'collection': 'app'}

        Person(name="Test User").save()

        user_obj = Person.objects.first()
        self.assertEqual(user_obj.name, "Test User")

        Person.drop_collection()


if __name__ == '__main__':
    unittest.main()
