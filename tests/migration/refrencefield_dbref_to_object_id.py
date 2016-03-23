# -*- coding: utf-8 -*-
import unittest

from mongoengine import Document, connect
from mongoengine.connection import get_db
from mongoengine.fields import StringField, ReferenceField, ListField

__all__ = ('ConvertToObjectIdsModel', )


class ConvertToObjectIdsModel(unittest.TestCase):

    def setUp(self):
        connect(db='mongoenginetest')
        self.db = get_db()

    def test_how_to_convert_to_object_id_reference_fields(self):
        """Demonstrates migrating from 0.7 to 0.8
        """

        # 1. Old definition - using dbrefs
        class Person(Document):
            name = StringField()
            parent = ReferenceField('self', dbref=True)
            friends = ListField(ReferenceField('self', dbref=True))

        Person.drop_collection()

        p1 = Person(name="Wilson", parent=None).save()
        f1 = Person(name="John", parent=None).save()
        f2 = Person(name="Paul", parent=None).save()
        f3 = Person(name="George", parent=None).save()
        f4 = Person(name="Ringo", parent=None).save()
        Person(name="Wilson Jr", parent=p1, friends=[f1, f2, f3, f4]).save()

        # 2. Start the migration by changing the schema
        # Change ReferenceField as now dbref defaults to False
        class Person(Document):
            name = StringField()
            parent = ReferenceField('self')
            friends = ListField(ReferenceField('self'))

        # 3. Loop all the objects and mark parent as changed
        for p in Person.objects:
            p._mark_as_changed('parent')
            p._mark_as_changed('friends')
            p.save()

        # 4. Confirmation of the fix!
        wilson = Person.objects(name="Wilson Jr").as_pymongo()[0]
        self.assertEqual(p1.id, wilson['parent'])
        self.assertEqual([f1.id, f2.id, f3.id, f4.id], wilson['friends'])
