# -*- coding: utf-8 -*-
import unittest
import uuid

from mongoengine import Document, connect
from mongoengine.connection import get_db
from mongoengine.fields import StringField, UUIDField, ListField

__all__ = ('ConvertToBinaryUUID', )


class ConvertToBinaryUUID(unittest.TestCase):

    def setUp(self):
        connect(db='mongoenginetest')
        self.db = get_db()

    def test_how_to_convert_to_binary_uuid_fields(self):
        """Demonstrates migrating from 0.7 to 0.8
        """

        # 1. Old definition - using dbrefs
        class Person(Document):
            name = StringField()
            uuid = UUIDField(binary=False)
            uuids = ListField(UUIDField(binary=False))

        Person.drop_collection()
        Person(name="Wilson Jr", uuid=uuid.uuid4(),
               uuids=[uuid.uuid4(), uuid.uuid4()]).save()

        # 2. Start the migration by changing the schema
        # Change UUIDFIeld as now binary defaults to True
        class Person(Document):
            name = StringField()
            uuid = UUIDField()
            uuids = ListField(UUIDField())

        # 3. Loop all the objects and mark parent as changed
        for p in Person.objects:
            p._mark_as_changed('uuid')
            p._mark_as_changed('uuids')
            p.save()

        # 4. Confirmation of the fix!
        wilson = Person.objects(name="Wilson Jr").as_pymongo()[0]
        self.assertTrue(isinstance(wilson['uuid'], uuid.UUID))
        self.assertTrue(all([isinstance(u, uuid.UUID) for u in wilson['uuids']]))
