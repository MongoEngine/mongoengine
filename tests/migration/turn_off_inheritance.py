# -*- coding: utf-8 -*-
import unittest

from mongoengine import Document, connect
from mongoengine.connection import get_db
from mongoengine.fields import StringField

__all__ = ('TurnOffInheritanceTest', )


class TurnOffInheritanceTest(unittest.TestCase):

    def setUp(self):
        connect(db='mongoenginetest')
        self.db = get_db()

    def tearDown(self):
        for collection in self.db.collection_names():
            if 'system.' in collection:
                continue
            self.db.drop_collection(collection)

    def test_how_to_turn_off_inheritance(self):
        """Demonstrates migrating from allow_inheritance = True to False.
        """

        # 1. Old declaration of the class

        class Animal(Document):
            name = StringField()
            meta = {
                'allow_inheritance': True,
                'indexes': ['name']
            }

        # 2. Turn off inheritance
        class Animal(Document):
            name = StringField()
            meta = {
                'allow_inheritance': False,
                'indexes': ['name']
            }

        # 3. Remove _types and _cls
        collection = Animal._get_collection()
        collection.update({}, {"$unset": {"_types": 1, "_cls": 1}}, multi=True)

        # 3. Confirm extra data is removed
        count = collection.find({"$or": [{'_types': {"$exists": True}},
                                         {'_cls': {"$exists": True}}]}).count()
        assert count == 0

        # 4. Remove indexes
        info = collection.index_information()
        indexes_to_drop = [key for key, value in info.iteritems()
                           if '_types' in dict(value['key'])
                              or '_cls' in dict(value['key'])]
        for index in indexes_to_drop:
            collection.drop_index(index)

        # 5. Recreate indexes
        Animal.ensure_indexes()
