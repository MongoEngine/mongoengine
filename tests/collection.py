import unittest
import pymongo

from mongomap.collection import CollectionManager
from mongomap import *


class CollectionManagerTest(unittest.TestCase):
    
    def setUp(self):
        connect(db='mongotest')

        class Person(Document):
            name = StringField()
            age = IntField()
        self.Person = Person

    def test_initialisation(self):
        """Ensure that CollectionManager is correctly initialised.
        """
        class Person(Document):
            name = StringField()
            age = IntField()

        self.assertTrue(isinstance(Person.collection, CollectionManager))
        self.assertEqual(Person.collection._collection_name, 
                         Person._meta['collection'])
        self.assertTrue(isinstance(Person.collection._collection,
                                   pymongo.collection.Collection))


if __name__ == '__main__':
    unittest.main()
