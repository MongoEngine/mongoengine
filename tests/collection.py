import unittest
import pymongo

from mongomap.collection import CollectionManager
from mongomap.connection import _get_db
from mongomap import *


class CollectionManagerTest(unittest.TestCase):
    
    def setUp(self):
        connect(db='mongotest')

        class Person(Document):
            name = StringField()
            age = IntField()
        self.Person = Person

        self.db = _get_db()
        self.db.drop_collection(self.Person._meta['collection'])

    def test_initialisation(self):
        """Ensure that CollectionManager is correctly initialised.
        """
        self.assertTrue(isinstance(self.Person.objects, CollectionManager))
        self.assertEqual(self.Person.objects._collection_name, 
                         self.Person._meta['collection'])
        self.assertTrue(isinstance(self.Person.objects._collection,
                                   pymongo.collection.Collection))

    def test_transform_query(self):
        """Ensure that the _transform_query function operates correctly.
        """
        manager = self.Person().objects
        self.assertEqual(manager._transform_query(name='test', age=30),
                         {'name': 'test', 'age': 30})
        self.assertEqual(manager._transform_query(age__lt=30), 
                         {'age': {'$lt': 30}})
        self.assertEqual(manager._transform_query(friend__age__gte=30), 
                         {'friend.age': {'$gte': 30}})
        self.assertEqual(manager._transform_query(name__exists=True), 
                         {'name': {'$exists': True}})

    def test_find(self):
        """Ensure that a query returns a valid set of results.
        """
        person1 = self.Person(name="User A", age=20)
        person1.save()
        person2 = self.Person(name="User B", age=30)
        person2.save()

        # Find all people in the collection
        people = self.Person.objects.find()
        self.assertEqual(people.count(), 2)
        results = list(people)
        self.assertTrue(isinstance(results[0], self.Person))
        self.assertEqual(results[0].name, "User A")
        self.assertEqual(results[0].age, 20)
        self.assertEqual(results[1].name, "User B")
        self.assertEqual(results[1].age, 30)

        # Use a query to filter the people found to just person1
        people = self.Person.objects.find(age=20)
        self.assertEqual(people.count(), 1)
        person = people.next()
        self.assertEqual(person.name, "User A")
        self.assertEqual(person.age, 20)

        # Test limit
        people = list(self.Person.objects.find().limit(1))
        self.assertEqual(len(people), 1)
        self.assertEqual(people[0].name, 'User A')

        # Test skip
        people = list(self.Person.objects.find().skip(1))
        self.assertEqual(len(people), 1)
        self.assertEqual(people[0].name, 'User B')

    def test_find_one(self):
        """Ensure that a query using find_one returns a valid result.
        """
        person1 = self.Person(name="User A", age=20)
        person1.save()
        person2 = self.Person(name="User B", age=30)
        person2.save()

        # Retrieve the first person from the database
        person = self.Person.objects.find_one()
        self.assertTrue(isinstance(person, self.Person))
        self.assertEqual(person.name, "User A")
        self.assertEqual(person.age, 20)

        # Use a query to filter the people found to just person2
        person = self.Person.objects.find_one(age=30)
        self.assertEqual(person.name, "User B")

        person = self.Person.objects.find_one(age__lt=30)
        self.assertEqual(person.name, "User A")

    def test_find_embedded(self):
        """Ensure that an embedded document is properly returned from a query.
        """
        class User(EmbeddedDocument):
            name = StringField()

        class BlogPost(Document):
            content = StringField()
            author = EmbeddedDocumentField(User)

        post = BlogPost(content='Had a good coffee today...')
        post.author = User(name='Test User')
        post.save()

        result = BlogPost.objects.find_one()
        self.assertTrue(isinstance(result.author, User))
        self.assertEqual(result.author.name, 'Test User')


if __name__ == '__main__':
    unittest.main()
