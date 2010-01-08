import unittest
import pymongo

from mongoengine.queryset import QuerySet
from mongoengine import *


class QuerySetTest(unittest.TestCase):
    
    def setUp(self):
        connect(db='mongoenginetest')

        class Person(Document):
            name = StringField()
            age = IntField()
        self.Person = Person

    def test_initialisation(self):
        """Ensure that CollectionManager is correctly initialised.
        """
        self.assertTrue(isinstance(self.Person.objects, QuerySet))
        self.assertEqual(self.Person.objects._collection.name(), 
                         self.Person._meta['collection'])
        self.assertTrue(isinstance(self.Person.objects._collection,
                                   pymongo.collection.Collection))

    def test_transform_query(self):
        """Ensure that the _transform_query function operates correctly.
        """
        self.assertEqual(QuerySet._transform_query(name='test', age=30),
                         {'name': 'test', 'age': 30})
        self.assertEqual(QuerySet._transform_query(age__lt=30), 
                         {'age': {'$lt': 30}})
        self.assertEqual(QuerySet._transform_query(age__gt=20, age__lt=50),
                         {'age': {'$gt': 20, '$lt': 50}})
        self.assertEqual(QuerySet._transform_query(age=20, age__gt=50),
                         {'age': 20})
        self.assertEqual(QuerySet._transform_query(friend__age__gte=30), 
                         {'friend.age': {'$gte': 30}})
        self.assertEqual(QuerySet._transform_query(name__exists=True), 
                         {'name': {'$exists': True}})

    def test_find(self):
        """Ensure that a query returns a valid set of results.
        """
        person1 = self.Person(name="User A", age=20)
        person1.save()
        person2 = self.Person(name="User B", age=30)
        person2.save()

        # Find all people in the collection
        people = self.Person.objects
        self.assertEqual(len(people), 2)
        results = list(people)
        self.assertTrue(isinstance(results[0], self.Person))
        self.assertTrue(isinstance(results[0].id, (pymongo.objectid.ObjectId,
                                                    str, unicode)))
        self.assertEqual(results[0].name, "User A")
        self.assertEqual(results[0].age, 20)
        self.assertEqual(results[1].name, "User B")
        self.assertEqual(results[1].age, 30)

        # Use a query to filter the people found to just person1
        people = self.Person.objects(age=20)
        self.assertEqual(len(people), 1)
        person = people.next()
        self.assertEqual(person.name, "User A")
        self.assertEqual(person.age, 20)

        # Test limit
        people = list(self.Person.objects.limit(1))
        self.assertEqual(len(people), 1)
        self.assertEqual(people[0].name, 'User A')

        # Test skip
        people = list(self.Person.objects.skip(1))
        self.assertEqual(len(people), 1)
        self.assertEqual(people[0].name, 'User B')

        person3 = self.Person(name="User C", age=40)
        person3.save()

        # Test slice limit
        people = list(self.Person.objects[:2])
        self.assertEqual(len(people), 2)
        self.assertEqual(people[0].name, 'User A')
        self.assertEqual(people[1].name, 'User B')

        # Test slice skip
        people = list(self.Person.objects[1:])
        self.assertEqual(len(people), 2)
        self.assertEqual(people[0].name, 'User B')
        self.assertEqual(people[1].name, 'User C')

        # Test slice limit and skip
        people = list(self.Person.objects[1:2])
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
        person = self.Person.objects.first()
        self.assertTrue(isinstance(person, self.Person))
        self.assertEqual(person.name, "User A")
        self.assertEqual(person.age, 20)

        # Use a query to filter the people found to just person2
        person = self.Person.objects(age=30).first()
        self.assertEqual(person.name, "User B")

        person = self.Person.objects(age__lt=30).first()
        self.assertEqual(person.name, "User A")

        # Use array syntax
        person = self.Person.objects[0]
        self.assertEqual(person.name, "User A")

        person = self.Person.objects[1]
        self.assertEqual(person.name, "User B")

        self.assertRaises(IndexError, self.Person.objects.__getitem__, 2)
        
        # Find a document using just the object id
        person = self.Person.objects.with_id(person1.id)
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

        result = BlogPost.objects.first()
        self.assertTrue(isinstance(result.author, User))
        self.assertEqual(result.author.name, 'Test User')
        
        BlogPost.drop_collection()

    def test_delete(self):
        """Ensure that documents are properly deleted from the database.
        """
        self.Person(name="User A", age=20).save()
        self.Person(name="User B", age=30).save()
        self.Person(name="User C", age=40).save()

        self.assertEqual(len(self.Person.objects), 3)

        self.Person.objects(age__lt=30).delete()
        self.assertEqual(len(self.Person.objects), 2)

        self.Person.objects.delete()
        self.assertEqual(len(self.Person.objects), 0)

    def test_order_by(self):
        """Ensure that QuerySets may be ordered.
        """
        self.Person(name="User A", age=20).save()
        self.Person(name="User B", age=40).save()
        self.Person(name="User C", age=30).save()

        names = [p.name for p in self.Person.objects.order_by('-age')]
        self.assertEqual(names, ['User B', 'User C', 'User A'])

        names = [p.name for p in self.Person.objects.order_by('+age')]
        self.assertEqual(names, ['User A', 'User C', 'User B'])

        names = [p.name for p in self.Person.objects.order_by('age')]
        self.assertEqual(names, ['User A', 'User C', 'User B'])
        
        ages = [p.age for p in self.Person.objects.order_by('-name')]
        self.assertEqual(ages, [30, 40, 20])

    def test_item_frequencies(self):
        """Ensure that item frequencies are properly generated from lists.
        """
        class BlogPost(Document):
            hits = IntField()
            tags = ListField(StringField(), name='blogTags')

        BlogPost.drop_collection()

        BlogPost(hits=1, tags=['music', 'film', 'actors']).save()
        BlogPost(hits=2, tags=['music']).save()
        BlogPost(hits=3, tags=['music', 'actors']).save()

        f = BlogPost.objects.item_frequencies('tags')
        f = dict((key, int(val)) for key, val in f.items())
        self.assertEqual(set(['music', 'film', 'actors']), set(f.keys()))
        self.assertEqual(f['music'], 3)
        self.assertEqual(f['actors'], 2)
        self.assertEqual(f['film'], 1)

        # Ensure query is taken into account
        f = BlogPost.objects(hits__gt=1).item_frequencies('tags')
        f = dict((key, int(val)) for key, val in f.items())
        self.assertEqual(set(['music', 'actors']), set(f.keys()))
        self.assertEqual(f['music'], 2)
        self.assertEqual(f['actors'], 1)

        # Check that normalization works
        f = BlogPost.objects.item_frequencies('tags', normalize=True)
        self.assertAlmostEqual(f['music'], 3.0/6.0)
        self.assertAlmostEqual(f['actors'], 2.0/6.0)
        self.assertAlmostEqual(f['film'], 1.0/6.0)

        BlogPost.drop_collection()

    def test_average(self):
        """Ensure that field can be averaged correctly.
        """
        ages = [23, 54, 12, 94, 27]
        for i, age in enumerate(ages):
            self.Person(name='test%s' % i, age=age).save()

        avg = float(sum(ages)) / len(ages)
        self.assertAlmostEqual(int(self.Person.objects.average('age')), avg)

        self.Person(name='ageless person').save()
        self.assertEqual(int(self.Person.objects.average('age')), avg)

    def test_sum(self):
        """Ensure that field can be summed over correctly.
        """
        ages = [23, 54, 12, 94, 27]
        for i, age in enumerate(ages):
            self.Person(name='test%s' % i, age=age).save()

        self.assertEqual(int(self.Person.objects.sum('age')), sum(ages))

        self.Person(name='ageless person').save()
        self.assertEqual(int(self.Person.objects.sum('age')), sum(ages))

    def test_custom_manager(self):
        """Ensure that custom QuerySetManager instances work as expected.
        """
        class BlogPost(Document):
            tags = ListField(StringField())

            @queryset_manager
            def music_posts(queryset):
                return queryset(tags='music')

        BlogPost.drop_collection()

        post1 = BlogPost(tags=['music', 'film'])
        post1.save()
        post2 = BlogPost(tags=['music'])
        post2.save()
        post3 = BlogPost(tags=['film', 'actors'])
        post3.save()

        self.assertEqual([p.id for p in BlogPost.objects],
                         [post1.id, post2.id, post3.id])
        self.assertEqual([p.id for p in BlogPost.music_posts],
                         [post1.id, post2.id])

        BlogPost.drop_collection()

    def test_query_field_name(self):
        """Ensure that the correct field name is used when querying.
        """
        class Comment(EmbeddedDocument):
            content = StringField(name='commentContent')

        class BlogPost(Document):
            title = StringField(name='postTitle')
            comments = ListField(EmbeddedDocumentField(Comment),
                                 name='postComments')
                                 

        BlogPost.drop_collection()

        data = {'title': 'Post 1', 'comments': [Comment(content='test')]}
        BlogPost(**data).save()

        self.assertTrue('postTitle' in 
                        BlogPost.objects(title=data['title'])._query)
        self.assertFalse('title' in 
                         BlogPost.objects(title=data['title'])._query)
        self.assertEqual(len(BlogPost.objects(title=data['title'])), 1)

        self.assertTrue('postComments.commentContent' in 
                        BlogPost.objects(comments__content='test')._query)
        self.assertEqual(len(BlogPost.objects(comments__content='test')), 1)

        BlogPost.drop_collection()

    def test_query_value_conversion(self):
        """Ensure that query values are properly converted when necessary.
        """
        class BlogPost(Document):
            author = ReferenceField(self.Person)

        BlogPost.drop_collection()

        person = self.Person(name='test', age=30)
        person.save()

        post = BlogPost(author=person)
        post.save()

        # Test that query may be performed by providing a document as a value
        # while using a ReferenceField's name - the document should be 
        # converted to an DBRef, which is legal, unlike a Document object
        post_obj = BlogPost.objects(author=person).first()
        self.assertEqual(post.id, post_obj.id)

        # Test that lists of values work when using the 'in', 'nin' and 'all'
        post_obj = BlogPost.objects(author__in=[person]).first()
        self.assertEqual(post.id, post_obj.id)

        BlogPost.drop_collection()


    def test_types_index(self):
        """Ensure that and index is used when '_types' is being used in a
        query.
        """
        # Indexes are lazy so use list() to perform query
        list(self.Person.objects)
        info = self.Person.objects._collection.index_information()
        self.assertTrue([('_types', 1)] in info.values())

        class BlogPost(Document):
            title = StringField()
            meta = {'allow_inheritance': False}

        # _types is not used on objects where allow_inheritance is False
        list(BlogPost.objects)
        info = BlogPost.objects._collection.index_information()
        self.assertFalse([('_types', 1)] in info.values())

        BlogPost.drop_collection()

    def tearDown(self):
        self.Person.drop_collection()


if __name__ == '__main__':
    unittest.main()
