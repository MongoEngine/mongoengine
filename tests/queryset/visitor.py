from __future__ import with_statement
import sys
sys.path[0:0] = [""]

import unittest

from bson import ObjectId
from datetime import datetime

from mongoengine import *
from mongoengine.queryset import Q
from mongoengine.errors import InvalidQueryError

__all__ = ("QTest",)


class QTest(unittest.TestCase):

    def setUp(self):
        connect(db='mongoenginetest')

        class Person(Document):
            name = StringField()
            age = IntField()
            meta = {'allow_inheritance': True}

        Person.drop_collection()
        self.Person = Person

    def test_empty_q(self):
        """Ensure that empty Q objects won't hurt.
        """
        q1 = Q()
        q2 = Q(age__gte=18)
        q3 = Q()
        q4 = Q(name='test')
        q5 = Q()

        class Person(Document):
            name = StringField()
            age = IntField()

        query = {'$or': [{'age': {'$gte': 18}}, {'name': 'test'}]}
        self.assertEqual((q1 | q2 | q3 | q4 | q5).to_query(Person), query)

        query = {'age': {'$gte': 18}, 'name': 'test'}
        self.assertEqual((q1 & q2 & q3 & q4 & q5).to_query(Person), query)

    def test_q_with_dbref(self):
        """Ensure Q objects handle DBRefs correctly"""
        connect(db='mongoenginetest')

        class User(Document):
            pass

        class Post(Document):
            created_user = ReferenceField(User)

        user = User.objects.create()
        Post.objects.create(created_user=user)

        self.assertEqual(Post.objects.filter(created_user=user).count(), 1)
        self.assertEqual(Post.objects.filter(Q(created_user=user)).count(), 1)

    def test_and_combination(self):
        """Ensure that Q-objects correctly AND together.
        """
        class TestDoc(Document):
            x = IntField()
            y = StringField()

        # Check than an error is raised when conflicting queries are anded
        def invalid_combination():
            query = Q(x__lt=7) & Q(x__lt=3)
            query.to_query(TestDoc)
        self.assertRaises(InvalidQueryError, invalid_combination)

        # Check normal cases work without an error
        query = Q(x__lt=7) & Q(x__gt=3)

        q1 = Q(x__lt=7)
        q2 = Q(x__gt=3)
        query = (q1 & q2).to_query(TestDoc)
        self.assertEqual(query, {'x': {'$lt': 7, '$gt': 3}})

        # More complex nested example
        query = Q(x__lt=100) & Q(y__ne='NotMyString')
        query &= Q(y__in=['a', 'b', 'c']) & Q(x__gt=-100)
        mongo_query = {
            'x': {'$lt': 100, '$gt': -100},
            'y': {'$ne': 'NotMyString', '$in': ['a', 'b', 'c']},
        }
        self.assertEqual(query.to_query(TestDoc), mongo_query)

    def test_or_combination(self):
        """Ensure that Q-objects correctly OR together.
        """
        class TestDoc(Document):
            x = IntField()

        q1 = Q(x__lt=3)
        q2 = Q(x__gt=7)
        query = (q1 | q2).to_query(TestDoc)
        self.assertEqual(query, {
            '$or': [
                {'x': {'$lt': 3}},
                {'x': {'$gt': 7}},
            ]
        })

    def test_and_or_combination(self):
        """Ensure that Q-objects handle ANDing ORed components.
        """
        class TestDoc(Document):
            x = IntField()
            y = BooleanField()

        query = (Q(x__gt=0) | Q(x__exists=False))
        query &= Q(x__lt=100)
        self.assertEqual(query.to_query(TestDoc), {
            '$or': [
                {'x': {'$lt': 100, '$gt': 0}},
                {'x': {'$lt': 100, '$exists': False}},
            ]
        })

        q1 = (Q(x__gt=0) | Q(x__exists=False))
        q2 = (Q(x__lt=100) | Q(y=True))
        query = (q1 & q2).to_query(TestDoc)

        self.assertEqual(['$or'], query.keys())
        conditions = [
            {'x': {'$lt': 100, '$gt': 0}},
            {'x': {'$lt': 100, '$exists': False}},
            {'x': {'$gt': 0}, 'y': True},
            {'x': {'$exists': False}, 'y': True},
        ]
        self.assertEqual(len(conditions), len(query['$or']))
        for condition in conditions:
            self.assertTrue(condition in query['$or'])

    def test_or_and_or_combination(self):
        """Ensure that Q-objects handle ORing ANDed ORed components. :)
        """
        class TestDoc(Document):
            x = IntField()
            y = BooleanField()

        q1 = (Q(x__gt=0) & (Q(y=True) | Q(y__exists=False)))
        q2 = (Q(x__lt=100) & (Q(y=False) | Q(y__exists=False)))
        query = (q1 | q2).to_query(TestDoc)

        self.assertEqual(['$or'], query.keys())
        conditions = [
            {'x': {'$gt': 0}, 'y': True},
            {'x': {'$gt': 0}, 'y': {'$exists': False}},
            {'x': {'$lt': 100}, 'y':False},
            {'x': {'$lt': 100}, 'y': {'$exists': False}},
        ]
        self.assertEqual(len(conditions), len(query['$or']))
        for condition in conditions:
            self.assertTrue(condition in query['$or'])

    def test_q_clone(self):

        class TestDoc(Document):
            x = IntField()

        TestDoc.drop_collection()
        for i in xrange(1, 101):
            t = TestDoc(x=i)
            t.save()

        # Check normal cases work without an error
        test = TestDoc.objects(Q(x__lt=7) & Q(x__gt=3))

        self.assertEqual(test.count(), 3)

        test2 = test.clone()
        self.assertEqual(test2.count(), 3)
        self.assertFalse(test2 == test)

        test2.filter(x=6)
        self.assertEqual(test2.count(), 1)
        self.assertEqual(test.count(), 3)

    def test_q(self):
        """Ensure that Q objects may be used to query for documents.
        """
        class BlogPost(Document):
            title = StringField()
            publish_date = DateTimeField()
            published = BooleanField()

        BlogPost.drop_collection()

        post1 = BlogPost(title='Test 1', publish_date=datetime(2010, 1, 8), published=False)
        post1.save()

        post2 = BlogPost(title='Test 2', publish_date=datetime(2010, 1, 15), published=True)
        post2.save()

        post3 = BlogPost(title='Test 3', published=True)
        post3.save()

        post4 = BlogPost(title='Test 4', publish_date=datetime(2010, 1, 8))
        post4.save()

        post5 = BlogPost(title='Test 1', publish_date=datetime(2010, 1, 15))
        post5.save()

        post6 = BlogPost(title='Test 1', published=False)
        post6.save()

        # Check ObjectId lookup works
        obj = BlogPost.objects(id=post1.id).first()
        self.assertEqual(obj, post1)

        # Check Q object combination with one does not exist
        q = BlogPost.objects(Q(title='Test 5') | Q(published=True))
        posts = [post.id for post in q]

        published_posts = (post2, post3)
        self.assertTrue(all(obj.id in posts for obj in published_posts))

        q = BlogPost.objects(Q(title='Test 1') | Q(published=True))
        posts = [post.id for post in q]
        published_posts = (post1, post2, post3, post5, post6)
        self.assertTrue(all(obj.id in posts for obj in published_posts))

        # Check Q object combination
        date = datetime(2010, 1, 10)
        q = BlogPost.objects(Q(publish_date__lte=date) | Q(published=True))
        posts = [post.id for post in q]

        published_posts = (post1, post2, post3, post4)
        self.assertTrue(all(obj.id in posts for obj in published_posts))

        self.assertFalse(any(obj.id in posts for obj in [post5, post6]))

        BlogPost.drop_collection()

        # Check the 'in' operator
        self.Person(name='user1', age=20).save()
        self.Person(name='user2', age=20).save()
        self.Person(name='user3', age=30).save()
        self.Person(name='user4', age=40).save()

        self.assertEqual(len(self.Person.objects(Q(age__in=[20]))), 2)
        self.assertEqual(len(self.Person.objects(Q(age__in=[20, 30]))), 3)

        # Test invalid query objs
        def wrong_query_objs():
            self.Person.objects('user1')
        def wrong_query_objs_filter():
            self.Person.objects('user1')
        self.assertRaises(InvalidQueryError, wrong_query_objs)
        self.assertRaises(InvalidQueryError, wrong_query_objs_filter)

    def test_q_regex(self):
        """Ensure that Q objects can be queried using regexes.
        """
        person = self.Person(name='Guido van Rossum')
        person.save()

        import re
        obj = self.Person.objects(Q(name=re.compile('^Gui'))).first()
        self.assertEqual(obj, person)
        obj = self.Person.objects(Q(name=re.compile('^gui'))).first()
        self.assertEqual(obj, None)

        obj = self.Person.objects(Q(name=re.compile('^gui', re.I))).first()
        self.assertEqual(obj, person)

        obj = self.Person.objects(Q(name__not=re.compile('^bob'))).first()
        self.assertEqual(obj, person)

        obj = self.Person.objects(Q(name__not=re.compile('^Gui'))).first()
        self.assertEqual(obj, None)

    def test_q_lists(self):
        """Ensure that Q objects query ListFields correctly.
        """
        class BlogPost(Document):
            tags = ListField(StringField())

        BlogPost.drop_collection()

        BlogPost(tags=['python', 'mongo']).save()
        BlogPost(tags=['python']).save()

        self.assertEqual(len(BlogPost.objects(Q(tags='mongo'))), 1)
        self.assertEqual(len(BlogPost.objects(Q(tags='python'))), 2)

        BlogPost.drop_collection()

    def test_q_merge_queries_edge_case(self):

        class User(Document):
            email = EmailField(required=False)
            name = StringField()

        User.drop_collection()
        pk = ObjectId()
        User(email='example@example.com', pk=pk).save()

        self.assertEqual(1, User.objects.filter(
                                Q(email='example@example.com') |
                                Q(name='John Doe')
                                ).limit(2).filter(pk=pk).count())

if __name__ == '__main__':
    unittest.main()
