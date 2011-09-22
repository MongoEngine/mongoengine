# -*- coding: utf-8 -*-
import unittest
import pymongo
from datetime import datetime, timedelta

from mongoengine.queryset import (QuerySet, QuerySetManager,
                                  MultipleObjectsReturned, DoesNotExist,
                                  QueryFieldList)
from mongoengine import *
from mongoengine.connection import _get_connection
from mongoengine.tests import query_counter


class QuerySetTest(unittest.TestCase):

    def setUp(self):
        connect(db='mongoenginetest')
        
        class Person(Document):
            name = StringField()
            age = IntField()
        self.Person = Person

    def test_initialisation(self):
        """Ensure that a QuerySet is correctly initialised by QuerySetManager.
        """
        self.assertTrue(isinstance(self.Person.objects, QuerySet))
        self.assertEqual(self.Person.objects._collection.name,
                         self.Person._get_collection_name())
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

        people = list(self.Person.objects[1:1])
        self.assertEqual(len(people), 0)

        # Test slice out of range
        people = list(self.Person.objects[80000:80001])
        self.assertEqual(len(people), 0)

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

    def test_find_only_one(self):
        """Ensure that a query using ``get`` returns at most one result.
        """
        # Try retrieving when no objects exists
        self.assertRaises(DoesNotExist, self.Person.objects.get)
        self.assertRaises(self.Person.DoesNotExist, self.Person.objects.get)

        person1 = self.Person(name="User A", age=20)
        person1.save()
        person2 = self.Person(name="User B", age=30)
        person2.save()

        # Retrieve the first person from the database
        self.assertRaises(MultipleObjectsReturned, self.Person.objects.get)
        self.assertRaises(self.Person.MultipleObjectsReturned,
                          self.Person.objects.get)

        # Use a query to filter the people found to just person2
        person = self.Person.objects.get(age=30)
        self.assertEqual(person.name, "User B")

        person = self.Person.objects.get(age__lt=30)
        self.assertEqual(person.name, "User A")

    def test_find_array_position(self):
        """Ensure that query by array position works.
        """
        class Comment(EmbeddedDocument):
            name = StringField()

        class Post(EmbeddedDocument):
            comments = ListField(EmbeddedDocumentField(Comment))

        class Blog(Document):
            tags = ListField(StringField())
            posts = ListField(EmbeddedDocumentField(Post))

        Blog.drop_collection()

        Blog.objects.create(tags=['a', 'b'])
        self.assertEqual(len(Blog.objects(tags__0='a')), 1)
        self.assertEqual(len(Blog.objects(tags__0='b')), 0)
        self.assertEqual(len(Blog.objects(tags__1='a')), 0)
        self.assertEqual(len(Blog.objects(tags__1='b')), 1)

        Blog.drop_collection()

        comment1 = Comment(name='testa')
        comment2 = Comment(name='testb')
        post1 = Post(comments=[comment1, comment2])
        post2 = Post(comments=[comment2, comment2])
        blog1 = Blog.objects.create(posts=[post1, post2])
        blog2 = Blog.objects.create(posts=[post2, post1])

        blog = Blog.objects(posts__0__comments__0__name='testa').get()
        self.assertEqual(blog, blog1)

        query = Blog.objects(posts__1__comments__1__name='testb')
        self.assertEqual(len(query), 2)

        query = Blog.objects(posts__1__comments__1__name='testa')
        self.assertEqual(len(query), 0)

        query = Blog.objects(posts__0__comments__1__name='testa')
        self.assertEqual(len(query), 0)

        Blog.drop_collection()

    def test_update_write_options(self):
        """Test that passing write_options works"""

        self.Person.drop_collection()

        write_options = {"fsync": True}

        author, created = self.Person.objects.get_or_create(
                            name='Test User', write_options=write_options)
        author.save(write_options=write_options)

        self.Person.objects.update(set__name='Ross', write_options=write_options)

        author = self.Person.objects.first()
        self.assertEquals(author.name, 'Ross')

        self.Person.objects.update_one(set__name='Test User', write_options=write_options)
        author = self.Person.objects.first()
        self.assertEquals(author.name, 'Test User')

    def test_update_update_has_a_value(self):
        """Test to ensure that update is passed a value to update to"""
        self.Person.drop_collection()

        author = self.Person(name='Test User')
        author.save()

        def update_raises():
            self.Person.objects(pk=author.pk).update({})

        def update_one_raises():
            self.Person.objects(pk=author.pk).update_one({})

        self.assertRaises(OperationError, update_raises)
        self.assertRaises(OperationError, update_one_raises)

    def test_update_array_position(self):
        """Ensure that updating by array position works.

        Check update() and update_one() can take syntax like:
            set__posts__1__comments__1__name="testc"
        Check that it only works for ListFields.
        """
        class Comment(EmbeddedDocument):
            name = StringField()

        class Post(EmbeddedDocument):
            comments = ListField(EmbeddedDocumentField(Comment))

        class Blog(Document):
            tags = ListField(StringField())
            posts = ListField(EmbeddedDocumentField(Post))

        Blog.drop_collection()

        comment1 = Comment(name='testa')
        comment2 = Comment(name='testb')
        post1 = Post(comments=[comment1, comment2])
        post2 = Post(comments=[comment2, comment2])
        blog1 = Blog.objects.create(posts=[post1, post2])
        blog2 = Blog.objects.create(posts=[post2, post1])

        # Update all of the first comments of second posts of all blogs
        blog = Blog.objects().update(set__posts__1__comments__0__name="testc")
        testc_blogs = Blog.objects(posts__1__comments__0__name="testc")
        self.assertEqual(len(testc_blogs), 2)

        Blog.drop_collection()

        blog1 = Blog.objects.create(posts=[post1, post2])
        blog2 = Blog.objects.create(posts=[post2, post1])

        # Update only the first blog returned by the query
        blog = Blog.objects().update_one(
            set__posts__1__comments__1__name="testc")
        testc_blogs = Blog.objects(posts__1__comments__1__name="testc")
        self.assertEqual(len(testc_blogs), 1)

        # Check that using this indexing syntax on a non-list fails
        def non_list_indexing():
            Blog.objects().update(set__posts__1__comments__0__name__1="asdf")
        self.assertRaises(InvalidQueryError, non_list_indexing)

        Blog.drop_collection()

    def test_update_using_positional_operator(self):
        """Ensure that the list fields can be updated using the positional
        operator."""

        class Comment(EmbeddedDocument):
            by = StringField()
            votes = IntField()

        class BlogPost(Document):
            title = StringField()
            comments = ListField(EmbeddedDocumentField(Comment))

        BlogPost.drop_collection()

        c1 = Comment(by="joe", votes=3)
        c2 = Comment(by="jane", votes=7)

        BlogPost(title="ABC", comments=[c1, c2]).save()

        BlogPost.objects(comments__by="joe").update(inc__comments__S__votes=1)

        post = BlogPost.objects.first()
        self.assertEquals(post.comments[0].by, 'joe')
        self.assertEquals(post.comments[0].votes, 4)

        # Currently the $ operator only applies to the first matched item in
        # the query

        class Simple(Document):
            x = ListField()

        Simple.drop_collection()
        Simple(x=[1, 2, 3, 2]).save()
        Simple.objects(x=2).update(inc__x__S=1)

        simple = Simple.objects.first()
        self.assertEquals(simple.x, [1, 3, 3, 2])
        Simple.drop_collection()

        # You can set multiples
        Simple.drop_collection()
        Simple(x=[1, 2, 3, 4]).save()
        Simple(x=[2, 3, 4, 5]).save()
        Simple(x=[3, 4, 5, 6]).save()
        Simple(x=[4, 5, 6, 7]).save()
        Simple.objects(x=3).update(set__x__S=0)

        s = Simple.objects()
        self.assertEquals(s[0].x, [1, 2, 0, 4])
        self.assertEquals(s[1].x, [2, 0, 4, 5])
        self.assertEquals(s[2].x, [0, 4, 5, 6])
        self.assertEquals(s[3].x, [4, 5, 6, 7])

        # Using "$unset" with an expression like this "array.$" will result in
        # the array item becoming None, not being removed.
        Simple.drop_collection()
        Simple(x=[1, 2, 3, 4, 3, 2, 3, 4]).save()
        Simple.objects(x=3).update(unset__x__S=1)
        simple = Simple.objects.first()
        self.assertEquals(simple.x, [1, 2, None, 4, 3, 2, 3, 4])

        # Nested updates arent supported yet..
        def update_nested():
            Simple.drop_collection()
            Simple(x=[{'test': [1, 2, 3, 4]}]).save()
            Simple.objects(x__test=2).update(set__x__S__test__S=3)
            self.assertEquals(simple.x, [1, 2, 3, 4])

        self.assertRaises(OperationError, update_nested)
        Simple.drop_collection()

    def test_mapfield_update(self):
        """Ensure that the MapField can be updated."""
        class Member(EmbeddedDocument):
            gender = StringField()
            age = IntField()

        class Club(Document):
            members = MapField(EmbeddedDocumentField(Member))

        Club.drop_collection()

        club = Club()
        club.members['John'] = Member(gender="M", age=13)
        club.save()

        Club.objects().update(
            set__members={"John": Member(gender="F", age=14)})

        club = Club.objects().first()
        self.assertEqual(club.members['John'].gender, "F")
        self.assertEqual(club.members['John'].age, 14)

    def test_dictfield_update(self):
        """Ensure that the DictField can be updated."""
        class Club(Document):
            members = DictField()

        club = Club()
        club.members['John'] = dict(gender="M", age=13)
        club.save()

        Club.objects().update(
            set__members={"John": dict(gender="F", age=14)})

        club = Club.objects().first()
        self.assertEqual(club.members['John']['gender'], "F")
        self.assertEqual(club.members['John']['age'], 14)

    def test_get_or_create(self):
        """Ensure that ``get_or_create`` returns one result or creates a new
        document.
        """
        person1 = self.Person(name="User A", age=20)
        person1.save()
        person2 = self.Person(name="User B", age=30)
        person2.save()

        # Retrieve the first person from the database
        self.assertRaises(MultipleObjectsReturned,
                          self.Person.objects.get_or_create)
        self.assertRaises(self.Person.MultipleObjectsReturned,
                          self.Person.objects.get_or_create)

        # Use a query to filter the people found to just person2
        person, created = self.Person.objects.get_or_create(age=30)
        self.assertEqual(person.name, "User B")
        self.assertEqual(created, False)

        person, created = self.Person.objects.get_or_create(age__lt=30)
        self.assertEqual(person.name, "User A")
        self.assertEqual(created, False)

        # Try retrieving when no objects exists - new doc should be created
        kwargs = dict(age=50, defaults={'name': 'User C'})
        person, created = self.Person.objects.get_or_create(**kwargs)
        self.assertEqual(created, True)

        person = self.Person.objects.get(age=50)
        self.assertEqual(person.name, "User C")

    def test_bulk_insert(self):
        """Ensure that query by array position works.
        """

        class Comment(EmbeddedDocument):
            name = StringField()

        class Post(EmbeddedDocument):
            comments = ListField(EmbeddedDocumentField(Comment))

        class Blog(Document):
            title = StringField()
            tags = ListField(StringField())
            posts = ListField(EmbeddedDocumentField(Post))

        Blog.drop_collection()

        with query_counter() as q:
            self.assertEqual(q, 0)

            comment1 = Comment(name='testa')
            comment2 = Comment(name='testb')
            post1 = Post(comments=[comment1, comment2])
            post2 = Post(comments=[comment2, comment2])

            blogs = []
            for i in xrange(1, 100):
                blogs.append(Blog(title="post %s" % i, posts=[post1, post2]))

            Blog.objects.insert(blogs, load_bulk=False)
            self.assertEqual(q, 2) # 1 for the inital connection and 1 for the insert

            Blog.objects.insert(blogs)
            self.assertEqual(q, 4) # 1 for insert, and 1 for in bulk

        Blog.drop_collection()

        comment1 = Comment(name='testa')
        comment2 = Comment(name='testb')
        post1 = Post(comments=[comment1, comment2])
        post2 = Post(comments=[comment2, comment2])
        blog1 = Blog(title="code", posts=[post1, post2])
        blog2 = Blog(title="mongodb", posts=[post2, post1])
        blog1, blog2 = Blog.objects.insert([blog1, blog2])
        self.assertEqual(blog1.title, "code")
        self.assertEqual(blog2.title, "mongodb")

        self.assertEqual(Blog.objects.count(), 2)

        # test handles people trying to upsert
        def throw_operation_error():
            blogs = Blog.objects
            Blog.objects.insert(blogs)

        self.assertRaises(OperationError, throw_operation_error)

        # test handles other classes being inserted
        def throw_operation_error_wrong_doc():
            class Author(Document):
                pass
            Blog.objects.insert(Author())

        self.assertRaises(OperationError, throw_operation_error_wrong_doc)

        def throw_operation_error_not_a_document():
            Blog.objects.insert("HELLO WORLD")

        self.assertRaises(OperationError, throw_operation_error_not_a_document)

        Blog.drop_collection()

        blog1 = Blog(title="code", posts=[post1, post2])
        blog1 = Blog.objects.insert(blog1)
        self.assertEqual(blog1.title, "code")
        self.assertEqual(Blog.objects.count(), 1)

        Blog.drop_collection()
        blog1 = Blog(title="code", posts=[post1, post2])
        obj_id = Blog.objects.insert(blog1, load_bulk=False)
        self.assertEquals(obj_id.__class__.__name__, 'ObjectId')

    def test_slave_okay(self):
        """Ensures that a query can take slave_okay syntax
        """
        person1 = self.Person(name="User A", age=20)
        person1.save()
        person2 = self.Person(name="User B", age=30)
        person2.save()

        # Retrieve the first person from the database
        person = self.Person.objects.slave_okay(True).first()
        self.assertTrue(isinstance(person, self.Person))
        self.assertEqual(person.name, "User A")
        self.assertEqual(person.age, 20)

    def test_cursor_args(self):
        """Ensures the cursor args can be set as expected
        """
        p = self.Person.objects
        # Check default
        self.assertEqual(p._cursor_args,
                {'snapshot': False, 'slave_okay': False, 'timeout': True})

        p.snapshot(False).slave_okay(False).timeout(False)
        self.assertEqual(p._cursor_args,
                {'snapshot': False, 'slave_okay': False, 'timeout': False})

        p.snapshot(True).slave_okay(False).timeout(False)
        self.assertEqual(p._cursor_args,
                {'snapshot': True, 'slave_okay': False, 'timeout': False})

        p.snapshot(True).slave_okay(True).timeout(False)
        self.assertEqual(p._cursor_args,
                {'snapshot': True, 'slave_okay': True, 'timeout': False})

        p.snapshot(True).slave_okay(True).timeout(True)
        self.assertEqual(p._cursor_args,
                {'snapshot': True, 'slave_okay': True, 'timeout': True})

    def test_repeated_iteration(self):
        """Ensure that QuerySet rewinds itself one iteration finishes.
        """
        self.Person(name='Person 1').save()
        self.Person(name='Person 2').save()

        queryset = self.Person.objects
        people1 = [person for person in queryset]
        people2 = [person for person in queryset]

        self.assertEqual(people1, people2)

    def test_repr_iteration(self):
        """Ensure that QuerySet __repr__ can handle loops
        """
        self.Person(name='Person 1').save()
        self.Person(name='Person 2').save()

        queryset = self.Person.objects
        self.assertEquals('[<Person: Person object>, <Person: Person object>]', repr(queryset))
        for person in queryset:
            self.assertEquals('.. queryset mid-iteration ..', repr(queryset))


    def test_regex_query_shortcuts(self):
        """Ensure that contains, startswith, endswith, etc work.
        """
        person = self.Person(name='Guido van Rossum')
        person.save()

        # Test contains
        obj = self.Person.objects(name__contains='van').first()
        self.assertEqual(obj, person)
        obj = self.Person.objects(name__contains='Van').first()
        self.assertEqual(obj, None)
        obj = self.Person.objects(Q(name__contains='van')).first()
        self.assertEqual(obj, person)
        obj = self.Person.objects(Q(name__contains='Van')).first()
        self.assertEqual(obj, None)

        # Test icontains
        obj = self.Person.objects(name__icontains='Van').first()
        self.assertEqual(obj, person)
        obj = self.Person.objects(Q(name__icontains='Van')).first()
        self.assertEqual(obj, person)

        # Test startswith
        obj = self.Person.objects(name__startswith='Guido').first()
        self.assertEqual(obj, person)
        obj = self.Person.objects(name__startswith='guido').first()
        self.assertEqual(obj, None)
        obj = self.Person.objects(Q(name__startswith='Guido')).first()
        self.assertEqual(obj, person)
        obj = self.Person.objects(Q(name__startswith='guido')).first()
        self.assertEqual(obj, None)

        # Test istartswith
        obj = self.Person.objects(name__istartswith='guido').first()
        self.assertEqual(obj, person)
        obj = self.Person.objects(Q(name__istartswith='guido')).first()
        self.assertEqual(obj, person)

        # Test endswith
        obj = self.Person.objects(name__endswith='Rossum').first()
        self.assertEqual(obj, person)
        obj = self.Person.objects(name__endswith='rossuM').first()
        self.assertEqual(obj, None)
        obj = self.Person.objects(Q(name__endswith='Rossum')).first()
        self.assertEqual(obj, person)
        obj = self.Person.objects(Q(name__endswith='rossuM')).first()
        self.assertEqual(obj, None)

        # Test iendswith
        obj = self.Person.objects(name__iendswith='rossuM').first()
        self.assertEqual(obj, person)
        obj = self.Person.objects(Q(name__iendswith='rossuM')).first()
        self.assertEqual(obj, person)

        # Test exact
        obj = self.Person.objects(name__exact='Guido van Rossum').first()
        self.assertEqual(obj, person)
        obj = self.Person.objects(name__exact='Guido van rossum').first()
        self.assertEqual(obj, None)
        obj = self.Person.objects(name__exact='Guido van Rossu').first()
        self.assertEqual(obj, None)
        obj = self.Person.objects(Q(name__exact='Guido van Rossum')).first()
        self.assertEqual(obj, person)
        obj = self.Person.objects(Q(name__exact='Guido van rossum')).first()
        self.assertEqual(obj, None)
        obj = self.Person.objects(Q(name__exact='Guido van Rossu')).first()
        self.assertEqual(obj, None)

        # Test iexact
        obj = self.Person.objects(name__iexact='gUIDO VAN rOSSUM').first()
        self.assertEqual(obj, person)
        obj = self.Person.objects(name__iexact='gUIDO VAN rOSSU').first()
        self.assertEqual(obj, None)
        obj = self.Person.objects(Q(name__iexact='gUIDO VAN rOSSUM')).first()
        self.assertEqual(obj, person)
        obj = self.Person.objects(Q(name__iexact='gUIDO VAN rOSSU')).first()
        self.assertEqual(obj, None)

        # Test unsafe expressions
        person = self.Person(name='Guido van Rossum [.\'Geek\']')
        person.save()

        obj = self.Person.objects(Q(name__icontains='[.\'Geek')).first()
        self.assertEqual(obj, person)

    def test_not(self):
        """Ensure that the __not operator works as expected.
        """
        alice = self.Person(name='Alice', age=25)
        alice.save()

        obj = self.Person.objects(name__iexact='alice').first()
        self.assertEqual(obj, alice)

        obj = self.Person.objects(name__not__iexact='alice').first()
        self.assertEqual(obj, None)

    def test_filter_chaining(self):
        """Ensure filters can be chained together.
        """
        class BlogPost(Document):
            title = StringField()
            is_published = BooleanField()
            published_date = DateTimeField()

            @queryset_manager
            def published(doc_cls, queryset):
                return queryset(is_published=True)

        blog_post_1 = BlogPost(title="Blog Post #1",
                               is_published = True,
                               published_date=datetime(2010, 1, 5, 0, 0 ,0))
        blog_post_2 = BlogPost(title="Blog Post #2",
                               is_published = True,
                               published_date=datetime(2010, 1, 6, 0, 0 ,0))
        blog_post_3 = BlogPost(title="Blog Post #3",
                               is_published = True,
                               published_date=datetime(2010, 1, 7, 0, 0 ,0))

        blog_post_1.save()
        blog_post_2.save()
        blog_post_3.save()

        # find all published blog posts before 2010-01-07
        published_posts = BlogPost.published()
        published_posts = published_posts.filter(
            published_date__lt=datetime(2010, 1, 7, 0, 0 ,0))
        self.assertEqual(published_posts.count(), 2)

        BlogPost.drop_collection()

    def test_ordering(self):
        """Ensure default ordering is applied and can be overridden.
        """
        class BlogPost(Document):
            title = StringField()
            published_date = DateTimeField()

            meta = {
                'ordering': ['-published_date']
            }

        BlogPost.drop_collection()

        blog_post_1 = BlogPost(title="Blog Post #1",
                               published_date=datetime(2010, 1, 5, 0, 0 ,0))
        blog_post_2 = BlogPost(title="Blog Post #2",
                               published_date=datetime(2010, 1, 6, 0, 0 ,0))
        blog_post_3 = BlogPost(title="Blog Post #3",
                               published_date=datetime(2010, 1, 7, 0, 0 ,0))

        blog_post_1.save()
        blog_post_2.save()
        blog_post_3.save()

        # get the "first" BlogPost using default ordering
        # from BlogPost.meta.ordering
        latest_post = BlogPost.objects.first()
        self.assertEqual(latest_post.title, "Blog Post #3")

        # override default ordering, order BlogPosts by "published_date"
        first_post = BlogPost.objects.order_by("+published_date").first()
        self.assertEqual(first_post.title, "Blog Post #1")

        BlogPost.drop_collection()

    def test_only(self):
        """Ensure that QuerySet.only only returns the requested fields.
        """
        person = self.Person(name='test', age=25)
        person.save()

        obj = self.Person.objects.only('name').get()
        self.assertEqual(obj.name, person.name)
        self.assertEqual(obj.age, None)

        obj = self.Person.objects.only('age').get()
        self.assertEqual(obj.name, None)
        self.assertEqual(obj.age, person.age)

        obj = self.Person.objects.only('name', 'age').get()
        self.assertEqual(obj.name, person.name)
        self.assertEqual(obj.age, person.age)

        # Check polymorphism still works
        class Employee(self.Person):
            salary = IntField(db_field='wage')

        employee = Employee(name='test employee', age=40, salary=30000)
        employee.save()

        obj = self.Person.objects(id=employee.id).only('age').get()
        self.assertTrue(isinstance(obj, Employee))

        # Check field names are looked up properly
        obj = Employee.objects(id=employee.id).only('salary').get()
        self.assertEqual(obj.salary, employee.salary)
        self.assertEqual(obj.name, None)

    def test_only_with_subfields(self):
        class User(EmbeddedDocument):
            name = StringField()
            email = StringField()

        class Comment(EmbeddedDocument):
            title = StringField()
            text = StringField()

        class BlogPost(Document):
            content = StringField()
            author = EmbeddedDocumentField(User)
            comments = ListField(EmbeddedDocumentField(Comment))

        BlogPost.drop_collection()

        post = BlogPost(content='Had a good coffee today...')
        post.author = User(name='Test User')
        post.comments = [Comment(title='I aggree', text='Great post!'), Comment(title='Coffee', text='I hate coffee')]
        post.save()

        obj = BlogPost.objects.only('author.name',).get()
        self.assertEqual(obj.content, None)
        self.assertEqual(obj.author.email, None)
        self.assertEqual(obj.author.name, 'Test User')
        self.assertEqual(obj.comments, [])

        obj = BlogPost.objects.only('content', 'comments.title',).get()
        self.assertEqual(obj.content, 'Had a good coffee today...')
        self.assertEqual(obj.author, None)
        self.assertEqual(obj.comments[0].title, 'I aggree')
        self.assertEqual(obj.comments[1].title, 'Coffee')
        self.assertEqual(obj.comments[0].text, None)
        self.assertEqual(obj.comments[1].text, None)

        obj = BlogPost.objects.only('comments',).get()
        self.assertEqual(obj.content, None)
        self.assertEqual(obj.author, None)
        self.assertEqual(obj.comments[0].title, 'I aggree')
        self.assertEqual(obj.comments[1].title, 'Coffee')
        self.assertEqual(obj.comments[0].text, 'Great post!')
        self.assertEqual(obj.comments[1].text, 'I hate coffee')

        BlogPost.drop_collection()

    def test_exclude(self):
        class User(EmbeddedDocument):
            name = StringField()
            email = StringField()

        class Comment(EmbeddedDocument):
            title = StringField()
            text = StringField()

        class BlogPost(Document):
            content = StringField()
            author = EmbeddedDocumentField(User)
            comments = ListField(EmbeddedDocumentField(Comment))

        BlogPost.drop_collection()

        post = BlogPost(content='Had a good coffee today...')
        post.author = User(name='Test User')
        post.comments = [Comment(title='I aggree', text='Great post!'), Comment(title='Coffee', text='I hate coffee')]
        post.save()

        obj = BlogPost.objects.exclude('author', 'comments.text').get()
        self.assertEqual(obj.author, None)
        self.assertEqual(obj.content, 'Had a good coffee today...')
        self.assertEqual(obj.comments[0].title, 'I aggree')
        self.assertEqual(obj.comments[0].text, None)

        BlogPost.drop_collection()

    def test_exclude_only_combining(self):
        class Attachment(EmbeddedDocument):
            name = StringField()
            content = StringField()

        class Email(Document):
            sender = StringField()
            to = StringField()
            subject = StringField()
            body = StringField()
            content_type = StringField()
            attachments = ListField(EmbeddedDocumentField(Attachment))

        Email.drop_collection()
        email = Email(sender='me', to='you', subject='From Russia with Love', body='Hello!', content_type='text/plain')
        email.attachments = [
            Attachment(name='file1.doc', content='ABC'),
            Attachment(name='file2.doc', content='XYZ'),
        ]
        email.save()

        obj = Email.objects.exclude('content_type').exclude('body').get()
        self.assertEqual(obj.sender, 'me')
        self.assertEqual(obj.to, 'you')
        self.assertEqual(obj.subject, 'From Russia with Love')
        self.assertEqual(obj.body, None)
        self.assertEqual(obj.content_type, None)

        obj = Email.objects.only('sender', 'to').exclude('body', 'sender').get()
        self.assertEqual(obj.sender, None)
        self.assertEqual(obj.to, 'you')
        self.assertEqual(obj.subject, None)
        self.assertEqual(obj.body, None)
        self.assertEqual(obj.content_type, None)

        obj = Email.objects.exclude('attachments.content').exclude('body').only('to', 'attachments.name').get()
        self.assertEqual(obj.attachments[0].name, 'file1.doc')
        self.assertEqual(obj.attachments[0].content, None)
        self.assertEqual(obj.sender, None)
        self.assertEqual(obj.to, 'you')
        self.assertEqual(obj.subject, None)
        self.assertEqual(obj.body, None)
        self.assertEqual(obj.content_type, None)

        Email.drop_collection()

    def test_all_fields(self):

        class Email(Document):
            sender = StringField()
            to = StringField()
            subject = StringField()
            body = StringField()
            content_type = StringField()

        Email.drop_collection()

        email = Email(sender='me', to='you', subject='From Russia with Love', body='Hello!', content_type='text/plain')
        email.save()

        obj = Email.objects.exclude('content_type', 'body').only('to', 'body').all_fields().get()
        self.assertEqual(obj.sender, 'me')
        self.assertEqual(obj.to, 'you')
        self.assertEqual(obj.subject, 'From Russia with Love')
        self.assertEqual(obj.body, 'Hello!')
        self.assertEqual(obj.content_type, 'text/plain')

        Email.drop_collection()

    def test_slicing_fields(self):
        """Ensure that query slicing an array works.
        """
        class Numbers(Document):
            n = ListField(IntField())

        Numbers.drop_collection()

        numbers = Numbers(n=[0,1,2,3,4,5,-5,-4,-3,-2,-1])
        numbers.save()

        # first three
        numbers = Numbers.objects.fields(slice__n=3).get()
        self.assertEquals(numbers.n, [0, 1, 2])

        # last three
        numbers = Numbers.objects.fields(slice__n=-3).get()
        self.assertEquals(numbers.n, [-3, -2, -1])

        # skip 2, limit 3
        numbers = Numbers.objects.fields(slice__n=[2, 3]).get()
        self.assertEquals(numbers.n, [2, 3, 4])

        # skip to fifth from last, limit 4
        numbers = Numbers.objects.fields(slice__n=[-5, 4]).get()
        self.assertEquals(numbers.n, [-5, -4, -3, -2])

        # skip to fifth from last, limit 10
        numbers = Numbers.objects.fields(slice__n=[-5, 10]).get()
        self.assertEquals(numbers.n, [-5, -4, -3, -2, -1])

        # skip to fifth from last, limit 10 dict method
        numbers = Numbers.objects.fields(n={"$slice": [-5, 10]}).get()
        self.assertEquals(numbers.n, [-5, -4, -3, -2, -1])

    def test_slicing_nested_fields(self):
        """Ensure that query slicing an embedded array works.
        """

        class EmbeddedNumber(EmbeddedDocument):
            n = ListField(IntField())

        class Numbers(Document):
            embedded = EmbeddedDocumentField(EmbeddedNumber)

        Numbers.drop_collection()

        numbers = Numbers()
        numbers.embedded = EmbeddedNumber(n=[0,1,2,3,4,5,-5,-4,-3,-2,-1])
        numbers.save()

        # first three
        numbers = Numbers.objects.fields(slice__embedded__n=3).get()
        self.assertEquals(numbers.embedded.n, [0, 1, 2])

        # last three
        numbers = Numbers.objects.fields(slice__embedded__n=-3).get()
        self.assertEquals(numbers.embedded.n, [-3, -2, -1])

        # skip 2, limit 3
        numbers = Numbers.objects.fields(slice__embedded__n=[2, 3]).get()
        self.assertEquals(numbers.embedded.n, [2, 3, 4])

        # skip to fifth from last, limit 4
        numbers = Numbers.objects.fields(slice__embedded__n=[-5, 4]).get()
        self.assertEquals(numbers.embedded.n, [-5, -4, -3, -2])

        # skip to fifth from last, limit 10
        numbers = Numbers.objects.fields(slice__embedded__n=[-5, 10]).get()
        self.assertEquals(numbers.embedded.n, [-5, -4, -3, -2, -1])

        # skip to fifth from last, limit 10 dict method
        numbers = Numbers.objects.fields(embedded__n={"$slice": [-5, 10]}).get()
        self.assertEquals(numbers.embedded.n, [-5, -4, -3, -2, -1])

    def test_find_embedded(self):
        """Ensure that an embedded document is properly returned from a query.
        """
        class User(EmbeddedDocument):
            name = StringField()

        class BlogPost(Document):
            content = StringField()
            author = EmbeddedDocumentField(User)

        BlogPost.drop_collection()

        post = BlogPost(content='Had a good coffee today...')
        post.author = User(name='Test User')
        post.save()

        result = BlogPost.objects.first()
        self.assertTrue(isinstance(result.author, User))
        self.assertEqual(result.author.name, 'Test User')

        BlogPost.drop_collection()

    def test_find_dict_item(self):
        """Ensure that DictField items may be found.
        """
        class BlogPost(Document):
            info = DictField()

        BlogPost.drop_collection()

        post = BlogPost(info={'title': 'test'})
        post.save()

        post_obj = BlogPost.objects(info__title='test').first()
        self.assertEqual(post_obj.id, post.id)

        BlogPost.drop_collection()

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

    def test_exec_js_query(self):
        """Ensure that queries are properly formed for use in exec_js.
        """
        class BlogPost(Document):
            hits = IntField()
            published = BooleanField()

        BlogPost.drop_collection()

        post1 = BlogPost(hits=1, published=False)
        post1.save()

        post2 = BlogPost(hits=1, published=True)
        post2.save()

        post3 = BlogPost(hits=1, published=True)
        post3.save()

        js_func = """
            function(hitsField) {
                var count = 0;
                db[collection].find(query).forEach(function(doc) {
                    count += doc[hitsField];
                });
                return count;
            }
        """

        # Ensure that normal queries work
        c = BlogPost.objects(published=True).exec_js(js_func, 'hits')
        self.assertEqual(c, 2)

        c = BlogPost.objects(published=False).exec_js(js_func, 'hits')
        self.assertEqual(c, 1)

        # Ensure that Q object queries work
        c = BlogPost.objects(Q(published=True)).exec_js(js_func, 'hits')
        self.assertEqual(c, 2)

        c = BlogPost.objects(Q(published=False)).exec_js(js_func, 'hits')
        self.assertEqual(c, 1)

        BlogPost.drop_collection()

    def test_exec_js_field_sub(self):
        """Ensure that field substitutions occur properly in exec_js functions.
        """
        class Comment(EmbeddedDocument):
            content = StringField(db_field='body')

        class BlogPost(Document):
            name = StringField(db_field='doc-name')
            comments = ListField(EmbeddedDocumentField(Comment),
                                 db_field='cmnts')

        BlogPost.drop_collection()

        comments1 = [Comment(content='cool'), Comment(content='yay')]
        post1 = BlogPost(name='post1', comments=comments1)
        post1.save()

        comments2 = [Comment(content='nice stuff')]
        post2 = BlogPost(name='post2', comments=comments2)
        post2.save()

        code = """
        function getComments() {
            var comments = [];
            db[collection].find(query).forEach(function(doc) {
                var docComments = doc[~comments];
                for (var i = 0; i < docComments.length; i++) {
                    comments.push({
                        'document': doc[~name],
                        'comment': doc[~comments][i][~comments.content]
                    });
                }
            });
            return comments;
        }
        """

        sub_code = BlogPost.objects._sub_js_fields(code)
        code_chunks = ['doc["cmnts"];', 'doc["doc-name"],',
                       'doc["cmnts"][i]["body"]']
        for chunk in code_chunks:
            self.assertTrue(chunk in sub_code)

        results = BlogPost.objects.exec_js(code)
        expected_results = [
            {u'comment': u'cool', u'document': u'post1'},
            {u'comment': u'yay', u'document': u'post1'},
            {u'comment': u'nice stuff', u'document': u'post2'},
        ]
        self.assertEqual(results, expected_results)

        # Test template style
        code = "{{~comments.content}}"
        sub_code = BlogPost.objects._sub_js_fields(code)
        self.assertEquals("cmnts.body", sub_code)

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

    def test_reverse_delete_rule_cascade(self):
        """Ensure cascading deletion of referring documents from the database.
        """
        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(self.Person, reverse_delete_rule=CASCADE)
        BlogPost.drop_collection()

        me = self.Person(name='Test User')
        me.save()
        someoneelse = self.Person(name='Some-one Else')
        someoneelse.save()

        BlogPost(content='Watching TV', author=me).save()
        BlogPost(content='Chilling out', author=me).save()
        BlogPost(content='Pro Testing', author=someoneelse).save()

        self.assertEqual(3, BlogPost.objects.count())
        self.Person.objects(name='Test User').delete()
        self.assertEqual(1, BlogPost.objects.count())

    def test_reverse_delete_rule_nullify(self):
        """Ensure nullification of references to deleted documents.
        """
        class Category(Document):
            name = StringField()

        class BlogPost(Document):
            content = StringField()
            category = ReferenceField(Category, reverse_delete_rule=NULLIFY)

        BlogPost.drop_collection()
        Category.drop_collection()

        lameness = Category(name='Lameness')
        lameness.save()

        post = BlogPost(content='Watching TV', category=lameness)
        post.save()

        self.assertEqual(1, BlogPost.objects.count())
        self.assertEqual('Lameness', BlogPost.objects.first().category.name)
        Category.objects.delete()
        self.assertEqual(1, BlogPost.objects.count())
        self.assertEqual(None, BlogPost.objects.first().category)

    def test_reverse_delete_rule_deny(self):
        """Ensure deletion gets denied on documents that still have references
        to them.
        """
        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(self.Person, reverse_delete_rule=DENY)

        BlogPost.drop_collection()
        self.Person.drop_collection()

        me = self.Person(name='Test User')
        me.save()

        post = BlogPost(content='Watching TV', author=me)
        post.save()

        self.assertRaises(OperationError, self.Person.objects.delete)

    def test_update(self):
        """Ensure that atomic updates work properly.
        """
        class BlogPost(Document):
            title = StringField()
            hits = IntField()
            tags = ListField(StringField())

        BlogPost.drop_collection()

        post = BlogPost(name="Test Post", hits=5, tags=['test'])
        post.save()

        BlogPost.objects.update(set__hits=10)
        post.reload()
        self.assertEqual(post.hits, 10)

        BlogPost.objects.update_one(inc__hits=1)
        post.reload()
        self.assertEqual(post.hits, 11)

        BlogPost.objects.update_one(dec__hits=1)
        post.reload()
        self.assertEqual(post.hits, 10)

        BlogPost.objects.update(push__tags='mongo')
        post.reload()
        self.assertTrue('mongo' in post.tags)

        BlogPost.objects.update_one(push_all__tags=['db', 'nosql'])
        post.reload()
        self.assertTrue('db' in post.tags and 'nosql' in post.tags)

        tags = post.tags[:-1]
        BlogPost.objects.update(pop__tags=1)
        post.reload()
        self.assertEqual(post.tags, tags)

        BlogPost.objects.update_one(add_to_set__tags='unique')
        BlogPost.objects.update_one(add_to_set__tags='unique')
        post.reload()
        self.assertEqual(post.tags.count('unique'), 1)

        self.assertNotEqual(post.hits, None)
        BlogPost.objects.update_one(unset__hits=1)
        post.reload()
        self.assertEqual(post.hits, None)

        BlogPost.drop_collection()

    def test_update_pull(self):
        """Ensure that the 'pull' update operation works correctly.
        """
        class BlogPost(Document):
            slug = StringField()
            tags = ListField(StringField())

        post = BlogPost(slug="test", tags=['code', 'mongodb', 'code'])
        post.save()

        BlogPost.objects(slug="test").update(pull__tags="code")
        post.reload()
        self.assertTrue('code' not in post.tags)
        self.assertEqual(len(post.tags), 1)

    def test_update_one_pop_generic_reference(self):

        class BlogTag(Document):
            name = StringField(required=True)

        class BlogPost(Document):
            slug = StringField()
            tags = ListField(ReferenceField(BlogTag), required=True)

        BlogPost.drop_collection()
        BlogTag.drop_collection()

        tag_1 = BlogTag(name='code')
        tag_1.save()
        tag_2 = BlogTag(name='mongodb')
        tag_2.save()

        post = BlogPost(slug="test", tags=[tag_1])
        post.save()

        post = BlogPost(slug="test-2", tags=[tag_1, tag_2])
        post.save()
        self.assertEqual(len(post.tags), 2)

        BlogPost.objects(slug="test-2").update_one(pop__tags=-1)

        post.reload()
        self.assertEqual(len(post.tags), 1)

        BlogPost.drop_collection()
        BlogTag.drop_collection()

    def test_editting_embedded_objects(self):

        class BlogTag(EmbeddedDocument):
            name = StringField(required=True)

        class BlogPost(Document):
            slug = StringField()
            tags = ListField(EmbeddedDocumentField(BlogTag), required=True)

        BlogPost.drop_collection()

        tag_1 = BlogTag(name='code')
        tag_2 = BlogTag(name='mongodb')

        post = BlogPost(slug="test", tags=[tag_1])
        post.save()

        post = BlogPost(slug="test-2", tags=[tag_1, tag_2])
        post.save()
        self.assertEqual(len(post.tags), 2)

        BlogPost.objects(slug="test-2").update_one(set__tags__0__name="python")
        post.reload()
        self.assertEquals(post.tags[0].name, 'python')

        BlogPost.objects(slug="test-2").update_one(pop__tags=-1)
        post.reload()
        self.assertEqual(len(post.tags), 1)

        BlogPost.drop_collection()

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

    def test_confirm_order_by_reference_wont_work(self):
        """Ordering by reference is not possible.  Use map / reduce.. or
        denormalise"""

        class Author(Document):
            author = ReferenceField(self.Person)

        Author.drop_collection()

        person_a = self.Person(name="User A", age=20)
        person_a.save()
        person_b = self.Person(name="User B", age=40)
        person_b.save()
        person_c = self.Person(name="User C", age=30)
        person_c.save()

        Author(author=person_a).save()
        Author(author=person_b).save()
        Author(author=person_c).save()

        names = [a.author.name for a in Author.objects.order_by('-author__age')]
        self.assertEqual(names, ['User A', 'User B', 'User C'])

    def test_map_reduce(self):
        """Ensure map/reduce is both mapping and reducing.
        """
        class BlogPost(Document):
            title = StringField()
            tags = ListField(StringField(), db_field='post-tag-list')

        BlogPost.drop_collection()

        BlogPost(title="Post #1", tags=['music', 'film', 'print']).save()
        BlogPost(title="Post #2", tags=['music', 'film']).save()
        BlogPost(title="Post #3", tags=['film', 'photography']).save()

        map_f = """
            function() {
                this[~tags].forEach(function(tag) {
                    emit(tag, 1);
                });
            }
        """

        reduce_f = """
            function(key, values) {
                var total = 0;
                for(var i=0; i<values.length; i++) {
                    total += values[i];
                }
                return total;
            }
        """

        # run a map/reduce operation spanning all posts
        results = BlogPost.objects.map_reduce(map_f, reduce_f, "myresults")
        results = list(results)
        self.assertEqual(len(results), 4)

        music = filter(lambda r: r.key == "music", results)[0]
        self.assertEqual(music.value, 2)

        film = filter(lambda r: r.key == "film", results)[0]
        self.assertEqual(film.value, 3)

        BlogPost.drop_collection()

    def test_map_reduce_with_custom_object_ids(self):
        """Ensure that QuerySet.map_reduce works properly with custom
        primary keys.
        """

        class BlogPost(Document):
            title = StringField(primary_key=True)
            tags = ListField(StringField())

        post1 = BlogPost(title="Post #1", tags=["mongodb", "mongoengine"])
        post2 = BlogPost(title="Post #2", tags=["django", "mongodb"])
        post3 = BlogPost(title="Post #3", tags=["hitchcock films"])

        post1.save()
        post2.save()
        post3.save()

        self.assertEqual(BlogPost._fields['title'].db_field, '_id')
        self.assertEqual(BlogPost._meta['id_field'], 'title')

        map_f = """
            function() {
                emit(this._id, 1);
            }
        """

        # reduce to a list of tag ids and counts
        reduce_f = """
            function(key, values) {
                var total = 0;
                for(var i=0; i<values.length; i++) {
                    total += values[i];
                }
                return total;
            }
        """

        results = BlogPost.objects.map_reduce(map_f, reduce_f, "myresults")
        results = list(results)

        self.assertEqual(results[0].object, post1)
        self.assertEqual(results[1].object, post2)
        self.assertEqual(results[2].object, post3)

        BlogPost.drop_collection()

    def test_map_reduce_finalize(self):
        """Ensure that map, reduce, and finalize run and introduce "scope"
        by simulating "hotness" ranking with Reddit algorithm.
        """
        from time import mktime

        class Link(Document):
            title = StringField(db_field='bpTitle')
            up_votes = IntField()
            down_votes = IntField()
            submitted = DateTimeField(db_field='sTime')

        Link.drop_collection()

        now = datetime.utcnow()

        # Note: Test data taken from a custom Reddit homepage on
        # Fri, 12 Feb 2010 14:36:00 -0600. Link ordering should
        # reflect order of insertion below, but is not influenced
        # by insertion order.
        Link(title = "Google Buzz auto-followed a woman's abusive ex ...",
             up_votes = 1079,
             down_votes = 553,
             submitted = now-timedelta(hours=4)).save()
        Link(title = "We did it! Barbie is a computer engineer.",
             up_votes = 481,
             down_votes = 124,
             submitted = now-timedelta(hours=2)).save()
        Link(title = "This Is A Mosquito Getting Killed By A Laser",
             up_votes = 1446,
             down_votes = 530,
             submitted=now-timedelta(hours=13)).save()
        Link(title = "Arabic flashcards land physics student in jail.",
             up_votes = 215,
             down_votes = 105,
             submitted = now-timedelta(hours=6)).save()
        Link(title = "The Burger Lab: Presenting, the Flood Burger",
             up_votes = 48,
             down_votes = 17,
             submitted = now-timedelta(hours=5)).save()
        Link(title="How to see polarization with the naked eye",
             up_votes = 74,
             down_votes = 13,
             submitted = now-timedelta(hours=10)).save()

        map_f = """
            function() {
                emit(this[~id], {up_delta: this[~up_votes] - this[~down_votes],
                                sub_date: this[~submitted].getTime() / 1000})
            }
        """

        reduce_f = """
            function(key, values) {
                data = values[0];

                x = data.up_delta;

                // calculate time diff between reddit epoch and submission
                sec_since_epoch = data.sub_date - reddit_epoch;

                // calculate 'Y'
                if(x > 0) {
                    y = 1;
                } else if (x = 0) {
                    y = 0;
                } else {
                    y = -1;
                }

                // calculate 'Z', the maximal value
                if(Math.abs(x) >= 1) {
                    z = Math.abs(x);
                } else {
                    z = 1;
                }

                return {x: x, y: y, z: z, t_s: sec_since_epoch};
            }
        """

        finalize_f = """
            function(key, value) {
                // f(sec_since_epoch,y,z) =
                //                    log10(z) + ((y*sec_since_epoch) / 45000)
                z_10 = Math.log(value.z) / Math.log(10);
                weight = z_10 + ((value.y * value.t_s) / 45000);
                return weight;
            }
        """

        # provide the reddit epoch (used for ranking) as a variable available
        # to all phases of the map/reduce operation: map, reduce, and finalize.
        reddit_epoch = mktime(datetime(2005, 12, 8, 7, 46, 43).timetuple())
        scope = {'reddit_epoch': reddit_epoch}

        # run a map/reduce operation across all links. ordering is set
        # to "-value", which orders the "weight" value returned from
        # "finalize_f" in descending order.
        results = Link.objects.order_by("-value")
        results = results.map_reduce(map_f,
                                     reduce_f,
                                     "myresults",
                                     finalize_f=finalize_f,
                                     scope=scope)
        results = list(results)

        # assert troublesome Buzz article is ranked 1st
        self.assertTrue(results[0].object.title.startswith("Google Buzz"))

        # assert laser vision is ranked last
        self.assertTrue(results[-1].object.title.startswith("How to see"))

        Link.drop_collection()

    def test_item_frequencies(self):
        """Ensure that item frequencies are properly generated from lists.
        """
        class BlogPost(Document):
            hits = IntField()
            tags = ListField(StringField(), db_field='blogTags')

        BlogPost.drop_collection()

        BlogPost(hits=1, tags=['music', 'film', 'actors', 'watch']).save()
        BlogPost(hits=2, tags=['music', 'watch']).save()
        BlogPost(hits=2, tags=['music', 'actors']).save()

        def test_assertions(f):
            f = dict((key, int(val)) for key, val in f.items())
            self.assertEqual(set(['music', 'film', 'actors', 'watch']), set(f.keys()))
            self.assertEqual(f['music'], 3)
            self.assertEqual(f['actors'], 2)
            self.assertEqual(f['watch'], 2)
            self.assertEqual(f['film'], 1)

        exec_js = BlogPost.objects.item_frequencies('tags')
        map_reduce = BlogPost.objects.item_frequencies('tags', map_reduce=True)
        test_assertions(exec_js)
        test_assertions(map_reduce)

        # Ensure query is taken into account
        def test_assertions(f):
            f = dict((key, int(val)) for key, val in f.items())
            self.assertEqual(set(['music', 'actors', 'watch']), set(f.keys()))
            self.assertEqual(f['music'], 2)
            self.assertEqual(f['actors'], 1)
            self.assertEqual(f['watch'], 1)

        exec_js = BlogPost.objects(hits__gt=1).item_frequencies('tags')
        map_reduce = BlogPost.objects(hits__gt=1).item_frequencies('tags', map_reduce=True)
        test_assertions(exec_js)
        test_assertions(map_reduce)

        # Check that normalization works
        def test_assertions(f):
            self.assertAlmostEqual(f['music'], 3.0/8.0)
            self.assertAlmostEqual(f['actors'], 2.0/8.0)
            self.assertAlmostEqual(f['watch'], 2.0/8.0)
            self.assertAlmostEqual(f['film'], 1.0/8.0)

        exec_js = BlogPost.objects.item_frequencies('tags', normalize=True)
        map_reduce = BlogPost.objects.item_frequencies('tags', normalize=True, map_reduce=True)
        test_assertions(exec_js)
        test_assertions(map_reduce)

        # Check item_frequencies works for non-list fields
        def test_assertions(f):
            self.assertEqual(set(['1', '2']), set(f.keys()))
            self.assertEqual(f['1'], 1)
            self.assertEqual(f['2'], 2)

        exec_js = BlogPost.objects.item_frequencies('hits')
        map_reduce = BlogPost.objects.item_frequencies('hits', map_reduce=True)
        test_assertions(exec_js)
        test_assertions(map_reduce)

        BlogPost.drop_collection()

    def test_item_frequencies_on_embedded(self):
        """Ensure that item frequencies are properly generated from lists.
        """

        class Phone(EmbeddedDocument):
            number = StringField()

        class Person(Document):
            name = StringField()
            phone = EmbeddedDocumentField(Phone)

        Person.drop_collection()

        doc = Person(name="Guido")
        doc.phone = Phone(number='62-3331-1656')
        doc.save()

        doc = Person(name="Marr")
        doc.phone = Phone(number='62-3331-1656')
        doc.save()

        doc = Person(name="WP Junior")
        doc.phone = Phone(number='62-3332-1656')
        doc.save()


        def test_assertions(f):
            f = dict((key, int(val)) for key, val in f.items())
            self.assertEqual(set(['62-3331-1656', '62-3332-1656']), set(f.keys()))
            self.assertEqual(f['62-3331-1656'], 2)
            self.assertEqual(f['62-3332-1656'], 1)

        exec_js = Person.objects.item_frequencies('phone.number')
        map_reduce = Person.objects.item_frequencies('phone.number', map_reduce=True)
        test_assertions(exec_js)
        test_assertions(map_reduce)

        # Ensure query is taken into account
        def test_assertions(f):
            f = dict((key, int(val)) for key, val in f.items())
            self.assertEqual(set(['62-3331-1656']), set(f.keys()))
            self.assertEqual(f['62-3331-1656'], 2)

        exec_js = Person.objects(phone__number='62-3331-1656').item_frequencies('phone.number')
        map_reduce = Person.objects(phone__number='62-3331-1656').item_frequencies('phone.number', map_reduce=True)
        test_assertions(exec_js)
        test_assertions(map_reduce)

        # Check that normalization works
        def test_assertions(f):
            self.assertEqual(f['62-3331-1656'], 2.0/3.0)
            self.assertEqual(f['62-3332-1656'], 1.0/3.0)

        exec_js = Person.objects.item_frequencies('phone.number', normalize=True)
        map_reduce = Person.objects.item_frequencies('phone.number', normalize=True, map_reduce=True)
        test_assertions(exec_js)
        test_assertions(map_reduce)

    def test_item_frequencies_null_values(self):

        class Person(Document):
            name = StringField()
            city = StringField()

        Person.drop_collection()

        Person(name="Wilson Snr", city="CRB").save()
        Person(name="Wilson Jr").save()

        freq = Person.objects.item_frequencies('city')
        self.assertEquals(freq, {'CRB': 1.0, None: 1.0})
        freq = Person.objects.item_frequencies('city', normalize=True)
        self.assertEquals(freq, {'CRB': 0.5, None: 0.5})


        freq = Person.objects.item_frequencies('city', map_reduce=True)
        self.assertEquals(freq, {'CRB': 1.0, None: 1.0})
        freq = Person.objects.item_frequencies('city', normalize=True, map_reduce=True)
        self.assertEquals(freq, {'CRB': 0.5, None: 0.5})

    def test_average(self):
        """Ensure that field can be averaged correctly.
        """
        self.Person(name='person', age=0).save()
        self.assertEqual(int(self.Person.objects.average('age')), 0)

        ages = [23, 54, 12, 94, 27]
        for i, age in enumerate(ages):
            self.Person(name='test%s' % i, age=age).save()

        avg = float(sum(ages)) / (len(ages) + 1) # take into account the 0
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

    def test_distinct(self):
        """Ensure that the QuerySet.distinct method works.
        """
        self.Person(name='Mr Orange', age=20).save()
        self.Person(name='Mr White', age=20).save()
        self.Person(name='Mr Orange', age=30).save()
        self.Person(name='Mr Pink', age=30).save()
        self.assertEqual(set(self.Person.objects.distinct('name')),
                         set(['Mr Orange', 'Mr White', 'Mr Pink']))
        self.assertEqual(set(self.Person.objects.distinct('age')),
                         set([20, 30]))
        self.assertEqual(set(self.Person.objects(age=30).distinct('name')),
                         set(['Mr Orange', 'Mr Pink']))

    def test_custom_manager(self):
        """Ensure that custom QuerySetManager instances work as expected.
        """
        class BlogPost(Document):
            tags = ListField(StringField())
            deleted = BooleanField(default=False)
            date = DateTimeField(default=datetime.now)

            @queryset_manager
            def objects(doc_cls, queryset):
                return queryset(deleted=False)

            @queryset_manager
            def music_posts(doc_cls, queryset):
                return queryset(tags='music', deleted=False).order_by('-date')

        BlogPost.drop_collection()

        post1 = BlogPost(tags=['music', 'film'])
        post1.save()
        post2 = BlogPost(tags=['music'])
        post2.save()
        post3 = BlogPost(tags=['film', 'actors'])
        post3.save()
        post4 = BlogPost(tags=['film', 'actors'], deleted=True)
        post4.save()

        self.assertEqual([p.id for p in BlogPost.objects],
                         [post1.id, post2.id, post3.id])
        self.assertEqual([p.id for p in BlogPost.music_posts],
                         [post2.id, post1.id])

        BlogPost.drop_collection()

    def test_query_field_name(self):
        """Ensure that the correct field name is used when querying.
        """
        class Comment(EmbeddedDocument):
            content = StringField(db_field='commentContent')

        class BlogPost(Document):
            title = StringField(db_field='postTitle')
            comments = ListField(EmbeddedDocumentField(Comment),
                                 db_field='postComments')


        BlogPost.drop_collection()

        data = {'title': 'Post 1', 'comments': [Comment(content='test')]}
        post = BlogPost(**data)
        post.save()

        self.assertTrue('postTitle' in
                        BlogPost.objects(title=data['title'])._query)
        self.assertFalse('title' in
                         BlogPost.objects(title=data['title'])._query)
        self.assertEqual(len(BlogPost.objects(title=data['title'])), 1)

        self.assertTrue('_id' in BlogPost.objects(pk=post.id)._query)
        self.assertEqual(len(BlogPost.objects(pk=post.id)), 1)

        self.assertTrue('postComments.commentContent' in
                        BlogPost.objects(comments__content='test')._query)
        self.assertEqual(len(BlogPost.objects(comments__content='test')), 1)

        BlogPost.drop_collection()

    def test_query_pk_field_name(self):
        """Ensure that the correct "primary key" field name is used when querying
        """
        class BlogPost(Document):
            title = StringField(primary_key=True, db_field='postTitle')

        BlogPost.drop_collection()

        data = { 'title':'Post 1' }
        post = BlogPost(**data)
        post.save()

        self.assertTrue('_id' in BlogPost.objects(pk=data['title'])._query)
        self.assertTrue('_id' in BlogPost.objects(title=data['title'])._query)
        self.assertEqual(len(BlogPost.objects(pk=data['title'])), 1)

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

    def test_update_value_conversion(self):
        """Ensure that values used in updates are converted before use.
        """
        class Group(Document):
            members = ListField(ReferenceField(self.Person))

        Group.drop_collection()

        user1 = self.Person(name='user1')
        user1.save()
        user2 = self.Person(name='user2')
        user2.save()

        group = Group()
        group.save()

        Group.objects(id=group.id).update(set__members=[user1, user2])
        group.reload()

        self.assertTrue(len(group.members) == 2)
        self.assertEqual(group.members[0].name, user1.name)
        self.assertEqual(group.members[1].name, user2.name)

        Group.drop_collection()

    def test_types_index(self):
        """Ensure that and index is used when '_types' is being used in a
        query.
        """
        class BlogPost(Document):
            date = DateTimeField()
            meta = {'indexes': ['-date']}

        # Indexes are lazy so use list() to perform query
        list(BlogPost.objects)
        info = BlogPost.objects._collection.index_information()
        info = [value['key'] for key, value in info.iteritems()]
        self.assertTrue([('_types', 1)] in info)
        self.assertTrue([('_types', 1), ('date', -1)] in info)

    def test_dont_index_types(self):
        """Ensure that index_types will, when disabled, prevent _types
        being added to all indices.
        """
        class BlogPost(Document):
            date = DateTimeField()
            meta = {'index_types': False,
                    'indexes': ['-date']}

        # Indexes are lazy so use list() to perform query
        list(BlogPost.objects)
        info = BlogPost.objects._collection.index_information()
        info = [value['key'] for key, value in info.iteritems()]
        self.assertTrue([('_types', 1)] not in info)
        self.assertTrue([('date', -1)] in info)

        BlogPost.drop_collection()

        class BlogPost(Document):
            title = StringField()
            meta = {'allow_inheritance': False}

        # _types is not used on objects where allow_inheritance is False
        list(BlogPost.objects)
        info = BlogPost.objects._collection.index_information()
        self.assertFalse([('_types', 1)] in info.values())

        BlogPost.drop_collection()

    def test_dict_with_custom_baseclass(self):
        """Ensure DictField working with custom base clases.
        """
        class Test(Document):
            testdict = DictField()

        Test.drop_collection()

        t = Test(testdict={'f': 'Value'})
        t.save()

        self.assertEqual(len(Test.objects(testdict__f__startswith='Val')), 1)
        self.assertEqual(len(Test.objects(testdict__f='Value')), 1)
        Test.drop_collection()

        class Test(Document):
            testdict = DictField(basecls=StringField)

        t = Test(testdict={'f': 'Value'})
        t.save()

        self.assertEqual(len(Test.objects(testdict__f='Value')), 1)
        self.assertEqual(len(Test.objects(testdict__f__startswith='Val')), 1)
        Test.drop_collection()

    def test_bulk(self):
        """Ensure bulk querying by object id returns a proper dict.
        """
        class BlogPost(Document):
            title = StringField()

        BlogPost.drop_collection()

        post_1 = BlogPost(title="Post #1")
        post_2 = BlogPost(title="Post #2")
        post_3 = BlogPost(title="Post #3")
        post_4 = BlogPost(title="Post #4")
        post_5 = BlogPost(title="Post #5")

        post_1.save()
        post_2.save()
        post_3.save()
        post_4.save()
        post_5.save()

        ids = [post_1.id, post_2.id, post_5.id]
        objects = BlogPost.objects.in_bulk(ids)

        self.assertEqual(len(objects), 3)

        self.assertTrue(post_1.id in objects)
        self.assertTrue(post_2.id in objects)
        self.assertTrue(post_5.id in objects)

        self.assertTrue(objects[post_1.id].title == post_1.title)
        self.assertTrue(objects[post_2.id].title == post_2.title)
        self.assertTrue(objects[post_5.id].title == post_5.title)

        BlogPost.drop_collection()

    def tearDown(self):
        self.Person.drop_collection()

    def test_geospatial_operators(self):
        """Ensure that geospatial queries are working.
        """
        class Event(Document):
            title = StringField()
            date = DateTimeField()
            location = GeoPointField()

            def __unicode__(self):
                return self.title

        Event.drop_collection()

        event1 = Event(title="Coltrane Motion @ Double Door",
                       date=datetime.now() - timedelta(days=1),
                       location=[41.909889, -87.677137])
        event2 = Event(title="Coltrane Motion @ Bottom of the Hill",
                       date=datetime.now() - timedelta(days=10),
                       location=[37.7749295, -122.4194155])
        event3 = Event(title="Coltrane Motion @ Empty Bottle",
                       date=datetime.now(),
                       location=[41.900474, -87.686638])

        event1.save()
        event2.save()
        event3.save()

        # find all events "near" pitchfork office, chicago.
        # note that "near" will show the san francisco event, too,
        # although it sorts to last.
        events = Event.objects(location__near=[41.9120459, -87.67892])
        self.assertEqual(events.count(), 3)
        self.assertEqual(list(events), [event1, event3, event2])

        # find events within 5 degrees of pitchfork office, chicago
        point_and_distance = [[41.9120459, -87.67892], 5]
        events = Event.objects(location__within_distance=point_and_distance)
        self.assertEqual(events.count(), 2)
        events = list(events)
        self.assertTrue(event2 not in events)
        self.assertTrue(event1 in events)
        self.assertTrue(event3 in events)

        # ensure ordering is respected by "near"
        events = Event.objects(location__near=[41.9120459, -87.67892])
        events = events.order_by("-date")
        self.assertEqual(events.count(), 3)
        self.assertEqual(list(events), [event3, event1, event2])

        # find events within 10 degrees of san francisco
        point_and_distance = [[37.7566023, -122.415579], 10]
        events = Event.objects(location__within_distance=point_and_distance)
        self.assertEqual(events.count(), 1)
        self.assertEqual(events[0], event2)

        # find events within 1 degree of greenpoint, broolyn, nyc, ny
        point_and_distance = [[40.7237134, -73.9509714], 1]
        events = Event.objects(location__within_distance=point_and_distance)
        self.assertEqual(events.count(), 0)

        # ensure ordering is respected by "within_distance"
        point_and_distance = [[41.9120459, -87.67892], 10]
        events = Event.objects(location__within_distance=point_and_distance)
        events = events.order_by("-date")
        self.assertEqual(events.count(), 2)
        self.assertEqual(events[0], event3)

        # check that within_box works
        box = [(35.0, -125.0), (40.0, -100.0)]
        events = Event.objects(location__within_box=box)
        self.assertEqual(events.count(), 1)
        self.assertEqual(events[0].id, event2.id)
        
        # check that polygon works for users who have a server >= 1.9
        server_version = tuple(
            _get_connection().server_info()['version'].split('.')
        )
        required_version = tuple("1.9.0".split("."))
        if server_version >= required_version:
            polygon = [
                (41.912114,-87.694445),
                (41.919395,-87.69084),
                (41.927186,-87.681742),
                (41.911731,-87.654276),
                (41.898061,-87.656164),
            ]
            events = Event.objects(location__within_polygon=polygon)
            self.assertEqual(events.count(), 1)
            self.assertEqual(events[0].id, event1.id)
            
            polygon2 = [
                (54.033586,-1.742249),
                (52.792797,-1.225891),
                (53.389881,-4.40094)
            ]
            events = Event.objects(location__within_polygon=polygon2)
            self.assertEqual(events.count(), 0)
            
        Event.drop_collection()

    def test_spherical_geospatial_operators(self):
        """Ensure that spherical geospatial queries are working
        """
        class Point(Document):
            location = GeoPointField()

        Point.drop_collection()

        # These points are one degree apart, which (according to Google Maps)
        # is about 110 km apart at this place on the Earth.
        north_point = Point(location=[-122, 38]) # Near Concord, CA
        south_point = Point(location=[-122, 37]) # Near Santa Cruz, CA
        north_point.save()
        south_point.save()

        earth_radius = 6378.009; # in km (needs to be a float for dividing by)

        # Finds both points because they are within 60 km of the reference
        # point equidistant between them.
        points = Point.objects(location__near_sphere=[-122, 37.5])
        self.assertEqual(points.count(), 2)

        # Same behavior for _within_spherical_distance
        points = Point.objects(
            location__within_spherical_distance=[[-122, 37.5], 60/earth_radius]
        );
        self.assertEqual(points.count(), 2)

        # Finds both points, but orders the north point first because it's
        # closer to the reference point to the north.
        points = Point.objects(location__near_sphere=[-122, 38.5])
        self.assertEqual(points.count(), 2)
        self.assertEqual(points[0].id, north_point.id)
        self.assertEqual(points[1].id, south_point.id)

        # Finds both points, but orders the south point first because it's
        # closer to the reference point to the south.
        points = Point.objects(location__near_sphere=[-122, 36.5])
        self.assertEqual(points.count(), 2)
        self.assertEqual(points[0].id, south_point.id)
        self.assertEqual(points[1].id, north_point.id)

        # Finds only one point because only the first point is within 60km of
        # the reference point to the south.
        points = Point.objects(
            location__within_spherical_distance=[[-122, 36.5], 60/earth_radius]
        );
        self.assertEqual(points.count(), 1)
        self.assertEqual(points[0].id, south_point.id)

        Point.drop_collection()

    def test_custom_querysets(self):
        """Ensure that custom QuerySet classes may be used.
        """
        class CustomQuerySet(QuerySet):
            def not_empty(self):
                return len(self) > 0

        class Post(Document):
            meta = {'queryset_class': CustomQuerySet}

        Post.drop_collection()

        self.assertTrue(isinstance(Post.objects, CustomQuerySet))
        self.assertFalse(Post.objects.not_empty())

        Post().save()
        self.assertTrue(Post.objects.not_empty())

        Post.drop_collection()

    def test_custom_querysets_set_manager_directly(self):
        """Ensure that custom QuerySet classes may be used.
        """

        class CustomQuerySet(QuerySet):
            def not_empty(self):
                return len(self) > 0

        class CustomQuerySetManager(QuerySetManager):
            queryset_class = CustomQuerySet

        class Post(Document):
            objects = CustomQuerySetManager()

        Post.drop_collection()

        self.assertTrue(isinstance(Post.objects, CustomQuerySet))
        self.assertFalse(Post.objects.not_empty())

        Post().save()
        self.assertTrue(Post.objects.not_empty())

        Post.drop_collection()

    def test_custom_querysets_managers_directly(self):
        """Ensure that custom QuerySet classes may be used.
        """

        class CustomQuerySetManager(QuerySetManager):

            @staticmethod
            def get_queryset(doc_cls, queryset):
                return queryset(is_published=True)

        class Post(Document):
            is_published = BooleanField(default=False)
            published = CustomQuerySetManager()

        Post.drop_collection()

        Post().save()
        Post(is_published=True).save()
        self.assertEquals(Post.objects.count(), 2)
        self.assertEquals(Post.published.count(), 1)

        Post.drop_collection()

    def test_custom_querysets_inherited(self):
        """Ensure that custom QuerySet classes may be used.
        """

        class CustomQuerySet(QuerySet):
            def not_empty(self):
                return len(self) > 0

        class Base(Document):
            meta = {'abstract': True, 'queryset_class': CustomQuerySet}

        class Post(Base):
            pass

        Post.drop_collection()
        self.assertTrue(isinstance(Post.objects, CustomQuerySet))
        self.assertFalse(Post.objects.not_empty())

        Post().save()
        self.assertTrue(Post.objects.not_empty())

        Post.drop_collection()

    def test_custom_querysets_inherited_direct(self):
        """Ensure that custom QuerySet classes may be used.
        """

        class CustomQuerySet(QuerySet):
            def not_empty(self):
                return len(self) > 0

        class CustomQuerySetManager(QuerySetManager):
            queryset_class = CustomQuerySet

        class Base(Document):
            meta = {'abstract': True}
            objects = CustomQuerySetManager()

        class Post(Base):
            pass

        Post.drop_collection()
        self.assertTrue(isinstance(Post.objects, CustomQuerySet))
        self.assertFalse(Post.objects.not_empty())

        Post().save()
        self.assertTrue(Post.objects.not_empty())

        Post.drop_collection()

    def test_call_after_limits_set(self):
        """Ensure that re-filtering after slicing works
        """
        class Post(Document):
            title = StringField()

        Post.drop_collection()

        post1 = Post(title="Post 1")
        post1.save()
        post2 = Post(title="Post 2")
        post2.save()

        posts = Post.objects.all()[0:1]
        self.assertEqual(len(list(posts())), 1)

        Post.drop_collection()

    def test_order_then_filter(self):
        """Ensure that ordering still works after filtering.
        """
        class Number(Document):
            n = IntField()

        Number.drop_collection()

        n2 = Number.objects.create(n=2)
        n1 = Number.objects.create(n=1)

        self.assertEqual(list(Number.objects), [n2, n1])
        self.assertEqual(list(Number.objects.order_by('n')), [n1, n2])
        self.assertEqual(list(Number.objects.order_by('n').filter()), [n1, n2])

        Number.drop_collection()

    def test_clone(self):
        """Ensure that cloning clones complex querysets
        """
        class Number(Document):
            n = IntField()

        Number.drop_collection()

        for i in xrange(1, 101):
            t = Number(n=i)
            t.save()

        test = Number.objects
        test2 = test.clone()
        self.assertFalse(test == test2)
        self.assertEqual(test.count(), test2.count())

        test = test.filter(n__gt=11)
        test2 = test.clone()
        self.assertFalse(test == test2)
        self.assertEqual(test.count(), test2.count())

        test = test.limit(10)
        test2 = test.clone()
        self.assertFalse(test == test2)
        self.assertEqual(test.count(), test2.count())

        Number.drop_collection()

    def test_unset_reference(self):
        class Comment(Document):
            text = StringField()

        class Post(Document):
            comment = ReferenceField(Comment)

        Comment.drop_collection()
        Post.drop_collection()

        comment = Comment.objects.create(text='test')
        post = Post.objects.create(comment=comment)

        self.assertEqual(post.comment, comment)
        Post.objects.update(unset__comment=1)
        post.reload()
        self.assertEqual(post.comment, None)

        Comment.drop_collection()
        Post.drop_collection()

    def test_order_works_with_custom_db_field_names(self):
        class Number(Document):
            n = IntField(db_field='number')

        Number.drop_collection()

        n2 = Number.objects.create(n=2)
        n1 = Number.objects.create(n=1)

        self.assertEqual(list(Number.objects), [n2,n1])
        self.assertEqual(list(Number.objects.order_by('n')), [n1,n2])

        Number.drop_collection()

    def test_order_works_with_primary(self):
        """Ensure that order_by and primary work.
        """
        class Number(Document):
            n = IntField(primary_key=True)

        Number.drop_collection()

        Number(n=1).save()
        Number(n=2).save()
        Number(n=3).save()

        numbers = [n.n for n in Number.objects.order_by('-n')]
        self.assertEquals([3, 2, 1], numbers)

        numbers = [n.n for n in Number.objects.order_by('+n')]
        self.assertEquals([1, 2, 3], numbers)
        Number.drop_collection()


    def test_ensure_index(self):
        """Ensure that manual creation of indexes works.
        """
        class Comment(Document):
            message = StringField()

        Comment.objects.ensure_index('message')

        info = Comment.objects._collection.index_information()
        info = [(value['key'],
                 value.get('unique', False),
                 value.get('sparse', False))
                for key, value in info.iteritems()]
        self.assertTrue(([('_types', 1), ('message', 1)], False, False) in info)

    def test_where(self):
        """Ensure that where clauses work.
        """

        class IntPair(Document):
            fielda = IntField()
            fieldb = IntField()

        IntPair.objects._collection.remove()

        a = IntPair(fielda=1, fieldb=1)
        b = IntPair(fielda=1, fieldb=2)
        c = IntPair(fielda=2, fieldb=1)
        a.save()
        b.save()
        c.save()

        query = IntPair.objects.where('this[~fielda] >= this[~fieldb]')
        self.assertEqual('this["fielda"] >= this["fieldb"]', query._where_clause)
        results = list(query)
        self.assertEqual(2, len(results))
        self.assertTrue(a in results)
        self.assertTrue(c in results)

        query = IntPair.objects.where('this[~fielda] == this[~fieldb]')
        results = list(query)
        self.assertEqual(1, len(results))
        self.assertTrue(a in results)

        query = IntPair.objects.where('function() { return this[~fielda] >= this[~fieldb] }')
        self.assertEqual('function() { return this["fielda"] >= this["fieldb"] }', query._where_clause)
        results = list(query)
        self.assertEqual(2, len(results))
        self.assertTrue(a in results)
        self.assertTrue(c in results)

        def invalid_where():
            list(IntPair.objects.where(fielda__gte=3))

        self.assertRaises(TypeError, invalid_where)


class QTest(unittest.TestCase):

    def setUp(self):
        connect(db='mongoenginetest')

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

class QueryFieldListTest(unittest.TestCase):
    def test_empty(self):
        q = QueryFieldList()
        self.assertFalse(q)

        q = QueryFieldList(always_include=['_cls'])
        self.assertFalse(q)

    def test_include_include(self):
        q = QueryFieldList()
        q += QueryFieldList(fields=['a', 'b'], value=QueryFieldList.ONLY)
        self.assertEqual(q.as_dict(), {'a': True, 'b': True})
        q += QueryFieldList(fields=['b', 'c'], value=QueryFieldList.ONLY)
        self.assertEqual(q.as_dict(), {'b': True})

    def test_include_exclude(self):
        q = QueryFieldList()
        q += QueryFieldList(fields=['a', 'b'], value=QueryFieldList.ONLY)
        self.assertEqual(q.as_dict(), {'a': True, 'b': True})
        q += QueryFieldList(fields=['b', 'c'], value=QueryFieldList.EXCLUDE)
        self.assertEqual(q.as_dict(), {'a': True})

    def test_exclude_exclude(self):
        q = QueryFieldList()
        q += QueryFieldList(fields=['a', 'b'], value=QueryFieldList.EXCLUDE)
        self.assertEqual(q.as_dict(), {'a': False, 'b': False})
        q += QueryFieldList(fields=['b', 'c'], value=QueryFieldList.EXCLUDE)
        self.assertEqual(q.as_dict(), {'a': False, 'b': False, 'c': False})

    def test_exclude_include(self):
        q = QueryFieldList()
        q += QueryFieldList(fields=['a', 'b'], value=QueryFieldList.EXCLUDE)
        self.assertEqual(q.as_dict(), {'a': False, 'b': False})
        q += QueryFieldList(fields=['b', 'c'], value=QueryFieldList.ONLY)
        self.assertEqual(q.as_dict(), {'c': True})

    def test_always_include(self):
        q = QueryFieldList(always_include=['x', 'y'])
        q += QueryFieldList(fields=['a', 'b', 'x'], value=QueryFieldList.EXCLUDE)
        q += QueryFieldList(fields=['b', 'c'], value=QueryFieldList.ONLY)
        self.assertEqual(q.as_dict(), {'x': True, 'y': True, 'c': True})

    def test_reset(self):
        q = QueryFieldList(always_include=['x', 'y'])
        q += QueryFieldList(fields=['a', 'b', 'x'], value=QueryFieldList.EXCLUDE)
        q += QueryFieldList(fields=['b', 'c'], value=QueryFieldList.ONLY)
        self.assertEqual(q.as_dict(), {'x': True, 'y': True, 'c': True})
        q.reset()
        self.assertFalse(q)
        q += QueryFieldList(fields=['b', 'c'], value=QueryFieldList.ONLY)
        self.assertEqual(q.as_dict(), {'x': True, 'y': True, 'b': True, 'c': True})

    def test_using_a_slice(self):
        q = QueryFieldList()
        q += QueryFieldList(fields=['a'], value={"$slice": 5})
        self.assertEqual(q.as_dict(), {'a': {"$slice": 5}})


if __name__ == '__main__':
    unittest.main()
