# -*- coding: utf-8 -*-

import datetime
import unittest
import uuid

from bson import DBRef, ObjectId
from nose.plugins.skip import SkipTest
import pymongo
from pymongo.errors import ConfigurationError
from pymongo.read_preferences import ReadPreference
from pymongo.results import UpdateResult
import six

from mongoengine import *
from mongoengine.connection import get_connection, get_db
from mongoengine.context_managers import query_counter, switch_db
from mongoengine.errors import InvalidQueryError
from mongoengine.python_support import IS_PYMONGO_3
from mongoengine.queryset import (DoesNotExist, MultipleObjectsReturned,
                                  QuerySet, QuerySetManager, queryset_manager)

from tests.utils import needs_mongodb_v26, skip_pymongo3


__all__ = ("QuerySetTest",)


class db_ops_tracker(query_counter):

    def get_ops(self):
        ignore_query = {
            'ns': {'$ne': '%s.system.indexes' % self.db.name},
            'command.count': {'$ne': 'system.profile'}
        }
        return list(self.db.system.profile.find(ignore_query))


class QuerySetTest(unittest.TestCase):

    def setUp(self):
        connect(db='mongoenginetest')
        connect(db='mongoenginetest2', alias='test2')

        class PersonMeta(EmbeddedDocument):
            weight = IntField()

        class Person(Document):
            name = StringField()
            age = IntField()
            person_meta = EmbeddedDocumentField(PersonMeta)
            meta = {'allow_inheritance': True}

        Person.drop_collection()
        self.PersonMeta = PersonMeta
        self.Person = Person

    def test_initialisation(self):
        """Ensure that a QuerySet is correctly initialised by QuerySetManager.
        """
        self.assertTrue(isinstance(self.Person.objects, QuerySet))
        self.assertEqual(self.Person.objects._collection.name,
                         self.Person._get_collection_name())
        self.assertTrue(isinstance(self.Person.objects._collection,
                                   pymongo.collection.Collection))

    def test_cannot_perform_joins_references(self):

        class BlogPost(Document):
            author = ReferenceField(self.Person)
            author2 = GenericReferenceField()

        # test addressing a field from a reference
        with self.assertRaises(InvalidQueryError):
            list(BlogPost.objects(author__name="test"))

        # should fail for a generic reference as well
        with self.assertRaises(InvalidQueryError):
            list(BlogPost.objects(author2__name="test"))

    def test_find(self):
        """Ensure that a query returns a valid set of results."""
        user_a = self.Person.objects.create(name='User A', age=20)
        user_b = self.Person.objects.create(name='User B', age=30)

        # Find all people in the collection
        people = self.Person.objects
        self.assertEqual(people.count(), 2)
        results = list(people)

        self.assertTrue(isinstance(results[0], self.Person))
        self.assertTrue(isinstance(results[0].id, (ObjectId, str, unicode)))

        self.assertEqual(results[0], user_a)
        self.assertEqual(results[0].name, 'User A')
        self.assertEqual(results[0].age, 20)

        self.assertEqual(results[1], user_b)
        self.assertEqual(results[1].name, 'User B')
        self.assertEqual(results[1].age, 30)

        # Filter people by age
        people = self.Person.objects(age=20)
        self.assertEqual(people.count(), 1)
        person = people.next()
        self.assertEqual(person, user_a)
        self.assertEqual(person.name, "User A")
        self.assertEqual(person.age, 20)

    def test_limit(self):
        """Ensure that QuerySet.limit works as expected."""
        user_a = self.Person.objects.create(name='User A', age=20)
        user_b = self.Person.objects.create(name='User B', age=30)

        # Test limit on a new queryset
        people = list(self.Person.objects.limit(1))
        self.assertEqual(len(people), 1)
        self.assertEqual(people[0], user_a)

        # Test limit on an existing queryset
        people = self.Person.objects
        self.assertEqual(len(people), 2)
        people2 = people.limit(1)
        self.assertEqual(len(people), 2)
        self.assertEqual(len(people2), 1)
        self.assertEqual(people2[0], user_a)

        # Test chaining of only after limit
        person = self.Person.objects().limit(1).only('name').first()
        self.assertEqual(person, user_a)
        self.assertEqual(person.name, 'User A')
        self.assertEqual(person.age, None)

    def test_skip(self):
        """Ensure that QuerySet.skip works as expected."""
        user_a = self.Person.objects.create(name='User A', age=20)
        user_b = self.Person.objects.create(name='User B', age=30)

        # Test skip on a new queryset
        people = list(self.Person.objects.skip(1))
        self.assertEqual(len(people), 1)
        self.assertEqual(people[0], user_b)

        # Test skip on an existing queryset
        people = self.Person.objects
        self.assertEqual(len(people), 2)
        people2 = people.skip(1)
        self.assertEqual(len(people), 2)
        self.assertEqual(len(people2), 1)
        self.assertEqual(people2[0], user_b)

        # Test chaining of only after skip
        person = self.Person.objects().skip(1).only('name').first()
        self.assertEqual(person, user_b)
        self.assertEqual(person.name, 'User B')
        self.assertEqual(person.age, None)

    def test_slice(self):
        """Ensure slicing a queryset works as expected."""
        user_a = self.Person.objects.create(name='User A', age=20)
        user_b = self.Person.objects.create(name='User B', age=30)
        user_c = self.Person.objects.create(name="User C", age=40)

        # Test slice limit
        people = list(self.Person.objects[:2])
        self.assertEqual(len(people), 2)
        self.assertEqual(people[0], user_a)
        self.assertEqual(people[1], user_b)

        # Test slice skip
        people = list(self.Person.objects[1:])
        self.assertEqual(len(people), 2)
        self.assertEqual(people[0], user_b)
        self.assertEqual(people[1], user_c)

        # Test slice limit and skip
        people = list(self.Person.objects[1:2])
        self.assertEqual(len(people), 1)
        self.assertEqual(people[0], user_b)

        # Test slice limit and skip on an existing queryset
        people = self.Person.objects
        self.assertEqual(len(people), 3)
        people2 = people[1:2]
        self.assertEqual(len(people2), 1)
        self.assertEqual(people2[0], user_b)

        # Test slice limit and skip cursor reset
        qs = self.Person.objects[1:2]
        # fetch then delete the cursor
        qs._cursor
        qs._cursor_obj = None
        people = list(qs)
        self.assertEqual(len(people), 1)
        self.assertEqual(people[0].name, 'User B')

        # Test empty slice
        people = list(self.Person.objects[1:1])
        self.assertEqual(len(people), 0)

        # Test slice out of range
        people = list(self.Person.objects[80000:80001])
        self.assertEqual(len(people), 0)

        # Test larger slice __repr__
        self.Person.objects.delete()
        for i in range(55):
            self.Person(name='A%s' % i, age=i).save()

        self.assertEqual(self.Person.objects.count(), 55)
        self.assertEqual("Person object", "%s" % self.Person.objects[0])
        self.assertEqual("[<Person: Person object>, <Person: Person object>]",
                         "%s" % self.Person.objects[1:3])
        self.assertEqual("[<Person: Person object>, <Person: Person object>]",
                         "%s" % self.Person.objects[51:53])

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

        with self.assertRaises(IndexError):
            self.Person.objects[2]

        # Find a document using just the object id
        person = self.Person.objects.with_id(person1.id)
        self.assertEqual(person.name, "User A")

        with self.assertRaises(InvalidQueryError):
            self.Person.objects(name="User A").with_id(person1.id)

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
        self.assertEqual(Blog.objects(tags__0='a').count(), 1)
        self.assertEqual(Blog.objects(tags__0='b').count(), 0)
        self.assertEqual(Blog.objects(tags__1='a').count(), 0)
        self.assertEqual(Blog.objects(tags__1='b').count(), 1)

        Blog.drop_collection()

        comment1 = Comment(name='testa')
        comment2 = Comment(name='testb')
        post1 = Post(comments=[comment1, comment2])
        post2 = Post(comments=[comment2, comment2])
        blog1 = Blog.objects.create(posts=[post1, post2])
        blog2 = Blog.objects.create(posts=[post2, post1])

        blog = Blog.objects(posts__0__comments__0__name='testa').get()
        self.assertEqual(blog, blog1)

        blog = Blog.objects(posts__0__comments__0__name='testb').get()
        self.assertEqual(blog, blog2)

        query = Blog.objects(posts__1__comments__1__name='testb')
        self.assertEqual(query.count(), 2)

        query = Blog.objects(posts__1__comments__1__name='testa')
        self.assertEqual(query.count(), 0)

        query = Blog.objects(posts__0__comments__1__name='testa')
        self.assertEqual(query.count(), 0)

        Blog.drop_collection()

    def test_none(self):
        class A(Document):
            s = StringField()

        A.drop_collection()
        A().save()

        self.assertEqual(list(A.objects.none()), [])
        self.assertEqual(list(A.objects.none().all()), [])

    def test_chaining(self):
        class A(Document):
            s = StringField()

        class B(Document):
            ref = ReferenceField(A)
            boolfield = BooleanField(default=False)

        A.drop_collection()
        B.drop_collection()

        a1 = A(s="test1").save()
        a2 = A(s="test2").save()

        B(ref=a1, boolfield=True).save()

        # Works
        q1 = B.objects.filter(ref__in=[a1, a2], ref=a1)._query

        # Doesn't work
        q2 = B.objects.filter(ref__in=[a1, a2])
        q2 = q2.filter(ref=a1)._query
        self.assertEqual(q1, q2)

        a_objects = A.objects(s='test1')
        query = B.objects(ref__in=a_objects)
        query = query.filter(boolfield=True)
        self.assertEqual(query.count(), 1)

    def test_batch_size(self):
        """Ensure that batch_size works."""
        class A(Document):
            s = StringField()

        A.drop_collection()

        for i in range(100):
            A.objects.create(s=str(i))

        # test iterating over the result set
        cnt = 0
        for a in A.objects.batch_size(10):
            cnt += 1
        self.assertEqual(cnt, 100)

        # test chaining
        qs = A.objects.all()
        qs = qs.limit(10).batch_size(20).skip(91)
        cnt = 0
        for a in qs:
            cnt += 1
        self.assertEqual(cnt, 9)

        # test invalid batch size
        qs = A.objects.batch_size(-1)
        with self.assertRaises(ValueError):
            list(qs)

    def test_update_write_concern(self):
        """Test that passing write_concern works"""
        self.Person.drop_collection()

        write_concern = {"fsync": True}

        author = self.Person.objects.create(name='Test User')
        author.save(write_concern=write_concern)

        result = self.Person.objects.update(
            set__name='Ross', write_concern={"w": 1})
        self.assertEqual(result, 1)
        result = self.Person.objects.update(
            set__name='Ross', write_concern={"w": 0})
        self.assertEqual(result, None)

        result = self.Person.objects.update_one(
            set__name='Test User', write_concern={"w": 1})
        self.assertEqual(result, 1)
        result = self.Person.objects.update_one(
            set__name='Test User', write_concern={"w": 0})
        self.assertEqual(result, None)

    def test_update_update_has_a_value(self):
        """Test to ensure that update is passed a value to update to"""
        self.Person.drop_collection()

        author = self.Person.objects.create(name='Test User')

        with self.assertRaises(OperationError):
            self.Person.objects(pk=author.pk).update({})

        with self.assertRaises(OperationError):
            self.Person.objects(pk=author.pk).update_one({})

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
        Blog.objects.create(posts=[post1, post2])
        Blog.objects.create(posts=[post2, post1])

        # Update all of the first comments of second posts of all blogs
        Blog.objects().update(set__posts__1__comments__0__name='testc')
        testc_blogs = Blog.objects(posts__1__comments__0__name='testc')
        self.assertEqual(testc_blogs.count(), 2)

        Blog.drop_collection()
        Blog.objects.create(posts=[post1, post2])
        Blog.objects.create(posts=[post2, post1])

        # Update only the first blog returned by the query
        Blog.objects().update_one(
            set__posts__1__comments__1__name='testc')
        testc_blogs = Blog.objects(posts__1__comments__1__name='testc')
        self.assertEqual(testc_blogs.count(), 1)

        # Check that using this indexing syntax on a non-list fails
        with self.assertRaises(InvalidQueryError):
            Blog.objects().update(set__posts__1__comments__0__name__1='asdf')

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

        BlogPost.objects(comments__by="jane").update(inc__comments__S__votes=1)

        post = BlogPost.objects.first()
        self.assertEqual(post.comments[1].by, 'jane')
        self.assertEqual(post.comments[1].votes, 8)

    def test_update_using_positional_operator_matches_first(self):

        # Currently the $ operator only applies to the first matched item in
        # the query

        class Simple(Document):
            x = ListField()

        Simple.drop_collection()
        Simple(x=[1, 2, 3, 2]).save()
        Simple.objects(x=2).update(inc__x__S=1)

        simple = Simple.objects.first()
        self.assertEqual(simple.x, [1, 3, 3, 2])
        Simple.drop_collection()

        # You can set multiples
        Simple.drop_collection()
        Simple(x=[1, 2, 3, 4]).save()
        Simple(x=[2, 3, 4, 5]).save()
        Simple(x=[3, 4, 5, 6]).save()
        Simple(x=[4, 5, 6, 7]).save()
        Simple.objects(x=3).update(set__x__S=0)

        s = Simple.objects()
        self.assertEqual(s[0].x, [1, 2, 0, 4])
        self.assertEqual(s[1].x, [2, 0, 4, 5])
        self.assertEqual(s[2].x, [0, 4, 5, 6])
        self.assertEqual(s[3].x, [4, 5, 6, 7])

        # Using "$unset" with an expression like this "array.$" will result in
        # the array item becoming None, not being removed.
        Simple.drop_collection()
        Simple(x=[1, 2, 3, 4, 3, 2, 3, 4]).save()
        Simple.objects(x=3).update(unset__x__S=1)
        simple = Simple.objects.first()
        self.assertEqual(simple.x, [1, 2, None, 4, 3, 2, 3, 4])

        # Nested updates arent supported yet..
        with self.assertRaises(OperationError):
            Simple.drop_collection()
            Simple(x=[{'test': [1, 2, 3, 4]}]).save()
            Simple.objects(x__test=2).update(set__x__S__test__S=3)
            self.assertEqual(simple.x, [1, 2, 3, 4])

    def test_update_using_positional_operator_embedded_document(self):
        """Ensure that the embedded documents can be updated using the positional
        operator."""

        class Vote(EmbeddedDocument):
            score = IntField()

        class Comment(EmbeddedDocument):
            by = StringField()
            votes = EmbeddedDocumentField(Vote)

        class BlogPost(Document):
            title = StringField()
            comments = ListField(EmbeddedDocumentField(Comment))

        BlogPost.drop_collection()

        c1 = Comment(by="joe", votes=Vote(score=3))
        c2 = Comment(by="jane", votes=Vote(score=7))

        BlogPost(title="ABC", comments=[c1, c2]).save()

        BlogPost.objects(comments__by="joe").update(
            set__comments__S__votes=Vote(score=4))

        post = BlogPost.objects.first()
        self.assertEqual(post.comments[0].by, 'joe')
        self.assertEqual(post.comments[0].votes.score, 4)

    @needs_mongodb_v26
    def test_update_min_max(self):
        class Scores(Document):
            high_score = IntField()
            low_score = IntField()

        scores = Scores.objects.create(high_score=800, low_score=200)

        Scores.objects(id=scores.id).update(min__low_score=150)
        self.assertEqual(Scores.objects.get(id=scores.id).low_score, 150)
        Scores.objects(id=scores.id).update(min__low_score=250)
        self.assertEqual(Scores.objects.get(id=scores.id).low_score, 150)

        Scores.objects(id=scores.id).update(max__high_score=1000)
        self.assertEqual(Scores.objects.get(id=scores.id).high_score, 1000)
        Scores.objects(id=scores.id).update(max__high_score=500)
        self.assertEqual(Scores.objects.get(id=scores.id).high_score, 1000)

    @needs_mongodb_v26
    def test_update_multiple(self):
        class Product(Document):
            item = StringField()
            price = FloatField()

        product = Product.objects.create(item='ABC', price=10.99)
        product = Product.objects.create(item='ABC', price=10.99)
        Product.objects(id=product.id).update(mul__price=1.25)
        self.assertEqual(Product.objects.get(id=product.id).price, 13.7375)
        unknown_product = Product.objects.create(item='Unknown')
        Product.objects(id=unknown_product.id).update(mul__price=100)
        self.assertEqual(Product.objects.get(id=unknown_product.id).price, 0)

    def test_updates_can_have_match_operators(self):

        class Comment(EmbeddedDocument):
            content = StringField()
            name = StringField(max_length=120)
            vote = IntField()

        class Post(Document):
            title = StringField(required=True)
            tags = ListField(StringField())
            comments = ListField(EmbeddedDocumentField("Comment"))

        Post.drop_collection()

        comm1 = Comment(content="very funny indeed", name="John S", vote=1)
        comm2 = Comment(content="kind of funny", name="Mark P", vote=0)

        Post(title='Fun with MongoEngine', tags=['mongodb', 'mongoengine'],
             comments=[comm1, comm2]).save()

        Post.objects().update_one(pull__comments__vote__lt=1)

        self.assertEqual(1, len(Post.objects.first().comments))

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
        club.members['John'] = {'gender': 'M', 'age': 13}
        club.save()

        Club.objects().update(
            set__members={"John": {'gender': 'F', 'age': 14}})

        club = Club.objects().first()
        self.assertEqual(club.members['John']['gender'], "F")
        self.assertEqual(club.members['John']['age'], 14)

    def test_update_results(self):
        self.Person.drop_collection()

        result = self.Person(name="Bob", age=25).update(
            upsert=True, full_result=True)
        self.assertTrue(isinstance(result, UpdateResult))
        self.assertTrue("upserted" in result.raw_result)
        self.assertFalse(result.raw_result["updatedExisting"])

        bob = self.Person.objects.first()
        result = bob.update(set__age=30, full_result=True)
        self.assertTrue(isinstance(result, UpdateResult))
        self.assertTrue(result.raw_result["updatedExisting"])

        self.Person(name="Bob", age=20).save()
        result = self.Person.objects(name="Bob").update(
            set__name="bobby", multi=True)
        self.assertEqual(result, 2)

    def test_update_validate(self):
        class EmDoc(EmbeddedDocument):
            str_f = StringField()

        class Doc(Document):
            str_f = StringField()
            dt_f = DateTimeField()
            cdt_f = ComplexDateTimeField()
            ed_f = EmbeddedDocumentField(EmDoc)

        self.assertRaises(ValidationError, Doc.objects().update, str_f=1, upsert=True)
        self.assertRaises(ValidationError, Doc.objects().update, dt_f="datetime", upsert=True)
        self.assertRaises(ValidationError, Doc.objects().update, ed_f__str_f=1, upsert=True)

    def test_update_related_models(self):
            class TestPerson(Document):
                name = StringField()

            class TestOrganization(Document):
                name = StringField()
                owner = ReferenceField(TestPerson)

            TestPerson.drop_collection()
            TestOrganization.drop_collection()

            p = TestPerson(name='p1')
            p.save()
            o = TestOrganization(name='o1')
            o.save()

            o.owner = p
            p.name = 'p2'

            self.assertEqual(o._get_changed_fields(), ['owner'])
            self.assertEqual(p._get_changed_fields(), ['name'])

            o.save()

            self.assertEqual(o._get_changed_fields(), [])
            self.assertEqual(p._get_changed_fields(), ['name'])  # Fails; it's empty

            # This will do NOTHING at all, even though we changed the name
            p.save()

            p.reload()

            self.assertEqual(p.name, 'p2')  # Fails; it's still `p1`

    def test_upsert(self):
        self.Person.drop_collection()

        self.Person.objects(
            pk=ObjectId(), name="Bob", age=30).update(upsert=True)

        bob = self.Person.objects.first()
        self.assertEqual("Bob", bob.name)
        self.assertEqual(30, bob.age)

    def test_upsert_one(self):
        self.Person.drop_collection()

        bob = self.Person.objects(name="Bob", age=30).upsert_one()

        self.assertEqual("Bob", bob.name)
        self.assertEqual(30, bob.age)

        bob.name = "Bobby"
        bob.save()

        bobby = self.Person.objects(name="Bobby", age=30).upsert_one()

        self.assertEqual("Bobby", bobby.name)
        self.assertEqual(30, bobby.age)
        self.assertEqual(bob.id, bobby.id)

    def test_set_on_insert(self):
        self.Person.drop_collection()

        self.Person.objects(pk=ObjectId()).update(
            set__name='Bob', set_on_insert__age=30, upsert=True)

        bob = self.Person.objects.first()
        self.assertEqual("Bob", bob.name)
        self.assertEqual(30, bob.age)

    def test_save_and_only_on_fields_with_default(self):
        class Embed(EmbeddedDocument):
            field = IntField()

        class B(Document):
            meta = {'collection': 'b'}

            field = IntField(default=1)
            embed = EmbeddedDocumentField(Embed, default=Embed)
            embed_no_default = EmbeddedDocumentField(Embed)

        # Creating {field : 2, embed : {field: 2}, embed_no_default: {field: 2}}
        val = 2
        embed = Embed()
        embed.field = val
        record = B()
        record.field = val
        record.embed = embed
        record.embed_no_default = embed
        record.save()

        # Checking it was saved correctly
        record.reload()
        self.assertEqual(record.field, 2)
        self.assertEqual(record.embed_no_default.field, 2)
        self.assertEqual(record.embed.field, 2)

        # Request only the _id field and save
        clone = B.objects().only('id').first()
        clone.save()

        # Reload the record and see that the embed data is not lost
        record.reload()
        self.assertEqual(record.field, 2)
        self.assertEqual(record.embed_no_default.field, 2)
        self.assertEqual(record.embed.field, 2)

    def test_bulk_insert(self):
        """Ensure that bulk insert works
        """

        class Comment(EmbeddedDocument):
            name = StringField()

        class Post(EmbeddedDocument):
            comments = ListField(EmbeddedDocumentField(Comment))

        class Blog(Document):
            title = StringField(unique=True)
            tags = ListField(StringField())
            posts = ListField(EmbeddedDocumentField(Post))

        Blog.drop_collection()

        # get MongoDB version info
        connection = get_connection()
        info = connection.test.command('buildInfo')
        mongodb_version = tuple([int(i) for i in info['version'].split('.')])

        # Recreates the collection
        self.assertEqual(0, Blog.objects.count())

        with query_counter() as q:
            self.assertEqual(q, 0)

            comment1 = Comment(name='testa')
            comment2 = Comment(name='testb')
            post1 = Post(comments=[comment1, comment2])
            post2 = Post(comments=[comment2, comment2])

            blogs = []
            for i in range(1, 100):
                blogs.append(Blog(title="post %s" % i, posts=[post1, post2]))

            Blog.objects.insert(blogs, load_bulk=False)
            # profiling logs each doc now in the bulk op
            self.assertEqual(q, 99)

        Blog.drop_collection()
        Blog.ensure_indexes()

        with query_counter() as q:
            self.assertEqual(q, 0)

            Blog.objects.insert(blogs)
            self.assertEqual(q, 100) # 99 for insert 1 for fetch

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

        # test inserting an existing document (shouldn't be allowed)
        with self.assertRaises(OperationError):
            blog = Blog.objects.first()
            Blog.objects.insert(blog)

        # test inserting a query set
        with self.assertRaises(OperationError):
            blogs = Blog.objects
            Blog.objects.insert(blogs)

        # insert a new doc
        new_post = Blog(title="code123", id=ObjectId())
        Blog.objects.insert(new_post)

        class Author(Document):
            pass

        # try inserting a different document class
        with self.assertRaises(OperationError):
            Blog.objects.insert(Author())

        # try inserting a non-document
        with self.assertRaises(OperationError):
            Blog.objects.insert("HELLO WORLD")

        Blog.drop_collection()

        blog1 = Blog(title="code", posts=[post1, post2])
        blog1 = Blog.objects.insert(blog1)
        self.assertEqual(blog1.title, "code")
        self.assertEqual(Blog.objects.count(), 1)

        Blog.drop_collection()
        blog1 = Blog(title="code", posts=[post1, post2])
        obj_id = Blog.objects.insert(blog1, load_bulk=False)
        self.assertEqual(obj_id.__class__.__name__, 'ObjectId')

        Blog.drop_collection()
        post3 = Post(comments=[comment1, comment1])
        blog1 = Blog(title="foo", posts=[post1, post2])
        blog2 = Blog(title="bar", posts=[post2, post3])
        blog3 = Blog(title="baz", posts=[post1, post2])
        Blog.objects.insert([blog1, blog2])

        with self.assertRaises(NotUniqueError):
            Blog.objects.insert([blog2, blog3])

        self.assertEqual(Blog.objects.count(), 2)

    def test_get_changed_fields_query_count(self):
        """Make sure we don't perform unnecessary db operations when
        none of document's fields were updated.
        """
        class Person(Document):
            name = StringField()
            owns = ListField(ReferenceField('Organization'))
            projects = ListField(ReferenceField('Project'))

        class Organization(Document):
            name = StringField()
            owner = ReferenceField(Person)
            employees = ListField(ReferenceField(Person))

        class Project(Document):
            name = StringField()

        Person.drop_collection()
        Organization.drop_collection()
        Project.drop_collection()

        r1 = Project(name="r1").save()
        r2 = Project(name="r2").save()
        r3 = Project(name="r3").save()
        p1 = Person(name="p1", projects=[r1, r2]).save()
        p2 = Person(name="p2", projects=[r2, r3]).save()
        o1 = Organization(name="o1", employees=[p1]).save()

        with query_counter() as q:
            self.assertEqual(q, 0)

            # Fetching a document should result in a query.
            org = Organization.objects.get(id=o1.id)
            self.assertEqual(q, 1)

            # Checking changed fields of a newly fetched document should not
            # result in a query.
            org._get_changed_fields()
            self.assertEqual(q, 1)

        # Saving a doc without changing any of its fields should not result
        # in a query (with or without cascade=False).
        org = Organization.objects.get(id=o1.id)
        with query_counter() as q:
            org.save()
            self.assertEqual(q, 0)

        org = Organization.objects.get(id=o1.id)
        with query_counter() as q:
            org.save(cascade=False)
            self.assertEqual(q, 0)

        # Saving a doc after you append a reference to it should result in
        # two db operations (a query for the reference and an update).
        # TODO dereferencing of p2 shouldn't be necessary.
        org = Organization.objects.get(id=o1.id)
        with query_counter() as q:
            org.employees.append(p2)  # dereferences p2
            org.save()  # saves the org
            self.assertEqual(q, 2)

    @skip_pymongo3
    def test_slave_okay(self):
        """Ensures that a query can take slave_okay syntax.
        Useless with PyMongo 3+ as well as with MongoDB 3+.
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

    @needs_mongodb_v26
    @skip_pymongo3
    def test_cursor_args(self):
        """Ensures the cursor args can be set as expected
        """
        p = self.Person.objects
        # Check default
        self.assertEqual(p._cursor_args,
                         {'snapshot': False, 'slave_okay': False, 'timeout': True})

        p = p.snapshot(False).slave_okay(False).timeout(False)
        self.assertEqual(p._cursor_args,
                         {'snapshot': False, 'slave_okay': False, 'timeout': False})

        p = p.snapshot(True).slave_okay(False).timeout(False)
        self.assertEqual(p._cursor_args,
                         {'snapshot': True, 'slave_okay': False, 'timeout': False})

        p = p.snapshot(True).slave_okay(True).timeout(False)
        self.assertEqual(p._cursor_args,
                         {'snapshot': True, 'slave_okay': True, 'timeout': False})

        p = p.snapshot(True).slave_okay(True).timeout(True)
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

        # Check that it still works even if iteration is interrupted.
        for person in queryset:
            break
        people3 = [person for person in queryset]

        self.assertEqual(people1, people2)
        self.assertEqual(people1, people3)

    def test_repr(self):
        """Test repr behavior isnt destructive"""

        class Doc(Document):
            number = IntField()

            def __repr__(self):
                return "<Doc: %s>" % self.number

        Doc.drop_collection()

        for i in range(1000):
            Doc(number=i).save()

        docs = Doc.objects.order_by('number')

        self.assertEqual(docs.count(), 1000)

        docs_string = "%s" % docs
        self.assertTrue("Doc: 0" in docs_string)

        self.assertEqual(docs.count(), 1000)
        self.assertTrue('(remaining elements truncated)' in "%s" % docs)

        # Limit and skip
        docs = docs[1:4]
        self.assertEqual('[<Doc: 1>, <Doc: 2>, <Doc: 3>]', "%s" % docs)

        self.assertEqual(docs.count(with_limit_and_skip=True), 3)
        for doc in docs:
            self.assertEqual('.. queryset mid-iteration ..', repr(docs))

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

        # Test icontains
        obj = self.Person.objects(name__icontains='Van').first()
        self.assertEqual(obj, person)

        # Test startswith
        obj = self.Person.objects(name__startswith='Guido').first()
        self.assertEqual(obj, person)
        obj = self.Person.objects(name__startswith='guido').first()
        self.assertEqual(obj, None)

        # Test istartswith
        obj = self.Person.objects(name__istartswith='guido').first()
        self.assertEqual(obj, person)

        # Test endswith
        obj = self.Person.objects(name__endswith='Rossum').first()
        self.assertEqual(obj, person)
        obj = self.Person.objects(name__endswith='rossuM').first()
        self.assertEqual(obj, None)

        # Test iendswith
        obj = self.Person.objects(name__iendswith='rossuM').first()
        self.assertEqual(obj, person)

        # Test exact
        obj = self.Person.objects(name__exact='Guido van Rossum').first()
        self.assertEqual(obj, person)
        obj = self.Person.objects(name__exact='Guido van rossum').first()
        self.assertEqual(obj, None)
        obj = self.Person.objects(name__exact='Guido van Rossu').first()
        self.assertEqual(obj, None)

        # Test iexact
        obj = self.Person.objects(name__iexact='gUIDO VAN rOSSUM').first()
        self.assertEqual(obj, person)
        obj = self.Person.objects(name__iexact='gUIDO VAN rOSSU').first()
        self.assertEqual(obj, None)

        # Test unsafe expressions
        person = self.Person(name='Guido van Rossum [.\'Geek\']')
        person.save()

        obj = self.Person.objects(name__icontains='[.\'Geek').first()
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
        class Blog(Document):
            id = StringField(unique=True, primary_key=True)

        class BlogPost(Document):
            blog = ReferenceField(Blog)
            title = StringField()
            is_published = BooleanField()
            published_date = DateTimeField()

            @queryset_manager
            def published(doc_cls, queryset):
                return queryset(is_published=True)

        Blog.drop_collection()
        BlogPost.drop_collection()

        blog_1 = Blog(id="1")
        blog_2 = Blog(id="2")
        blog_3 = Blog(id="3")

        blog_1.save()
        blog_2.save()
        blog_3.save()

        BlogPost.objects.create(
            blog=blog_1,
            title="Blog Post #1",
            is_published=True,
            published_date=datetime.datetime(2010, 1, 5, 0, 0, 0)
        )
        BlogPost.objects.create(
            blog=blog_2,
            title="Blog Post #2",
            is_published=True,
            published_date=datetime.datetime(2010, 1, 6, 0, 0, 0)
        )
        BlogPost.objects.create(
            blog=blog_3,
            title="Blog Post #3",
            is_published=True,
            published_date=datetime.datetime(2010, 1, 7, 0, 0, 0)
        )

        # find all published blog posts before 2010-01-07
        published_posts = BlogPost.published()
        published_posts = published_posts.filter(
            published_date__lt=datetime.datetime(2010, 1, 7, 0, 0, 0))
        self.assertEqual(published_posts.count(), 2)

        blog_posts = BlogPost.objects
        blog_posts = blog_posts.filter(blog__in=[blog_1, blog_2])
        blog_posts = blog_posts.filter(blog=blog_3)
        self.assertEqual(blog_posts.count(), 0)

        BlogPost.drop_collection()
        Blog.drop_collection()

    def assertSequence(self, qs, expected):
        qs = list(qs)
        expected = list(expected)
        self.assertEqual(len(qs), len(expected))
        for i in range(len(qs)):
            self.assertEqual(qs[i], expected[i])

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

        blog_post_1 = BlogPost.objects.create(
            title="Blog Post #1",
            published_date=datetime.datetime(2010, 1, 5, 0, 0, 0)
        )
        blog_post_2 = BlogPost.objects.create(
            title="Blog Post #2",
            published_date=datetime.datetime(2010, 1, 6, 0, 0, 0)
        )
        blog_post_3 = BlogPost.objects.create(
            title="Blog Post #3",
            published_date=datetime.datetime(2010, 1, 7, 0, 0, 0)
        )

        # get the "first" BlogPost using default ordering
        # from BlogPost.meta.ordering
        expected = [blog_post_3, blog_post_2, blog_post_1]
        self.assertSequence(BlogPost.objects.all(), expected)

        # override default ordering, order BlogPosts by "published_date"
        qs = BlogPost.objects.order_by("+published_date")
        expected = [blog_post_1, blog_post_2, blog_post_3]
        self.assertSequence(qs, expected)

    def test_clear_ordering(self):
        """Ensure that the default ordering can be cleared by calling
        order_by() w/o any arguments.
        """
        class BlogPost(Document):
            title = StringField()
            published_date = DateTimeField()

            meta = {
                'ordering': ['-published_date']
            }

        BlogPost.drop_collection()

        # default ordering should be used by default
        with db_ops_tracker() as q:
            BlogPost.objects.filter(title='whatever').first()
            self.assertEqual(len(q.get_ops()), 1)
            self.assertEqual(
                q.get_ops()[0]['query']['$orderby'],
                {'published_date': -1}
            )

        # calling order_by() should clear the default ordering
        with db_ops_tracker() as q:
            BlogPost.objects.filter(title='whatever').order_by().first()
            self.assertEqual(len(q.get_ops()), 1)
            self.assertFalse('$orderby' in q.get_ops()[0]['query'])

        # calling an explicit order_by should use a specified sort
        with db_ops_tracker() as q:
            BlogPost.objects.filter(title='whatever').order_by('published_date').first()
            self.assertEqual(len(q.get_ops()), 1)
            self.assertEqual(
                q.get_ops()[0]['query']['$orderby'],
                {'published_date': 1}
            )

        # calling order_by() after an explicit sort should clear it
        with db_ops_tracker() as q:
            qs = BlogPost.objects.filter(title='whatever').order_by('published_date')
            qs.order_by().first()
            self.assertEqual(len(q.get_ops()), 1)
            self.assertFalse('$orderby' in q.get_ops()[0]['query'])

    def test_no_ordering_for_get(self):
        """ Ensure that Doc.objects.get doesn't use any ordering.
        """
        class BlogPost(Document):
            title = StringField()
            published_date = DateTimeField()

            meta = {
                'ordering': ['-published_date']
            }

        BlogPost.objects.create(
            title='whatever', published_date=datetime.datetime.utcnow())

        with db_ops_tracker() as q:
            BlogPost.objects.get(title='whatever')
            self.assertEqual(len(q.get_ops()), 1)
            self.assertFalse('$orderby' in q.get_ops()[0]['query'])

        # Ordering should be ignored for .get even if we set it explicitly
        with db_ops_tracker() as q:
            BlogPost.objects.order_by('-title').get(title='whatever')
            self.assertEqual(len(q.get_ops()), 1)
            self.assertFalse('$orderby' in q.get_ops()[0]['query'])

    def test_find_embedded(self):
        """Ensure that an embedded document is properly returned from
        different manners of querying.
        """
        class User(EmbeddedDocument):
            name = StringField()

        class BlogPost(Document):
            content = StringField()
            author = EmbeddedDocumentField(User)

        BlogPost.drop_collection()

        user = User(name='Test User')
        BlogPost.objects.create(
            author=user,
            content='Had a good coffee today...'
        )

        result = BlogPost.objects.first()
        self.assertTrue(isinstance(result.author, User))
        self.assertEqual(result.author.name, 'Test User')

        result = BlogPost.objects.get(author__name=user.name)
        self.assertTrue(isinstance(result.author, User))
        self.assertEqual(result.author.name, 'Test User')

        result = BlogPost.objects.get(author={'name': user.name})
        self.assertTrue(isinstance(result.author, User))
        self.assertEqual(result.author.name, 'Test User')

        # Fails, since the string is not a type that is able to represent the
        # author's document structure (should be dict)
        with self.assertRaises(InvalidQueryError):
            BlogPost.objects.get(author=user.name)

    def test_find_empty_embedded(self):
        """Ensure that you can save and find an empty embedded document."""
        class User(EmbeddedDocument):
            name = StringField()

        class BlogPost(Document):
            content = StringField()
            author = EmbeddedDocumentField(User)

        BlogPost.drop_collection()

        BlogPost.objects.create(content='Anonymous post...')

        result = BlogPost.objects.get(author=None)
        self.assertEqual(result.author, None)

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
        self.assertEqual("cmnts.body", sub_code)

        BlogPost.drop_collection()

    def test_delete(self):
        """Ensure that documents are properly deleted from the database.
        """
        self.Person(name="User A", age=20).save()
        self.Person(name="User B", age=30).save()
        self.Person(name="User C", age=40).save()

        self.assertEqual(self.Person.objects.count(), 3)

        self.Person.objects(age__lt=30).delete()
        self.assertEqual(self.Person.objects.count(), 2)

        self.Person.objects.delete()
        self.assertEqual(self.Person.objects.count(), 0)

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

    def test_reverse_delete_rule_cascade_on_abstract_document(self):
        """Ensure cascading deletion of referring documents from the database
        does not fail on abstract document.
        """
        class AbstractBlogPost(Document):
            meta = {'abstract': True}
            author = ReferenceField(self.Person, reverse_delete_rule=CASCADE)

        class BlogPost(AbstractBlogPost):
            content = StringField()
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

    def test_reverse_delete_rule_cascade_cycle(self):
        """Ensure reference cascading doesn't loop if reference graph isn't
        a tree
        """
        class Dummy(Document):
            reference = ReferenceField('self', reverse_delete_rule=CASCADE)

        base = Dummy().save()
        other = Dummy(reference=base).save()
        base.reference = other
        base.save()

        base.delete()

        self.assertRaises(DoesNotExist, base.reload)
        self.assertRaises(DoesNotExist, other.reload)

    def test_reverse_delete_rule_cascade_complex_cycle(self):
        """Ensure reference cascading doesn't loop if reference graph isn't
        a tree
        """
        class Category(Document):
            name = StringField()

        class Dummy(Document):
            reference = ReferenceField('self', reverse_delete_rule=CASCADE)
            cat = ReferenceField(Category, reverse_delete_rule=CASCADE)

        cat = Category(name='cat').save()
        base = Dummy(cat=cat).save()
        other = Dummy(reference=base).save()
        other2 = Dummy(reference=other).save()
        base.reference = other
        base.save()

        cat.delete()

        self.assertRaises(DoesNotExist, base.reload)
        self.assertRaises(DoesNotExist, other.reload)
        self.assertRaises(DoesNotExist, other2.reload)

    def test_reverse_delete_rule_cascade_self_referencing(self):
        """Ensure self-referencing CASCADE deletes do not result in infinite
        loop
        """
        class Category(Document):
            name = StringField()
            parent = ReferenceField('self', reverse_delete_rule=CASCADE)

        Category.drop_collection()

        num_children = 3
        base = Category(name='Root')
        base.save()

        # Create a simple parent-child tree
        for i in range(num_children):
            child_name = 'Child-%i' % i
            child = Category(name=child_name, parent=base)
            child.save()

            for i in range(num_children):
                child_child_name = 'Child-Child-%i' % i
                child_child = Category(name=child_child_name, parent=child)
                child_child.save()

        tree_size = 1 + num_children + (num_children * num_children)
        self.assertEqual(tree_size, Category.objects.count())
        self.assertEqual(num_children, Category.objects(parent=base).count())

        # The delete should effectively wipe out the Category collection
        # without resulting in infinite parent-child cascade recursion
        base.delete()
        self.assertEqual(0, Category.objects.count())

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

    def test_reverse_delete_rule_nullify_on_abstract_document(self):
        """Ensure nullification of references to deleted documents when
        reference is on an abstract document.
        """
        class AbstractBlogPost(Document):
            meta = {'abstract': True}
            author = ReferenceField(self.Person, reverse_delete_rule=NULLIFY)

        class BlogPost(AbstractBlogPost):
            content = StringField()
        BlogPost.drop_collection()

        me = self.Person(name='Test User')
        me.save()
        someoneelse = self.Person(name='Some-one Else')
        someoneelse.save()

        BlogPost(content='Watching TV', author=me).save()

        self.assertEqual(1, BlogPost.objects.count())
        self.assertEqual(me, BlogPost.objects.first().author)
        self.Person.objects(name='Test User').delete()
        self.assertEqual(1, BlogPost.objects.count())
        self.assertEqual(None, BlogPost.objects.first().author)

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

    def test_reverse_delete_rule_deny_on_abstract_document(self):
        """Ensure deletion gets denied on documents that still have references
        to them, when reference is on an abstract document.
        """
        class AbstractBlogPost(Document):
            meta = {'abstract': True}
            author = ReferenceField(self.Person, reverse_delete_rule=DENY)

        class BlogPost(AbstractBlogPost):
            content = StringField()
        BlogPost.drop_collection()

        me = self.Person(name='Test User')
        me.save()

        BlogPost(content='Watching TV', author=me).save()

        self.assertEqual(1, BlogPost.objects.count())
        self.assertRaises(OperationError, self.Person.objects.delete)

    def test_reverse_delete_rule_pull(self):
        """Ensure pulling of references to deleted documents.
        """
        class BlogPost(Document):
            content = StringField()
            authors = ListField(ReferenceField(self.Person,
                                               reverse_delete_rule=PULL))

        BlogPost.drop_collection()
        self.Person.drop_collection()

        me = self.Person(name='Test User')
        me.save()

        someoneelse = self.Person(name='Some-one Else')
        someoneelse.save()

        post = BlogPost(content='Watching TV', authors=[me, someoneelse])
        post.save()

        another = BlogPost(content='Chilling Out', authors=[someoneelse])
        another.save()

        someoneelse.delete()
        post.reload()
        another.reload()

        self.assertEqual(post.authors, [me])
        self.assertEqual(another.authors, [])

    def test_reverse_delete_rule_pull_on_abstract_documents(self):
        """Ensure pulling of references to deleted documents when reference
        is defined on an abstract document..
        """
        class AbstractBlogPost(Document):
            meta = {'abstract': True}
            authors = ListField(ReferenceField(self.Person,
                                               reverse_delete_rule=PULL))

        class BlogPost(AbstractBlogPost):
            content = StringField()

        BlogPost.drop_collection()
        self.Person.drop_collection()

        me = self.Person(name='Test User')
        me.save()

        someoneelse = self.Person(name='Some-one Else')
        someoneelse.save()

        post = BlogPost(content='Watching TV', authors=[me, someoneelse])
        post.save()

        another = BlogPost(content='Chilling Out', authors=[someoneelse])
        another.save()

        someoneelse.delete()
        post.reload()
        another.reload()

        self.assertEqual(post.authors, [me])
        self.assertEqual(another.authors, [])

    def test_delete_with_limits(self):

        class Log(Document):
            pass

        Log.drop_collection()

        for i in range(10):
            Log().save()

        Log.objects()[3:5].delete()
        self.assertEqual(8, Log.objects.count())

    def test_delete_with_limit_handles_delete_rules(self):
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
        self.Person.objects()[:1].delete()
        self.assertEqual(1, BlogPost.objects.count())

    def test_limit_with_write_concern_0(self):

        p1 = self.Person(name="User Z", age=20).save()
        del_result = p1.delete(w=0)
        self.assertEqual(None, del_result)

    def test_reference_field_find(self):
        """Ensure cascading deletion of referring documents from the database.
        """
        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(self.Person)

        BlogPost.drop_collection()
        self.Person.drop_collection()

        me = self.Person(name='Test User').save()
        BlogPost(content="test 123", author=me).save()

        self.assertEqual(1, BlogPost.objects(author=me).count())
        self.assertEqual(1, BlogPost.objects(author=me.pk).count())
        self.assertEqual(1, BlogPost.objects(author="%s" % me.pk).count())

        self.assertEqual(1, BlogPost.objects(author__in=[me]).count())
        self.assertEqual(1, BlogPost.objects(author__in=[me.pk]).count())
        self.assertEqual(
            1, BlogPost.objects(author__in=["%s" % me.pk]).count())

    def test_reference_field_find_dbref(self):
        """Ensure cascading deletion of referring documents from the database.
        """
        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(self.Person, dbref=True)

        BlogPost.drop_collection()
        self.Person.drop_collection()

        me = self.Person(name='Test User').save()
        BlogPost(content="test 123", author=me).save()

        self.assertEqual(1, BlogPost.objects(author=me).count())
        self.assertEqual(1, BlogPost.objects(author=me.pk).count())
        self.assertEqual(1, BlogPost.objects(author="%s" % me.pk).count())

        self.assertEqual(1, BlogPost.objects(author__in=[me]).count())
        self.assertEqual(1, BlogPost.objects(author__in=[me.pk]).count())
        self.assertEqual(
            1, BlogPost.objects(author__in=["%s" % me.pk]).count())

    def test_update(self):
        """Ensure that atomic updates work properly.
        """
        class BlogPost(Document):
            name = StringField()
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

        # Negative dec operator is equal to a positive inc operator
        BlogPost.objects.update_one(dec__hits=-1)
        post.reload()
        self.assertEqual(post.hits, 11)

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

    @needs_mongodb_v26
    def test_update_push_with_position(self):
        """Ensure that the 'push' update with position works properly.
        """
        class BlogPost(Document):
            slug = StringField()
            tags = ListField(StringField())

        BlogPost.drop_collection()

        post = BlogPost.objects.create(slug="test")

        BlogPost.objects.filter(id=post.id).update(push__tags="code")
        BlogPost.objects.filter(id=post.id).update(push__tags__0=["mongodb", "python"])
        post.reload()
        self.assertEqual(post.tags, ['mongodb', 'python', 'code'])

        BlogPost.objects.filter(id=post.id).update(set__tags__2="java")
        post.reload()
        self.assertEqual(post.tags, ['mongodb', 'python', 'java'])

        #test push with singular value
        BlogPost.objects.filter(id=post.id).update(push__tags__0='scala')
        post.reload()
        self.assertEqual(post.tags, ['scala', 'mongodb', 'python', 'java'])

    def test_update_push_list_of_list(self):
        """Ensure that the 'push' update operation works in the list of list
        """
        class BlogPost(Document):
            slug = StringField()
            tags = ListField()

        BlogPost.drop_collection()

        post = BlogPost(slug="test").save()

        BlogPost.objects.filter(slug="test").update(push__tags=["value1", 123])
        post.reload()
        self.assertEqual(post.tags, [["value1", 123]])

    def test_update_push_and_pull_add_to_set(self):
        """Ensure that the 'pull' update operation works correctly.
        """
        class BlogPost(Document):
            slug = StringField()
            tags = ListField(StringField())

        BlogPost.drop_collection()

        post = BlogPost(slug="test")
        post.save()

        BlogPost.objects.filter(id=post.id).update(push__tags="code")
        post.reload()
        self.assertEqual(post.tags, ["code"])

        BlogPost.objects.filter(id=post.id).update(
            push_all__tags=["mongodb", "code"])
        post.reload()
        self.assertEqual(post.tags, ["code", "mongodb", "code"])

        BlogPost.objects(slug="test").update(pull__tags="code")
        post.reload()
        self.assertEqual(post.tags, ["mongodb"])

        BlogPost.objects(slug="test").update(
            pull_all__tags=["mongodb", "code"])
        post.reload()
        self.assertEqual(post.tags, [])

        BlogPost.objects(slug="test").update(
            __raw__={"$addToSet": {"tags": {"$each": ["code", "mongodb", "code"]}}})
        post.reload()
        self.assertEqual(post.tags, ["code", "mongodb"])

    def test_add_to_set_each(self):
        class Item(Document):
            name = StringField(required=True)
            description = StringField(max_length=50)
            parents = ListField(ReferenceField('self'))

        Item.drop_collection()

        item = Item(name='test item').save()
        parent_1 = Item(name='parent 1').save()
        parent_2 = Item(name='parent 2').save()

        item.update(add_to_set__parents=[parent_1, parent_2, parent_1])
        item.reload()

        self.assertEqual([parent_1, parent_2], item.parents)

    def test_pull_nested(self):

        class Collaborator(EmbeddedDocument):
            user = StringField()

            def __unicode__(self):
                return '%s' % self.user

        class Site(Document):
            name = StringField(max_length=75, unique=True, required=True)
            collaborators = ListField(EmbeddedDocumentField(Collaborator))

        Site.drop_collection()

        c = Collaborator(user='Esteban')
        s = Site(name="test", collaborators=[c]).save()

        Site.objects(id=s.id).update_one(pull__collaborators__user='Esteban')
        self.assertEqual(Site.objects.first().collaborators, [])

        with self.assertRaises(InvalidQueryError):
            Site.objects(id=s.id).update_one(
                pull_all__collaborators__user=['Ross'])

    def test_pull_from_nested_embedded(self):

        class User(EmbeddedDocument):
            name = StringField()

            def __unicode__(self):
                return '%s' % self.name

        class Collaborator(EmbeddedDocument):
            helpful = ListField(EmbeddedDocumentField(User))
            unhelpful = ListField(EmbeddedDocumentField(User))

        class Site(Document):
            name = StringField(max_length=75, unique=True, required=True)
            collaborators = EmbeddedDocumentField(Collaborator)

        Site.drop_collection()

        c = User(name='Esteban')
        f = User(name='Frank')
        s = Site(name="test", collaborators=Collaborator(
            helpful=[c], unhelpful=[f])).save()

        Site.objects(id=s.id).update_one(pull__collaborators__helpful=c)
        self.assertEqual(Site.objects.first().collaborators['helpful'], [])

        Site.objects(id=s.id).update_one(
            pull__collaborators__unhelpful={'name': 'Frank'})
        self.assertEqual(Site.objects.first().collaborators['unhelpful'], [])

        with self.assertRaises(InvalidQueryError):
            Site.objects(id=s.id).update_one(
                pull_all__collaborators__helpful__name=['Ross'])

    def test_pull_from_nested_mapfield(self):

        class Collaborator(EmbeddedDocument):
            user = StringField()

            def __unicode__(self):
                return '%s' % self.user

        class Site(Document):
            name = StringField(max_length=75, unique=True, required=True)
            collaborators = MapField(
                ListField(EmbeddedDocumentField(Collaborator)))

        Site.drop_collection()

        c = Collaborator(user='Esteban')
        f = Collaborator(user='Frank')
        s = Site(name="test", collaborators={'helpful': [c], 'unhelpful': [f]})
        s.save()

        Site.objects(id=s.id).update_one(
            pull__collaborators__helpful__user='Esteban')
        self.assertEqual(Site.objects.first().collaborators['helpful'], [])

        Site.objects(id=s.id).update_one(
            pull__collaborators__unhelpful={'user': 'Frank'})
        self.assertEqual(Site.objects.first().collaborators['unhelpful'], [])

        with self.assertRaises(InvalidQueryError):
            Site.objects(id=s.id).update_one(
                pull_all__collaborators__helpful__user=['Ross'])

    def test_pull_in_genericembedded_field(self):

        class Foo(EmbeddedDocument):
            name = StringField()

        class Bar(Document):
            foos = ListField(GenericEmbeddedDocumentField(
                choices=[Foo, ]))

        Bar.drop_collection()

        foo = Foo(name="bar")
        bar = Bar(foos=[foo]).save()
        Bar.objects(id=bar.id).update(pull__foos=foo)
        bar.reload()
        self.assertEqual(len(bar.foos), 0)

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
        self.assertEqual(post.tags[0].name, 'python')

        BlogPost.objects(slug="test-2").update_one(pop__tags=-1)
        post.reload()
        self.assertEqual(len(post.tags), 1)

        BlogPost.drop_collection()

    def test_set_list_embedded_documents(self):

        class Author(EmbeddedDocument):
            name = StringField()

        class Message(Document):
            title = StringField()
            authors = ListField(EmbeddedDocumentField('Author'))

        Message.drop_collection()

        message = Message(title="hello", authors=[Author(name="Harry")])
        message.save()

        Message.objects(authors__name="Harry").update_one(
            set__authors__S=Author(name="Ross"))

        message = message.reload()
        self.assertEqual(message.authors[0].name, "Ross")

        Message.objects(authors__name="Ross").update_one(
            set__authors=[Author(name="Harry"),
                          Author(name="Ross"),
                          Author(name="Adam")])

        message = message.reload()
        self.assertEqual(message.authors[0].name, "Harry")
        self.assertEqual(message.authors[1].name, "Ross")
        self.assertEqual(message.authors[2].name, "Adam")

    def test_set_generic_embedded_documents(self):

        class Bar(EmbeddedDocument):
            name = StringField()

        class User(Document):
            username = StringField()
            bar = GenericEmbeddedDocumentField(choices=[Bar,])

        User.drop_collection()

        User(username='abc').save()
        User.objects(username='abc').update(
            set__bar=Bar(name='test'), upsert=True)

        user = User.objects(username='abc').first()
        self.assertEqual(user.bar.name, "test")

    def test_reload_embedded_docs_instance(self):

        class SubDoc(EmbeddedDocument):
            val = IntField()

        class Doc(Document):
            embedded = EmbeddedDocumentField(SubDoc)

        doc = Doc(embedded=SubDoc(val=0)).save()
        doc.reload()

        self.assertEqual(doc.pk, doc.embedded._instance.pk)

    def test_reload_list_embedded_docs_instance(self):

        class SubDoc(EmbeddedDocument):
            val = IntField()

        class Doc(Document):
            embedded = ListField(EmbeddedDocumentField(SubDoc))

        doc = Doc(embedded=[SubDoc(val=0)]).save()
        doc.reload()

        self.assertEqual(doc.pk, doc.embedded[0]._instance.pk)

    def test_order_by(self):
        """Ensure that QuerySets may be ordered.
        """
        self.Person(name="User B", age=40).save()
        self.Person(name="User A", age=20).save()
        self.Person(name="User C", age=30).save()

        names = [p.name for p in self.Person.objects.order_by('-age')]
        self.assertEqual(names, ['User B', 'User C', 'User A'])

        names = [p.name for p in self.Person.objects.order_by('+age')]
        self.assertEqual(names, ['User A', 'User C', 'User B'])

        names = [p.name for p in self.Person.objects.order_by('age')]
        self.assertEqual(names, ['User A', 'User C', 'User B'])

        ages = [p.age for p in self.Person.objects.order_by('-name')]
        self.assertEqual(ages, [30, 40, 20])

    def test_order_by_optional(self):
        class BlogPost(Document):
            title = StringField()
            published_date = DateTimeField(required=False)

        BlogPost.drop_collection()

        blog_post_3 = BlogPost.objects.create(
            title="Blog Post #3",
            published_date=datetime.datetime(2010, 1, 6, 0, 0, 0)
        )
        blog_post_2 = BlogPost.objects.create(
            title="Blog Post #2",
            published_date=datetime.datetime(2010, 1, 5, 0, 0, 0)
        )
        blog_post_4 = BlogPost.objects.create(
            title="Blog Post #4",
            published_date=datetime.datetime(2010, 1, 7, 0, 0, 0)
        )
        blog_post_1 = BlogPost.objects.create(
            title="Blog Post #1",
            published_date=None
        )

        expected = [blog_post_1, blog_post_2, blog_post_3, blog_post_4]
        self.assertSequence(BlogPost.objects.order_by('published_date'),
                            expected)
        self.assertSequence(BlogPost.objects.order_by('+published_date'),
                            expected)

        expected.reverse()
        self.assertSequence(BlogPost.objects.order_by('-published_date'),
                            expected)

    def test_order_by_list(self):
        class BlogPost(Document):
            title = StringField()
            published_date = DateTimeField(required=False)

        BlogPost.drop_collection()

        blog_post_1 = BlogPost.objects.create(
            title="A",
            published_date=datetime.datetime(2010, 1, 6, 0, 0, 0)
        )
        blog_post_2 = BlogPost.objects.create(
            title="B",
            published_date=datetime.datetime(2010, 1, 6, 0, 0, 0)
        )
        blog_post_3 = BlogPost.objects.create(
            title="C",
            published_date=datetime.datetime(2010, 1, 7, 0, 0, 0)
        )

        qs = BlogPost.objects.order_by('published_date', 'title')
        expected = [blog_post_1, blog_post_2, blog_post_3]
        self.assertSequence(qs, expected)

        qs = BlogPost.objects.order_by('-published_date', '-title')
        expected.reverse()
        self.assertSequence(qs, expected)

    def test_order_by_chaining(self):
        """Ensure that an order_by query chains properly and allows .only()
        """
        self.Person(name="User B", age=40).save()
        self.Person(name="User A", age=20).save()
        self.Person(name="User C", age=30).save()

        only_age = self.Person.objects.order_by('-age').only('age')

        names = [p.name for p in only_age]
        ages = [p.age for p in only_age]

        # The .only('age') clause should mean that all names are None
        self.assertEqual(names, [None, None, None])
        self.assertEqual(ages, [40, 30, 20])

        qs = self.Person.objects.all().order_by('-age')
        qs = qs.limit(10)
        ages = [p.age for p in qs]
        self.assertEqual(ages, [40, 30, 20])

        qs = self.Person.objects.all().limit(10)
        qs = qs.order_by('-age')

        ages = [p.age for p in qs]
        self.assertEqual(ages, [40, 30, 20])

        qs = self.Person.objects.all().skip(0)
        qs = qs.order_by('-age')
        ages = [p.age for p in qs]
        self.assertEqual(ages, [40, 30, 20])

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

        names = [
            a.author.name for a in Author.objects.order_by('-author__age')]
        self.assertEqual(names, ['User A', 'User B', 'User C'])

    def test_comment(self):
        """Make sure adding a comment to the query works."""
        class User(Document):
            age = IntField()

        with db_ops_tracker() as q:
            adult1 = (User.objects.filter(age__gte=18)
                .comment('looking for an adult')
                .first())

            adult2 = (User.objects.comment('looking for an adult')
                .filter(age__gte=18)
                .first())

            ops = q.get_ops()
            self.assertEqual(len(ops), 2)
            for op in ops:
                self.assertEqual(op['query']['$query'], {'age': {'$gte': 18}})
                self.assertEqual(op['query']['$comment'], 'looking for an adult')

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

        music = list(filter(lambda r: r.key == "music", results))[0]
        self.assertEqual(music.value, 2)

        film = list(filter(lambda r: r.key == "film", results))[0]
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

    def test_map_reduce_custom_output(self):
        """
        Test map/reduce custom output
        """
        register_connection('test2', 'mongoenginetest2')

        class Family(Document):
            id = IntField(
                primary_key=True)
            log = StringField()

        class Person(Document):
            id = IntField(
                primary_key=True)
            name = StringField()
            age = IntField()
            family = ReferenceField(Family)

        Family.drop_collection()
        Person.drop_collection()

        # creating first family
        f1 = Family(id=1, log="Trav 02 de Julho")
        f1.save()

        # persons of first family
        Person(id=1, family=f1, name=u"Wilson Jr", age=21).save()
        Person(id=2, family=f1, name=u"Wilson Father", age=45).save()
        Person(id=3, family=f1, name=u"Eliana Costa", age=40).save()
        Person(id=4, family=f1, name=u"Tayza Mariana", age=17).save()

        # creating second family
        f2 = Family(id=2, log="Av prof frasc brunno")
        f2.save()

        # persons of second family
        Person(id=5, family=f2, name="Isabella Luanna", age=16).save()
        Person(id=6, family=f2, name="Sandra Mara", age=36).save()
        Person(id=7, family=f2, name="Igor Gabriel", age=10).save()

        # creating third family
        f3 = Family(id=3, log="Av brazil")
        f3.save()

        # persons of thrird family
        Person(id=8, family=f3, name="Arthur WA", age=30).save()
        Person(id=9, family=f3, name="Paula Leonel", age=25).save()

        # executing join map/reduce
        map_person = """
            function () {
                emit(this.family, {
                     totalAge: this.age,
                     persons: [{
                        name: this.name,
                        age: this.age
                }]});
            }
        """

        map_family = """
            function () {
                emit(this._id, {
                   totalAge: 0,
                   persons: []
                });
            }
        """

        reduce_f = """
            function (key, values) {
                var family = {persons: [], totalAge: 0};

                values.forEach(function(value) {
                    if (value.persons) {
                        value.persons.forEach(function (person) {
                            family.persons.push(person);
                            family.totalAge += person.age;
                        });
                    }
                });

                return family;
            }
        """
        cursor = Family.objects.map_reduce(
            map_f=map_family,
            reduce_f=reduce_f,
            output={'replace': 'family_map', 'db_alias': 'test2'})

        # start a map/reduce
        cursor.next()

        results = Person.objects.map_reduce(
            map_f=map_person,
            reduce_f=reduce_f,
            output={'reduce': 'family_map', 'db_alias': 'test2'})

        results = list(results)
        collection = get_db('test2').family_map

        self.assertEqual(
            collection.find_one({'_id': 1}), {
                '_id': 1,
                'value': {
                    'persons': [
                        {'age': 21, 'name': u'Wilson Jr'},
                        {'age': 45, 'name': u'Wilson Father'},
                        {'age': 40, 'name': u'Eliana Costa'},
                        {'age': 17, 'name': u'Tayza Mariana'}],
                    'totalAge': 123}
            })

        self.assertEqual(
            collection.find_one({'_id': 2}), {
                '_id': 2,
                'value': {
                    'persons': [
                        {'age': 16, 'name': u'Isabella Luanna'},
                        {'age': 36, 'name': u'Sandra Mara'},
                        {'age': 10, 'name': u'Igor Gabriel'}],
                    'totalAge': 62}
            })

        self.assertEqual(
            collection.find_one({'_id': 3}), {
                '_id': 3,
                'value': {
                    'persons': [
                        {'age': 30, 'name': u'Arthur WA'},
                        {'age': 25, 'name': u'Paula Leonel'}],
                    'totalAge': 55}
            })

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

        now = datetime.datetime.utcnow()

        # Note: Test data taken from a custom Reddit homepage on
        # Fri, 12 Feb 2010 14:36:00 -0600. Link ordering should
        # reflect order of insertion below, but is not influenced
        # by insertion order.
        Link(title="Google Buzz auto-followed a woman's abusive ex ...",
             up_votes=1079,
             down_votes=553,
             submitted=now - datetime.timedelta(hours=4)).save()
        Link(title="We did it! Barbie is a computer engineer.",
             up_votes=481,
             down_votes=124,
             submitted=now - datetime.timedelta(hours=2)).save()
        Link(title="This Is A Mosquito Getting Killed By A Laser",
             up_votes=1446,
             down_votes=530,
             submitted=now - datetime.timedelta(hours=13)).save()
        Link(title="Arabic flashcards land physics student in jail.",
             up_votes=215,
             down_votes=105,
             submitted=now - datetime.timedelta(hours=6)).save()
        Link(title="The Burger Lab: Presenting, the Flood Burger",
             up_votes=48,
             down_votes=17,
             submitted=now - datetime.timedelta(hours=5)).save()
        Link(title="How to see polarization with the naked eye",
             up_votes=74,
             down_votes=13,
             submitted=now - datetime.timedelta(hours=10)).save()

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
        reddit_epoch = mktime(datetime.datetime(2005, 12, 8, 7, 46, 43).timetuple())
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
            f = {key: int(val) for key, val in f.items()}
            self.assertEqual(
                set(['music', 'film', 'actors', 'watch']), set(f.keys()))
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
            f = {key: int(val) for key, val in f.items()}
            self.assertEqual(set(['music', 'actors', 'watch']), set(f.keys()))
            self.assertEqual(f['music'], 2)
            self.assertEqual(f['actors'], 1)
            self.assertEqual(f['watch'], 1)

        exec_js = BlogPost.objects(hits__gt=1).item_frequencies('tags')
        map_reduce = BlogPost.objects(
            hits__gt=1).item_frequencies('tags', map_reduce=True)
        test_assertions(exec_js)
        test_assertions(map_reduce)

        # Check that normalization works
        def test_assertions(f):
            self.assertAlmostEqual(f['music'], 3.0 / 8.0)
            self.assertAlmostEqual(f['actors'], 2.0 / 8.0)
            self.assertAlmostEqual(f['watch'], 2.0 / 8.0)
            self.assertAlmostEqual(f['film'], 1.0 / 8.0)

        exec_js = BlogPost.objects.item_frequencies('tags', normalize=True)
        map_reduce = BlogPost.objects.item_frequencies(
            'tags', normalize=True, map_reduce=True)
        test_assertions(exec_js)
        test_assertions(map_reduce)

        # Check item_frequencies works for non-list fields
        def test_assertions(f):
            self.assertEqual(set([1, 2]), set(f.keys()))
            self.assertEqual(f[1], 1)
            self.assertEqual(f[2], 2)

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
            f = {key: int(val) for key, val in f.items()}
            self.assertEqual(
                set(['62-3331-1656', '62-3332-1656']), set(f.keys()))
            self.assertEqual(f['62-3331-1656'], 2)
            self.assertEqual(f['62-3332-1656'], 1)

        exec_js = Person.objects.item_frequencies('phone.number')
        map_reduce = Person.objects.item_frequencies(
            'phone.number', map_reduce=True)
        test_assertions(exec_js)
        test_assertions(map_reduce)

        # Ensure query is taken into account
        def test_assertions(f):
            f = {key: int(val) for key, val in f.items()}
            self.assertEqual(set(['62-3331-1656']), set(f.keys()))
            self.assertEqual(f['62-3331-1656'], 2)

        exec_js = Person.objects(
            phone__number='62-3331-1656').item_frequencies('phone.number')
        map_reduce = Person.objects(
            phone__number='62-3331-1656').item_frequencies('phone.number', map_reduce=True)
        test_assertions(exec_js)
        test_assertions(map_reduce)

        # Check that normalization works
        def test_assertions(f):
            self.assertEqual(f['62-3331-1656'], 2.0 / 3.0)
            self.assertEqual(f['62-3332-1656'], 1.0 / 3.0)

        exec_js = Person.objects.item_frequencies(
            'phone.number', normalize=True)
        map_reduce = Person.objects.item_frequencies(
            'phone.number', normalize=True, map_reduce=True)
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
        self.assertEqual(freq, {'CRB': 1.0, None: 1.0})
        freq = Person.objects.item_frequencies('city', normalize=True)
        self.assertEqual(freq, {'CRB': 0.5, None: 0.5})

        freq = Person.objects.item_frequencies('city', map_reduce=True)
        self.assertEqual(freq, {'CRB': 1.0, None: 1.0})
        freq = Person.objects.item_frequencies(
            'city', normalize=True, map_reduce=True)
        self.assertEqual(freq, {'CRB': 0.5, None: 0.5})

    def test_item_frequencies_with_null_embedded(self):
        class Data(EmbeddedDocument):
            name = StringField()

        class Extra(EmbeddedDocument):
            tag = StringField()

        class Person(Document):
            data = EmbeddedDocumentField(Data, required=True)
            extra = EmbeddedDocumentField(Extra)

        Person.drop_collection()

        p = Person()
        p.data = Data(name="Wilson Jr")
        p.save()

        p = Person()
        p.data = Data(name="Wesley")
        p.extra = Extra(tag="friend")
        p.save()

        ot = Person.objects.item_frequencies('extra.tag', map_reduce=False)
        self.assertEqual(ot, {None: 1.0, u'friend': 1.0})

        ot = Person.objects.item_frequencies('extra.tag', map_reduce=True)
        self.assertEqual(ot, {None: 1.0, u'friend': 1.0})

    def test_item_frequencies_with_0_values(self):
        class Test(Document):
            val = IntField()

        Test.drop_collection()
        t = Test()
        t.val = 0
        t.save()

        ot = Test.objects.item_frequencies('val', map_reduce=True)
        self.assertEqual(ot, {0: 1})
        ot = Test.objects.item_frequencies('val', map_reduce=False)
        self.assertEqual(ot, {0: 1})

    def test_item_frequencies_with_False_values(self):
        class Test(Document):
            val = BooleanField()

        Test.drop_collection()
        t = Test()
        t.val = False
        t.save()

        ot = Test.objects.item_frequencies('val', map_reduce=True)
        self.assertEqual(ot, {False: 1})
        ot = Test.objects.item_frequencies('val', map_reduce=False)
        self.assertEqual(ot, {False: 1})

    def test_item_frequencies_normalize(self):
        class Test(Document):
            val = IntField()

        Test.drop_collection()

        for i in range(50):
            Test(val=1).save()

        for i in range(20):
            Test(val=2).save()

        freqs = Test.objects.item_frequencies(
            'val', map_reduce=False, normalize=True)
        self.assertEqual(freqs, {1: 50.0 / 70, 2: 20.0 / 70})

        freqs = Test.objects.item_frequencies(
            'val', map_reduce=True, normalize=True)
        self.assertEqual(freqs, {1: 50.0 / 70, 2: 20.0 / 70})

    def test_average(self):
        """Ensure that field can be averaged correctly.
        """
        self.Person(name='person', age=0).save()
        self.assertEqual(int(self.Person.objects.average('age')), 0)

        ages = [23, 54, 12, 94, 27]
        for i, age in enumerate(ages):
            self.Person(name='test%s' % i, age=age).save()

        avg = float(sum(ages)) / (len(ages) + 1)  # take into account the 0
        self.assertAlmostEqual(int(self.Person.objects.average('age')), avg)

        self.Person(name='ageless person').save()
        self.assertEqual(int(self.Person.objects.average('age')), avg)

        # dot notation
        self.Person(
            name='person meta', person_meta=self.PersonMeta(weight=0)).save()
        self.assertAlmostEqual(
            int(self.Person.objects.average('person_meta.weight')), 0)

        for i, weight in enumerate(ages):
            self.Person(
                name='test meta%i', person_meta=self.PersonMeta(weight=weight)).save()

        self.assertAlmostEqual(
            int(self.Person.objects.average('person_meta.weight')), avg
        )

        self.Person(name='test meta none').save()
        self.assertEqual(
            int(self.Person.objects.average('person_meta.weight')), avg
        )

        # test summing over a filtered queryset
        over_50 = [a for a in ages if a >= 50]
        avg = float(sum(over_50)) / len(over_50)
        self.assertEqual(
            self.Person.objects.filter(age__gte=50).average('age'),
            avg
        )

    def test_sum(self):
        """Ensure that field can be summed over correctly.
        """
        ages = [23, 54, 12, 94, 27]
        for i, age in enumerate(ages):
            self.Person(name='test%s' % i, age=age).save()

        self.assertEqual(self.Person.objects.sum('age'), sum(ages))

        self.Person(name='ageless person').save()
        self.assertEqual(self.Person.objects.sum('age'), sum(ages))

        for i, age in enumerate(ages):
            self.Person(name='test meta%s' %
                        i, person_meta=self.PersonMeta(weight=age)).save()

        self.assertEqual(
            self.Person.objects.sum('person_meta.weight'), sum(ages)
        )

        self.Person(name='weightless person').save()
        self.assertEqual(self.Person.objects.sum('age'), sum(ages))

        # test summing over a filtered queryset
        self.assertEqual(
            self.Person.objects.filter(age__gte=50).sum('age'),
            sum([a for a in ages if a >= 50])
        )

    def test_sum_over_db_field(self):
        """Ensure that a field mapped to a db field with a different name
        can be summed over correctly.
        """
        class UserVisit(Document):
            num_visits = IntField(db_field='visits')

        UserVisit.drop_collection()

        UserVisit.objects.create(num_visits=10)
        UserVisit.objects.create(num_visits=5)

        self.assertEqual(UserVisit.objects.sum('num_visits'), 15)

    def test_average_over_db_field(self):
        """Ensure that a field mapped to a db field with a different name
        can have its average computed correctly.
        """
        class UserVisit(Document):
            num_visits = IntField(db_field='visits')

        UserVisit.drop_collection()

        UserVisit.objects.create(num_visits=20)
        UserVisit.objects.create(num_visits=10)

        self.assertEqual(UserVisit.objects.average('num_visits'), 15)

    def test_embedded_average(self):
        class Pay(EmbeddedDocument):
            value = DecimalField()

        class Doc(Document):
            name = StringField()
            pay = EmbeddedDocumentField(
                Pay)

        Doc.drop_collection()

        Doc(name='Wilson Junior', pay=Pay(value=150)).save()
        Doc(name='Isabella Luanna', pay=Pay(value=530)).save()
        Doc(name='Tayza mariana', pay=Pay(value=165)).save()
        Doc(name='Eliana Costa', pay=Pay(value=115)).save()

        self.assertEqual(Doc.objects.average('pay.value'), 240)

    def test_embedded_array_average(self):
        class Pay(EmbeddedDocument):
            values = ListField(DecimalField())

        class Doc(Document):
            name = StringField()
            pay = EmbeddedDocumentField(Pay)

        Doc.drop_collection()

        Doc(name='Wilson Junior', pay=Pay(values=[150, 100])).save()
        Doc(name='Isabella Luanna', pay=Pay(values=[530, 100])).save()
        Doc(name='Tayza mariana', pay=Pay(values=[165, 100])).save()
        Doc(name='Eliana Costa', pay=Pay(values=[115, 100])).save()

        self.assertEqual(Doc.objects.average('pay.values'), 170)

    def test_array_average(self):
        class Doc(Document):
            values = ListField(DecimalField())

        Doc.drop_collection()

        Doc(values=[150, 100]).save()
        Doc(values=[530, 100]).save()
        Doc(values=[165, 100]).save()
        Doc(values=[115, 100]).save()

        self.assertEqual(Doc.objects.average('values'), 170)

    def test_embedded_sum(self):
        class Pay(EmbeddedDocument):
            value = DecimalField()

        class Doc(Document):
            name = StringField()
            pay = EmbeddedDocumentField(Pay)

        Doc.drop_collection()

        Doc(name='Wilson Junior', pay=Pay(value=150)).save()
        Doc(name='Isabella Luanna', pay=Pay(value=530)).save()
        Doc(name='Tayza mariana', pay=Pay(value=165)).save()
        Doc(name='Eliana Costa', pay=Pay(value=115)).save()

        self.assertEqual(Doc.objects.sum('pay.value'), 960)

    def test_embedded_array_sum(self):
        class Pay(EmbeddedDocument):
            values = ListField(DecimalField())

        class Doc(Document):
            name = StringField()
            pay = EmbeddedDocumentField(Pay)

        Doc.drop_collection()

        Doc(name='Wilson Junior', pay=Pay(values=[150, 100])).save()
        Doc(name='Isabella Luanna', pay=Pay(values=[530, 100])).save()
        Doc(name='Tayza mariana', pay=Pay(values=[165, 100])).save()
        Doc(name='Eliana Costa', pay=Pay(values=[115, 100])).save()

        self.assertEqual(Doc.objects.sum('pay.values'), 1360)

    def test_array_sum(self):
        class Doc(Document):
            values = ListField(DecimalField())

        Doc.drop_collection()

        Doc(values=[150, 100]).save()
        Doc(values=[530, 100]).save()
        Doc(values=[165, 100]).save()
        Doc(values=[115, 100]).save()

        self.assertEqual(Doc.objects.sum('values'), 1360)

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

    def test_distinct_handles_references(self):
        class Foo(Document):
            bar = ReferenceField("Bar")

        class Bar(Document):
            text = StringField()

        Bar.drop_collection()
        Foo.drop_collection()

        bar = Bar(text="hi")
        bar.save()

        foo = Foo(bar=bar)
        foo.save()

        self.assertEqual(Foo.objects.distinct("bar"), [bar])

    @needs_mongodb_v26
    def test_text_indexes(self):
        class News(Document):
            title = StringField()
            content = StringField()
            is_active = BooleanField(default=True)

            meta = {'indexes': [
                {'fields': ['$title', "$content"],
                 'default_language': 'portuguese',
                 'weight': {'title': 10, 'content': 2}
                 }
            ]}

        News.drop_collection()
        info = News.objects._collection.index_information()
        self.assertTrue('title_text_content_text' in info)
        self.assertTrue('textIndexVersion' in info['title_text_content_text'])

        News(title="Neymar quebrou a vertebra",
             content="O Brasil sofre com a perda de Neymar").save()

        News(title="Brasil passa para as quartas de finais",
             content="Com o brasil nas quartas de finais teremos um "
             "jogo complicado com a alemanha").save()

        count = News.objects.search_text(
            "neymar", language="portuguese").count()

        self.assertEqual(count, 1)

        count = News.objects.search_text(
            "brasil -neymar").count()

        self.assertEqual(count, 1)

        News(title=u"As eleições no Brasil já estão em planejamento",
             content=u"A candidata dilma roussef já começa o teu planejamento",
             is_active=False).save()

        new = News.objects(is_active=False).search_text(
            "dilma", language="pt").first()

        query = News.objects(is_active=False).search_text(
            "dilma", language="pt")._query

        self.assertEqual(
            query, {'$text': {
                '$search': 'dilma', '$language': 'pt'},
                'is_active': False})

        self.assertEqual(new.is_active, False)
        self.assertTrue('dilma' in new.content)
        self.assertTrue('planejamento' in new.title)

        query = News.objects.search_text("candidata")
        self.assertEqual(query._search_text, "candidata")
        new = query.first()

        self.assertTrue(isinstance(new.get_text_score(), float))

        # count
        query = News.objects.search_text('brasil').order_by('$text_score')
        self.assertEqual(query._search_text, "brasil")

        self.assertEqual(query.count(), 3)
        self.assertEqual(query._query, {'$text': {'$search': 'brasil'}})
        cursor_args = query._cursor_args
        if not IS_PYMONGO_3:
            cursor_args_fields = cursor_args['fields']
        else:
            cursor_args_fields = cursor_args['projection']
        self.assertEqual(
            cursor_args_fields, {'_text_score': {'$meta': 'textScore'}})

        text_scores = [i.get_text_score() for i in query]
        self.assertEqual(len(text_scores), 3)

        self.assertTrue(text_scores[0] > text_scores[1])
        self.assertTrue(text_scores[1] > text_scores[2])
        max_text_score = text_scores[0]

        # get item
        item = News.objects.search_text(
            'brasil').order_by('$text_score').first()
        self.assertEqual(item.get_text_score(), max_text_score)

    @needs_mongodb_v26
    def test_distinct_handles_references_to_alias(self):
        register_connection('testdb', 'mongoenginetest2')

        class Foo(Document):
            bar = ReferenceField("Bar")
            meta = {'db_alias': 'testdb'}

        class Bar(Document):
            text = StringField()
            meta = {'db_alias': 'testdb'}

        Bar.drop_collection()
        Foo.drop_collection()

        bar = Bar(text="hi")
        bar.save()

        foo = Foo(bar=bar)
        foo.save()

        self.assertEqual(Foo.objects.distinct("bar"), [bar])

    def test_distinct_handles_db_field(self):
        """Ensure that distinct resolves field name to db_field as expected.
        """
        class Product(Document):
            product_id = IntField(db_field='pid')

        Product.drop_collection()

        Product(product_id=1).save()
        Product(product_id=2).save()
        Product(product_id=1).save()

        self.assertEqual(set(Product.objects.distinct('product_id')),
                         set([1, 2]))
        self.assertEqual(set(Product.objects.distinct('pid')),
                         set([1, 2]))

        Product.drop_collection()

    def test_distinct_ListField_EmbeddedDocumentField(self):

        class Author(EmbeddedDocument):
            name = StringField()

        class Book(Document):
            title = StringField()
            authors = ListField(EmbeddedDocumentField(Author))

        Book.drop_collection()

        mark_twain = Author(name="Mark Twain")
        john_tolkien = Author(name="John Ronald Reuel Tolkien")

        Book.objects.create(title="Tom Sawyer", authors=[mark_twain])
        Book.objects.create(title="The Lord of the Rings", authors=[john_tolkien])
        Book.objects.create(title="The Stories", authors=[mark_twain, john_tolkien])

        authors = Book.objects.distinct("authors")
        self.assertEqual(authors, [mark_twain, john_tolkien])

    def test_distinct_ListField_EmbeddedDocumentField_EmbeddedDocumentField(self):
        class Continent(EmbeddedDocument):
            continent_name = StringField()

        class Country(EmbeddedDocument):
            country_name = StringField()
            continent = EmbeddedDocumentField(Continent)

        class Author(EmbeddedDocument):
            name = StringField()
            country = EmbeddedDocumentField(Country)

        class Book(Document):
            title = StringField()
            authors = ListField(EmbeddedDocumentField(Author))

        Book.drop_collection()

        europe = Continent(continent_name='europe')
        asia = Continent(continent_name='asia')

        scotland = Country(country_name="Scotland", continent=europe)
        tibet = Country(country_name="Tibet", continent=asia)

        mark_twain = Author(name="Mark Twain", country=scotland)
        john_tolkien = Author(name="John Ronald Reuel Tolkien", country=tibet)

        Book.objects.create(title="Tom Sawyer", authors=[mark_twain])
        Book.objects.create(title="The Lord of the Rings", authors=[john_tolkien])
        Book.objects.create(title="The Stories", authors=[mark_twain, john_tolkien])

        country_list = Book.objects.distinct("authors.country")
        self.assertEqual(country_list, [scotland, tibet])

        continent_list = Book.objects.distinct("authors.country.continent")
        self.assertEqual(continent_list, [europe, asia])

    def test_distinct_ListField_ReferenceField(self):

        class Bar(Document):
            text = StringField()

        class Foo(Document):
            bar = ReferenceField('Bar')
            bar_lst = ListField(ReferenceField('Bar'))

        Bar.drop_collection()
        Foo.drop_collection()

        bar_1 = Bar(text="hi")
        bar_1.save()

        bar_2 = Bar(text="bye")
        bar_2.save()

        foo = Foo(bar=bar_1, bar_lst=[bar_1, bar_2])
        foo.save()

        self.assertEqual(Foo.objects.distinct("bar_lst"), [bar_1, bar_2])

    def test_custom_manager(self):
        """Ensure that custom QuerySetManager instances work as expected.
        """
        class BlogPost(Document):
            tags = ListField(StringField())
            deleted = BooleanField(default=False)
            date = DateTimeField(default=datetime.datetime.now)

            @queryset_manager
            def objects(cls, qryset):
                opts = {"deleted": False}
                return qryset(**opts)

            @queryset_manager
            def music_posts(doc_cls, queryset, deleted=False):
                return queryset(tags='music',
                                deleted=deleted).order_by('date')

        BlogPost.drop_collection()

        post1 = BlogPost(tags=['music', 'film']).save()
        post2 = BlogPost(tags=['music']).save()
        post3 = BlogPost(tags=['film', 'actors']).save()
        post4 = BlogPost(tags=['film', 'actors', 'music'], deleted=True).save()

        self.assertEqual([p.id for p in BlogPost.objects()],
                         [post1.id, post2.id, post3.id])
        self.assertEqual([p.id for p in BlogPost.music_posts()],
                         [post1.id, post2.id])

        self.assertEqual([p.id for p in BlogPost.music_posts(True)],
                         [post4.id])

        BlogPost.drop_collection()

    def test_custom_manager_overriding_objects_works(self):

        class Foo(Document):
            bar = StringField(default='bar')
            active = BooleanField(default=False)

            @queryset_manager
            def objects(doc_cls, queryset):
                return queryset(active=True)

            @queryset_manager
            def with_inactive(doc_cls, queryset):
                return queryset(active=False)

        Foo.drop_collection()

        Foo(active=True).save()
        Foo(active=False).save()

        self.assertEqual(1, Foo.objects.count())
        self.assertEqual(1, Foo.with_inactive.count())

        Foo.with_inactive.first().delete()
        self.assertEqual(0, Foo.with_inactive.count())
        self.assertEqual(1, Foo.objects.count())

    def test_inherit_objects(self):

        class Foo(Document):
            meta = {'allow_inheritance': True}
            active = BooleanField(default=True)

            @queryset_manager
            def objects(klass, queryset):
                return queryset(active=True)

        class Bar(Foo):
            pass

        Bar.drop_collection()
        Bar.objects.create(active=False)
        self.assertEqual(0, Bar.objects.count())

    def test_inherit_objects_override(self):

        class Foo(Document):
            meta = {'allow_inheritance': True}
            active = BooleanField(default=True)

            @queryset_manager
            def objects(klass, queryset):
                return queryset(active=True)

        class Bar(Foo):

            @queryset_manager
            def objects(klass, queryset):
                return queryset(active=False)

        Bar.drop_collection()
        Bar.objects.create(active=False)
        self.assertEqual(0, Foo.objects.count())
        self.assertEqual(1, Bar.objects.count())

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

    def test_dict_with_custom_baseclass(self):
        """Ensure DictField working with custom base clases.
        """
        class Test(Document):
            testdict = DictField()

        Test.drop_collection()

        t = Test(testdict={'f': 'Value'})
        t.save()

        self.assertEqual(
            Test.objects(testdict__f__startswith='Val').count(), 1)
        self.assertEqual(Test.objects(testdict__f='Value').count(), 1)
        Test.drop_collection()

        class Test(Document):
            testdict = DictField(basecls=StringField)

        t = Test(testdict={'f': 'Value'})
        t.save()

        self.assertEqual(Test.objects(testdict__f='Value').count(), 1)
        self.assertEqual(
            Test.objects(testdict__f__startswith='Val').count(), 1)
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

    def test_custom_querysets(self):
        """Ensure that custom QuerySet classes may be used.
        """
        class CustomQuerySet(QuerySet):

            def not_empty(self):
                return self.count() > 0

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
                return self.count() > 0

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
        self.assertEqual(Post.objects.count(), 2)
        self.assertEqual(Post.published.count(), 1)

        Post.drop_collection()

    def test_custom_querysets_inherited(self):
        """Ensure that custom QuerySet classes may be used.
        """

        class CustomQuerySet(QuerySet):

            def not_empty(self):
                return self.count() > 0

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
                return self.count() > 0

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

    def test_count_limit_and_skip(self):
        class Post(Document):
            title = StringField()

        Post.drop_collection()

        for i in range(10):
            Post(title="Post %s" % i).save()

        self.assertEqual(5, Post.objects.limit(5).skip(5).count(with_limit_and_skip=True))

        self.assertEqual(
            10, Post.objects.limit(5).skip(5).count(with_limit_and_skip=False))

    def test_count_and_none(self):
        """Test count works with None()"""

        class MyDoc(Document):
            pass

        MyDoc.drop_collection()
        for i in range(0, 10):
            MyDoc().save()

        self.assertEqual(MyDoc.objects.count(), 10)
        self.assertEqual(MyDoc.objects.none().count(), 0)

    def test_count_list_embedded(self):
        class B(EmbeddedDocument):
            c = StringField()

        class A(Document):
            b = ListField(EmbeddedDocumentField(B))

        self.assertEqual(A.objects(b=[{'c': 'c'}]).count(), 0)

    def test_call_after_limits_set(self):
        """Ensure that re-filtering after slicing works
        """
        class Post(Document):
            title = StringField()

        Post.drop_collection()

        Post(title="Post 1").save()
        Post(title="Post 2").save()

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

        for i in range(1, 101):
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

    def test_using(self):
        """Ensure that switching databases for a queryset is possible
        """
        class Number2(Document):
            n = IntField()

        Number2.drop_collection()
        with switch_db(Number2, 'test2') as Number2:
            Number2.drop_collection()

        for i in range(1, 10):
            t = Number2(n=i)
            t.switch_db('test2')
            t.save()

        self.assertEqual(len(Number2.objects.using('test2')), 9)

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

        self.assertEqual(list(Number.objects), [n2, n1])
        self.assertEqual(list(Number.objects.order_by('n')), [n1, n2])

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
        self.assertEqual([3, 2, 1], numbers)

        numbers = [n.n for n in Number.objects.order_by('+n')]
        self.assertEqual([1, 2, 3], numbers)
        Number.drop_collection()

    def test_ensure_index(self):
        """Ensure that manual creation of indexes works.
        """
        class Comment(Document):
            message = StringField()
            meta = {'allow_inheritance': True}

        Comment.ensure_index('message')

        info = Comment.objects._collection.index_information()
        info = [(value['key'],
                 value.get('unique', False),
                 value.get('sparse', False))
                for key, value in info.iteritems()]
        self.assertTrue(([('_cls', 1), ('message', 1)], False, False) in info)

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
        self.assertEqual(
            'this["fielda"] >= this["fieldb"]', query._where_clause)
        results = list(query)
        self.assertEqual(2, len(results))
        self.assertTrue(a in results)
        self.assertTrue(c in results)

        query = IntPair.objects.where('this[~fielda] == this[~fieldb]')
        results = list(query)
        self.assertEqual(1, len(results))
        self.assertTrue(a in results)

        query = IntPair.objects.where(
            'function() { return this[~fielda] >= this[~fieldb] }')
        self.assertEqual(
            'function() { return this["fielda"] >= this["fieldb"] }', query._where_clause)
        results = list(query)
        self.assertEqual(2, len(results))
        self.assertTrue(a in results)
        self.assertTrue(c in results)

        with self.assertRaises(TypeError):
            list(IntPair.objects.where(fielda__gte=3))

    def test_scalar(self):

        class Organization(Document):
            name = StringField()

        class User(Document):
            name = StringField()
            organization = ObjectIdField()

        User.drop_collection()
        Organization.drop_collection()

        whitehouse = Organization(name="White House")
        whitehouse.save()
        User(name="Bob Dole", organization=whitehouse.id).save()

        # Efficient way to get all unique organization names for a given
        # set of users (Pretend this has additional filtering.)
        user_orgs = set(User.objects.scalar('organization'))
        orgs = Organization.objects(id__in=user_orgs).scalar('name')
        self.assertEqual(list(orgs), ['White House'])

        # Efficient for generating listings, too.
        orgs = Organization.objects.scalar('name').in_bulk(list(user_orgs))
        user_map = User.objects.scalar('name', 'organization')
        user_listing = [(user, orgs[org]) for user, org in user_map]
        self.assertEqual([("Bob Dole", "White House")], user_listing)

    def test_scalar_simple(self):
        class TestDoc(Document):
            x = IntField()
            y = BooleanField()

        TestDoc.drop_collection()

        TestDoc(x=10, y=True).save()
        TestDoc(x=20, y=False).save()
        TestDoc(x=30, y=True).save()

        plist = list(TestDoc.objects.scalar('x', 'y'))

        self.assertEqual(len(plist), 3)
        self.assertEqual(plist[0], (10, True))
        self.assertEqual(plist[1], (20, False))
        self.assertEqual(plist[2], (30, True))

        class UserDoc(Document):
            name = StringField()
            age = IntField()

        UserDoc.drop_collection()

        UserDoc(name="Wilson Jr", age=19).save()
        UserDoc(name="Wilson", age=43).save()
        UserDoc(name="Eliana", age=37).save()
        UserDoc(name="Tayza", age=15).save()

        ulist = list(UserDoc.objects.scalar('name', 'age'))

        self.assertEqual(ulist, [
            (u'Wilson Jr', 19),
            (u'Wilson', 43),
            (u'Eliana', 37),
            (u'Tayza', 15)])

        ulist = list(UserDoc.objects.scalar('name').order_by('age'))

        self.assertEqual(ulist, [
            (u'Tayza'),
            (u'Wilson Jr'),
            (u'Eliana'),
            (u'Wilson')])

    def test_scalar_embedded(self):
        class Profile(EmbeddedDocument):
            name = StringField()
            age = IntField()

        class Locale(EmbeddedDocument):
            city = StringField()
            country = StringField()

        class Person(Document):
            profile = EmbeddedDocumentField(Profile)
            locale = EmbeddedDocumentField(Locale)

        Person.drop_collection()

        Person(profile=Profile(name="Wilson Jr", age=19),
               locale=Locale(city="Corumba-GO", country="Brazil")).save()

        Person(profile=Profile(name="Gabriel Falcao", age=23),
               locale=Locale(city="New York", country="USA")).save()

        Person(profile=Profile(name="Lincoln de souza", age=28),
               locale=Locale(city="Belo Horizonte", country="Brazil")).save()

        Person(profile=Profile(name="Walter cruz", age=30),
               locale=Locale(city="Brasilia", country="Brazil")).save()

        self.assertEqual(
            list(Person.objects.order_by(
                'profile__age').scalar('profile__name')),
            [u'Wilson Jr', u'Gabriel Falcao', u'Lincoln de souza', u'Walter cruz'])

        ulist = list(Person.objects.order_by('locale.city')
                     .scalar('profile__name', 'profile__age', 'locale__city'))
        self.assertEqual(ulist,
                         [(u'Lincoln de souza', 28, u'Belo Horizonte'),
                          (u'Walter cruz', 30, u'Brasilia'),
                          (u'Wilson Jr', 19, u'Corumba-GO'),
                          (u'Gabriel Falcao', 23, u'New York')])

    def test_scalar_decimal(self):
        from decimal import Decimal

        class Person(Document):
            name = StringField()
            rating = DecimalField()

        Person.drop_collection()
        Person(name="Wilson Jr", rating=Decimal('1.0')).save()

        ulist = list(Person.objects.scalar('name', 'rating'))
        self.assertEqual(ulist, [(u'Wilson Jr', Decimal('1.0'))])

    def test_scalar_reference_field(self):
        class State(Document):
            name = StringField()

        class Person(Document):
            name = StringField()
            state = ReferenceField(State)

        State.drop_collection()
        Person.drop_collection()

        s1 = State(name="Goias")
        s1.save()

        Person(name="Wilson JR", state=s1).save()

        plist = list(Person.objects.scalar('name', 'state'))
        self.assertEqual(plist, [(u'Wilson JR', s1)])

    def test_scalar_generic_reference_field(self):
        class State(Document):
            name = StringField()

        class Person(Document):
            name = StringField()
            state = GenericReferenceField()

        State.drop_collection()
        Person.drop_collection()

        s1 = State(name="Goias")
        s1.save()

        Person(name="Wilson JR", state=s1).save()

        plist = list(Person.objects.scalar('name', 'state'))
        self.assertEqual(plist, [(u'Wilson JR', s1)])

    def test_generic_reference_field_with_only_and_as_pymongo(self):
        class TestPerson(Document):
            name = StringField()

        class TestActivity(Document):
            name = StringField()
            owner = GenericReferenceField()

        TestPerson.drop_collection()
        TestActivity.drop_collection()

        person = TestPerson(name='owner')
        person.save()

        a1 = TestActivity(name='a1', owner=person)
        a1.save()

        activity = TestActivity.objects(owner=person).scalar('id', 'owner').no_dereference().first()
        self.assertEqual(activity[0], a1.pk)
        self.assertEqual(activity[1]['_ref'], DBRef('test_person', person.pk))

        activity = TestActivity.objects(owner=person).only('id', 'owner')[0]
        self.assertEqual(activity.pk, a1.pk)
        self.assertEqual(activity.owner, person)

        activity = TestActivity.objects(owner=person).only('id', 'owner').as_pymongo().first()
        self.assertEqual(activity['_id'], a1.pk)
        self.assertTrue(activity['owner']['_ref'], DBRef('test_person', person.pk))

    def test_scalar_db_field(self):

        class TestDoc(Document):
            x = IntField()
            y = BooleanField()

        TestDoc.drop_collection()

        TestDoc(x=10, y=True).save()
        TestDoc(x=20, y=False).save()
        TestDoc(x=30, y=True).save()

        plist = list(TestDoc.objects.scalar('x', 'y'))
        self.assertEqual(len(plist), 3)
        self.assertEqual(plist[0], (10, True))
        self.assertEqual(plist[1], (20, False))
        self.assertEqual(plist[2], (30, True))

    def test_scalar_primary_key(self):

        class SettingValue(Document):
            key = StringField(primary_key=True)
            value = StringField()

        SettingValue.drop_collection()
        s = SettingValue(key="test", value="test value")
        s.save()

        val = SettingValue.objects.scalar('key', 'value')
        self.assertEqual(list(val), [('test', 'test value')])

    def test_scalar_cursor_behaviour(self):
        """Ensure that a query returns a valid set of results.
        """
        person1 = self.Person(name="User A", age=20)
        person1.save()
        person2 = self.Person(name="User B", age=30)
        person2.save()

        # Find all people in the collection
        people = self.Person.objects.scalar('name')
        self.assertEqual(people.count(), 2)
        results = list(people)
        self.assertEqual(results[0], "User A")
        self.assertEqual(results[1], "User B")

        # Use a query to filter the people found to just person1
        people = self.Person.objects(age=20).scalar('name')
        self.assertEqual(people.count(), 1)
        person = people.next()
        self.assertEqual(person, "User A")

        # Test limit
        people = list(self.Person.objects.limit(1).scalar('name'))
        self.assertEqual(len(people), 1)
        self.assertEqual(people[0], 'User A')

        # Test skip
        people = list(self.Person.objects.skip(1).scalar('name'))
        self.assertEqual(len(people), 1)
        self.assertEqual(people[0], 'User B')

        person3 = self.Person(name="User C", age=40)
        person3.save()

        # Test slice limit
        people = list(self.Person.objects[:2].scalar('name'))
        self.assertEqual(len(people), 2)
        self.assertEqual(people[0], 'User A')
        self.assertEqual(people[1], 'User B')

        # Test slice skip
        people = list(self.Person.objects[1:].scalar('name'))
        self.assertEqual(len(people), 2)
        self.assertEqual(people[0], 'User B')
        self.assertEqual(people[1], 'User C')

        # Test slice limit and skip
        people = list(self.Person.objects[1:2].scalar('name'))
        self.assertEqual(len(people), 1)
        self.assertEqual(people[0], 'User B')

        people = list(self.Person.objects[1:1].scalar('name'))
        self.assertEqual(len(people), 0)

        # Test slice out of range
        people = list(self.Person.objects.scalar('name')[80000:80001])
        self.assertEqual(len(people), 0)

        # Test larger slice __repr__
        self.Person.objects.delete()
        for i in range(55):
            self.Person(name='A%s' % i, age=i).save()

        self.assertEqual(self.Person.objects.scalar('name').count(), 55)
        self.assertEqual(
            "A0", "%s" % self.Person.objects.order_by('name').scalar('name').first())
        self.assertEqual(
            "A0", "%s" % self.Person.objects.scalar('name').order_by('name')[0])
        if six.PY3:
            self.assertEqual("['A1', 'A2']", "%s" % self.Person.objects.order_by(
                'age').scalar('name')[1:3])
            self.assertEqual("['A51', 'A52']", "%s" % self.Person.objects.order_by(
                'age').scalar('name')[51:53])
        else:
            self.assertEqual("[u'A1', u'A2']", "%s" % self.Person.objects.order_by(
                'age').scalar('name')[1:3])
            self.assertEqual("[u'A51', u'A52']", "%s" % self.Person.objects.order_by(
                'age').scalar('name')[51:53])

        # with_id and in_bulk
        person = self.Person.objects.order_by('name').first()
        self.assertEqual("A0", "%s" %
                         self.Person.objects.scalar('name').with_id(person.id))

        pks = self.Person.objects.order_by('age').scalar('pk')[1:3]
        names = self.Person.objects.scalar('name').in_bulk(list(pks)).values()
        if six.PY3:
            expected = "['A1', 'A2']"
        else:
            expected = "[u'A1', u'A2']"
        self.assertEqual(expected, "%s" % sorted(names))

    def test_elem_match(self):
        class Foo(EmbeddedDocument):
            shape = StringField()
            color = StringField()
            thick = BooleanField()
            meta = {'allow_inheritance': False}

        class Bar(Document):
            foo = ListField(EmbeddedDocumentField(Foo))
            meta = {'allow_inheritance': False}

        Bar.drop_collection()

        b1 = Bar(foo=[Foo(shape="square", color="purple", thick=False),
                      Foo(shape="circle", color="red", thick=True)])
        b1.save()

        b2 = Bar(foo=[Foo(shape="square", color="red", thick=True),
                      Foo(shape="circle", color="purple", thick=False)])
        b2.save()

        b3 = Bar(foo=[Foo(shape="square", thick=True),
                      Foo(shape="circle", color="purple", thick=False)])
        b3.save()

        ak = list(
            Bar.objects(foo__match={'shape': "square", "color": "purple"}))
        self.assertEqual([b1], ak)

        ak = list(
            Bar.objects(foo__elemMatch={'shape': "square", "color": "purple"}))
        self.assertEqual([b1], ak)

        ak = list(Bar.objects(foo__match=Foo(shape="square", color="purple")))
        self.assertEqual([b1], ak)

        ak = list(
            Bar.objects(foo__elemMatch={'shape': "square", "color__exists": True}))
        self.assertEqual([b1, b2], ak)

        ak = list(
            Bar.objects(foo__match={'shape': "square", "color__exists": True}))
        self.assertEqual([b1, b2], ak)

        ak = list(
            Bar.objects(foo__elemMatch={'shape': "square", "color__exists": False}))
        self.assertEqual([b3], ak)

        ak = list(
            Bar.objects(foo__match={'shape': "square", "color__exists": False}))
        self.assertEqual([b3], ak)

    def test_upsert_includes_cls(self):
        """Upserts should include _cls information for inheritable classes
        """

        class Test(Document):
            test = StringField()

        Test.drop_collection()
        Test.objects(test='foo').update_one(upsert=True, set__test='foo')
        self.assertFalse('_cls' in Test._collection.find_one())

        class Test(Document):
            meta = {'allow_inheritance': True}
            test = StringField()

        Test.drop_collection()

        Test.objects(test='foo').update_one(upsert=True, set__test='foo')
        self.assertTrue('_cls' in Test._collection.find_one())

    def test_update_upsert_looks_like_a_digit(self):
        class MyDoc(DynamicDocument):
            pass
        MyDoc.drop_collection()
        self.assertEqual(1, MyDoc.objects.update_one(upsert=True, inc__47=1))
        self.assertEqual(MyDoc.objects.get()['47'], 1)

    def test_dictfield_key_looks_like_a_digit(self):
        """Only should work with DictField even if they have numeric keys."""

        class MyDoc(Document):
            test = DictField()

        MyDoc.drop_collection()
        doc = MyDoc(test={'47': 1})
        doc.save()
        self.assertEqual(MyDoc.objects.only('test__47').get().test['47'], 1)

    def test_read_preference(self):
        class Bar(Document):
            txt = StringField()

            meta = {
                'indexes': ['txt']
            }

        Bar.drop_collection()
        bars = list(Bar.objects(read_preference=ReadPreference.PRIMARY))
        self.assertEqual([], bars)

        if not IS_PYMONGO_3:
            error_class = ConfigurationError
        else:
            error_class = TypeError
        self.assertRaises(error_class, Bar.objects, read_preference='Primary')

        # read_preference as a kwarg
        bars = Bar.objects(read_preference=ReadPreference.SECONDARY_PREFERRED)
        self.assertEqual(bars._read_preference,
                         ReadPreference.SECONDARY_PREFERRED)
        self.assertEqual(bars._cursor._Cursor__read_preference,
                         ReadPreference.SECONDARY_PREFERRED)

        # read_preference as a query set method
        bars = Bar.objects.read_preference(ReadPreference.SECONDARY_PREFERRED)
        self.assertEqual(bars._read_preference,
                         ReadPreference.SECONDARY_PREFERRED)
        self.assertEqual(bars._cursor._Cursor__read_preference,
                         ReadPreference.SECONDARY_PREFERRED)

        # read_preference after skip
        bars = Bar.objects.skip(1) \
            .read_preference(ReadPreference.SECONDARY_PREFERRED)
        self.assertEqual(bars._read_preference,
                         ReadPreference.SECONDARY_PREFERRED)
        self.assertEqual(bars._cursor._Cursor__read_preference,
                         ReadPreference.SECONDARY_PREFERRED)

        # read_preference after limit
        bars = Bar.objects.limit(1) \
            .read_preference(ReadPreference.SECONDARY_PREFERRED)
        self.assertEqual(bars._read_preference,
                         ReadPreference.SECONDARY_PREFERRED)
        self.assertEqual(bars._cursor._Cursor__read_preference,
                         ReadPreference.SECONDARY_PREFERRED)

        # read_preference after order_by
        bars = Bar.objects.order_by('txt') \
            .read_preference(ReadPreference.SECONDARY_PREFERRED)
        self.assertEqual(bars._read_preference,
                         ReadPreference.SECONDARY_PREFERRED)
        self.assertEqual(bars._cursor._Cursor__read_preference,
                         ReadPreference.SECONDARY_PREFERRED)

        # read_preference after hint
        bars = Bar.objects.hint([('txt', 1)]) \
            .read_preference(ReadPreference.SECONDARY_PREFERRED)
        self.assertEqual(bars._read_preference,
                         ReadPreference.SECONDARY_PREFERRED)
        self.assertEqual(bars._cursor._Cursor__read_preference,
                         ReadPreference.SECONDARY_PREFERRED)

    @needs_mongodb_v26
    def test_read_preference_aggregation_framework(self):
        class Bar(Document):
            txt = StringField()

            meta = {
                'indexes': ['txt']
            }
        # Aggregates with read_preference
        bars = Bar.objects \
                    .read_preference(ReadPreference.SECONDARY_PREFERRED) \
                    .aggregate()
        if IS_PYMONGO_3:
            self.assertEqual(bars._CommandCursor__collection.read_preference,
                             ReadPreference.SECONDARY_PREFERRED)
        else:
            self.assertNotEqual(bars._CommandCursor__collection.read_preference,
                             ReadPreference.SECONDARY_PREFERRED)


    def test_json_simple(self):

        class Embedded(EmbeddedDocument):
            string = StringField()

        class Doc(Document):
            string = StringField()
            embedded_field = EmbeddedDocumentField(Embedded)

        Doc.drop_collection()
        Doc(string="Hi", embedded_field=Embedded(string="Hi")).save()
        Doc(string="Bye", embedded_field=Embedded(string="Bye")).save()

        Doc().save()
        json_data = Doc.objects.to_json(sort_keys=True, separators=(',', ':'))
        doc_objects = list(Doc.objects)

        self.assertEqual(doc_objects, Doc.objects.from_json(json_data))

    def test_json_complex(self):
        if pymongo.version_tuple[0] <= 2 and pymongo.version_tuple[1] <= 3:
            raise SkipTest("Need pymongo 2.4 as has a fix for DBRefs")

        class EmbeddedDoc(EmbeddedDocument):
            pass

        class Simple(Document):
            pass

        class Doc(Document):
            string_field = StringField(default='1')
            int_field = IntField(default=1)
            float_field = FloatField(default=1.1)
            boolean_field = BooleanField(default=True)
            datetime_field = DateTimeField(default=datetime.datetime.now)
            embedded_document_field = EmbeddedDocumentField(
                EmbeddedDoc, default=lambda: EmbeddedDoc())
            list_field = ListField(default=lambda: [1, 2, 3])
            dict_field = DictField(default=lambda: {"hello": "world"})
            objectid_field = ObjectIdField(default=ObjectId)
            reference_field = ReferenceField(
                Simple, default=lambda: Simple().save())
            map_field = MapField(IntField(), default=lambda: {"simple": 1})
            decimal_field = DecimalField(default=1.0)
            complex_datetime_field = ComplexDateTimeField(default=datetime.datetime.now)
            url_field = URLField(default="http://mongoengine.org")
            dynamic_field = DynamicField(default=1)
            generic_reference_field = GenericReferenceField(
                default=lambda: Simple().save())
            sorted_list_field = SortedListField(IntField(),
                                                default=lambda: [1, 2, 3])
            email_field = EmailField(default="ross@example.com")
            geo_point_field = GeoPointField(default=lambda: [1, 2])
            sequence_field = SequenceField()
            uuid_field = UUIDField(default=uuid.uuid4)
            generic_embedded_document_field = GenericEmbeddedDocumentField(
                default=lambda: EmbeddedDoc())

        Simple.drop_collection()
        Doc.drop_collection()

        Doc().save()
        json_data = Doc.objects.to_json()
        doc_objects = list(Doc.objects)

        self.assertEqual(doc_objects, Doc.objects.from_json(json_data))

    def test_as_pymongo(self):
        from decimal import Decimal

        class LastLogin(EmbeddedDocument):
            location = StringField()
            ip = StringField()

        class User(Document):
            id = ObjectIdField('_id')
            name = StringField()
            age = IntField()
            price = DecimalField()
            last_login = EmbeddedDocumentField(LastLogin)

        User.drop_collection()

        User.objects.create(name="Bob Dole", age=89, price=Decimal('1.11'))
        User.objects.create(
            name="Barack Obama",
            age=51,
            price=Decimal('2.22'),
            last_login=LastLogin(
                location='White House',
                ip='104.107.108.116'
            )
        )

        results = User.objects.as_pymongo()
        self.assertEqual(
            set(results[0].keys()),
            set(['_id', 'name', 'age', 'price'])
        )
        self.assertEqual(
            set(results[1].keys()),
            set(['_id', 'name', 'age', 'price', 'last_login'])
        )

        results = User.objects.only('id', 'name').as_pymongo()
        self.assertEqual(set(results[0].keys()), set(['_id', 'name']))

        users = User.objects.only('name', 'price').as_pymongo()
        results = list(users)
        self.assertTrue(isinstance(results[0], dict))
        self.assertTrue(isinstance(results[1], dict))
        self.assertEqual(results[0]['name'], 'Bob Dole')
        self.assertEqual(results[0]['price'], 1.11)
        self.assertEqual(results[1]['name'], 'Barack Obama')
        self.assertEqual(results[1]['price'], 2.22)

        users = User.objects.only('name', 'last_login').as_pymongo()
        results = list(users)
        self.assertTrue(isinstance(results[0], dict))
        self.assertTrue(isinstance(results[1], dict))
        self.assertEqual(results[0], {
            'name': 'Bob Dole'
        })
        self.assertEqual(results[1], {
            'name': 'Barack Obama',
            'last_login': {
                'location': 'White House',
                'ip': '104.107.108.116'
            }
        })

    def test_as_pymongo_json_limit_fields(self):

        class User(Document):
            email = EmailField(unique=True, required=True)
            password_hash = StringField(
                db_field='password_hash', required=True)
            password_salt = StringField(
                db_field='password_salt', required=True)

        User.drop_collection()
        User(email="ross@example.com", password_salt="SomeSalt",
             password_hash="SomeHash").save()

        serialized_user = User.objects.exclude(
            'password_salt', 'password_hash').as_pymongo()[0]
        self.assertEqual(set(['_id', 'email']), set(serialized_user.keys()))

        serialized_user = User.objects.exclude(
            'id', 'password_salt', 'password_hash').to_json()
        self.assertEqual('[{"email": "ross@example.com"}]', serialized_user)

        serialized_user = User.objects.exclude(
            'password_salt').only('email').as_pymongo()[0]
        self.assertEqual(set(['email']), set(serialized_user.keys()))

        serialized_user = User.objects.exclude(
            'password_salt').only('email').to_json()
        self.assertEqual('[{"email": "ross@example.com"}]', serialized_user)

    def test_no_dereference(self):

        class Organization(Document):
            name = StringField()

        class User(Document):
            name = StringField()
            organization = ReferenceField(Organization)

        User.drop_collection()
        Organization.drop_collection()

        whitehouse = Organization(name="White House").save()
        User(name="Bob Dole", organization=whitehouse).save()

        qs = User.objects()
        self.assertTrue(isinstance(qs.first().organization, Organization))
        self.assertFalse(isinstance(qs.no_dereference().first().organization,
                                    Organization))
        self.assertFalse(isinstance(qs.no_dereference().get().organization,
                                    Organization))
        self.assertTrue(isinstance(qs.first().organization, Organization))

    def test_no_dereference_embedded_doc(self):

        class User(Document):
            name = StringField()

        class Member(EmbeddedDocument):
            name = StringField()
            user = ReferenceField(User)

        class Organization(Document):
            name = StringField()
            members = ListField(EmbeddedDocumentField(Member))
            ceo = ReferenceField(User)
            member = EmbeddedDocumentField(Member)
            admin = ListField(ReferenceField(User))

        Organization.drop_collection()
        User.drop_collection()

        user = User(name="Flash")
        user.save()

        member = Member(name="Flash", user=user)

        company = Organization(name="Mongo Inc", ceo=user, member=member)
        company.admin.append(user)
        company.members.append(member)
        company.save()

        result = Organization.objects().no_dereference().first()

        self.assertTrue(isinstance(result.admin[0], (DBRef, ObjectId)))
        self.assertTrue(isinstance(result.member.user, (DBRef, ObjectId)))
        self.assertTrue(isinstance(result.members[0].user, (DBRef, ObjectId)))

    def test_cached_queryset(self):
        class Person(Document):
            name = StringField()

        Person.drop_collection()
        for i in range(100):
            Person(name="No: %s" % i).save()

        with query_counter() as q:
            self.assertEqual(q, 0)
            people = Person.objects

            [x for x in people]
            self.assertEqual(100, len(people._result_cache))

            import platform

            if platform.python_implementation() != "PyPy":
                # PyPy evaluates __len__ when iterating with list comprehensions while CPython does not.
                # This may be a bug in PyPy (PyPy/#1802) but it does not affect
                # the behavior of MongoEngine.
                self.assertEqual(None, people._len)
            self.assertEqual(q, 1)

            list(people)
            self.assertEqual(100, people._len)  # Caused by list calling len
            self.assertEqual(q, 1)

            people.count(with_limit_and_skip=True)  # count is cached
            self.assertEqual(q, 1)

    def test_no_cached_queryset(self):
        class Person(Document):
            name = StringField()

        Person.drop_collection()
        for i in range(100):
            Person(name="No: %s" % i).save()

        with query_counter() as q:
            self.assertEqual(q, 0)
            people = Person.objects.no_cache()

            [x for x in people]
            self.assertEqual(q, 1)

            list(people)
            self.assertEqual(q, 2)

            people.count()
            self.assertEqual(q, 3)

    def test_cache_not_cloned(self):

        class User(Document):
            name = StringField()

            def __unicode__(self):
                return self.name

        User.drop_collection()

        User(name="Alice").save()
        User(name="Bob").save()

        users = User.objects.all().order_by('name')
        self.assertEqual("%s" % users, "[<User: Alice>, <User: Bob>]")
        self.assertEqual(2, len(users._result_cache))

        users = users.filter(name="Bob")
        self.assertEqual("%s" % users, "[<User: Bob>]")
        self.assertEqual(1, len(users._result_cache))

    def test_no_cache(self):
        """Ensure you can add meta data to file"""
        class Noddy(Document):
            fields = DictField()

        Noddy.drop_collection()
        for i in range(100):
            noddy = Noddy()
            for j in range(20):
                noddy.fields["key" + str(j)] = "value " + str(j)
            noddy.save()

        docs = Noddy.objects.no_cache()

        counter = len([1 for i in docs])
        self.assertEqual(counter, 100)

        self.assertEqual(len(list(docs)), 100)

        # Can't directly get a length of a no-cache queryset.
        with self.assertRaises(TypeError):
            len(docs)

        # Another iteration over the queryset should result in another db op.
        with query_counter() as q:
            list(docs)
            self.assertEqual(q, 1)

        # ... and another one to double-check.
        with query_counter() as q:
            list(docs)
            self.assertEqual(q, 1)

    def test_nested_queryset_iterator(self):
        # Try iterating the same queryset twice, nested.
        names = ['Alice', 'Bob', 'Chuck', 'David', 'Eric', 'Francis', 'George']

        class User(Document):
            name = StringField()

            def __unicode__(self):
                return self.name

        User.drop_collection()

        for name in names:
            User(name=name).save()

        users = User.objects.all().order_by('name')
        outer_count = 0
        inner_count = 0
        inner_total_count = 0

        with query_counter() as q:
            self.assertEqual(q, 0)

            self.assertEqual(users.count(with_limit_and_skip=True), 7)

            for i, outer_user in enumerate(users):
                self.assertEqual(outer_user.name, names[i])
                outer_count += 1
                inner_count = 0

                # Calling len might disrupt the inner loop if there are bugs
                self.assertEqual(users.count(with_limit_and_skip=True), 7)

                for j, inner_user in enumerate(users):
                    self.assertEqual(inner_user.name, names[j])
                    inner_count += 1
                    inner_total_count += 1

                # inner loop should always be executed seven times
                self.assertEqual(inner_count, 7)

            # outer loop should be executed seven times total
            self.assertEqual(outer_count, 7)
            # inner loop should be executed fourtynine times total
            self.assertEqual(inner_total_count, 7 * 7)

            self.assertEqual(q, 2)

    def test_no_sub_classes(self):
        class A(Document):
            x = IntField()
            y = IntField()

            meta = {'allow_inheritance': True}

        class B(A):
            z = IntField()

        class C(B):
            zz = IntField()

        A.drop_collection()

        A(x=10, y=20).save()
        A(x=15, y=30).save()
        B(x=20, y=40).save()
        B(x=30, y=50).save()
        C(x=40, y=60).save()

        self.assertEqual(A.objects.no_sub_classes().count(), 2)
        self.assertEqual(A.objects.count(), 5)

        self.assertEqual(B.objects.no_sub_classes().count(), 2)
        self.assertEqual(B.objects.count(), 3)

        self.assertEqual(C.objects.no_sub_classes().count(), 1)
        self.assertEqual(C.objects.count(), 1)

        for obj in A.objects.no_sub_classes():
            self.assertEqual(obj.__class__, A)

        for obj in B.objects.no_sub_classes():
            self.assertEqual(obj.__class__, B)

        for obj in C.objects.no_sub_classes():
            self.assertEqual(obj.__class__, C)

    def test_query_generic_embedded_document(self):
        """Ensure that querying sub field on generic_embedded_field works
        """
        class A(EmbeddedDocument):
            a_name = StringField()

        class B(EmbeddedDocument):
            b_name = StringField()

        class Doc(Document):
            document = GenericEmbeddedDocumentField(choices=(A, B))

        Doc.drop_collection()
        Doc(document=A(a_name='A doc')).save()
        Doc(document=B(b_name='B doc')).save()

        # Using raw in filter working fine
        self.assertEqual(Doc.objects(
            __raw__={'document.a_name': 'A doc'}).count(), 1)
        self.assertEqual(Doc.objects(
            __raw__={'document.b_name': 'B doc'}).count(), 1)
        self.assertEqual(Doc.objects(document__a_name='A doc').count(), 1)
        self.assertEqual(Doc.objects(document__b_name='B doc').count(), 1)

    def test_query_reference_to_custom_pk_doc(self):

        class A(Document):
            id = StringField(unique=True, primary_key=True)

        class B(Document):
            a = ReferenceField(A)

        A.drop_collection()
        B.drop_collection()

        a = A.objects.create(id='custom_id')
        B.objects.create(a=a)

        self.assertEqual(B.objects.count(), 1)
        self.assertEqual(B.objects.get(a=a).a, a)
        self.assertEqual(B.objects.get(a=a.id).a, a)

    def test_cls_query_in_subclassed_docs(self):

        class Animal(Document):
            name = StringField()

            meta = {
                'allow_inheritance': True
            }

        class Dog(Animal):
            pass

        class Cat(Animal):
            pass

        self.assertEqual(Animal.objects(name='Charlie')._query, {
            'name': 'Charlie',
            '_cls': {'$in': ('Animal', 'Animal.Dog', 'Animal.Cat')}
        })
        self.assertEqual(Dog.objects(name='Charlie')._query, {
            'name': 'Charlie',
            '_cls': 'Animal.Dog'
        })
        self.assertEqual(Cat.objects(name='Charlie')._query, {
            'name': 'Charlie',
            '_cls': 'Animal.Cat'
        })

    def test_can_have_field_same_name_as_query_operator(self):

        class Size(Document):
            name = StringField()

        class Example(Document):
            size = ReferenceField(Size)

        Size.drop_collection()
        Example.drop_collection()

        instance_size = Size(name="Large").save()
        Example(size=instance_size).save()

        self.assertEqual(Example.objects(size=instance_size).count(), 1)
        self.assertEqual(Example.objects(size__in=[instance_size]).count(), 1)

    def test_cursor_in_an_if_stmt(self):

        class Test(Document):
            test_field = StringField()

        Test.drop_collection()
        queryset = Test.objects

        if queryset:
            raise AssertionError('Empty cursor returns True')

        test = Test()
        test.test_field = 'test'
        test.save()

        queryset = Test.objects
        if not test:
            raise AssertionError('Cursor has data and returned False')

        queryset.next()
        if not queryset:
            raise AssertionError('Cursor has data and it must returns True,'
                                 ' even in the last item.')

    def test_bool_performance(self):

        class Person(Document):
            name = StringField()

        Person.drop_collection()
        for i in range(100):
            Person(name="No: %s" % i).save()

        with query_counter() as q:
            if Person.objects:
                pass

            self.assertEqual(q, 1)
            op = q.db.system.profile.find({"ns":
                                           {"$ne": "%s.system.indexes" % q.db.name}})[0]

            self.assertEqual(op['nreturned'], 1)

    def test_bool_with_ordering(self):

        class Person(Document):
            name = StringField()

        Person.drop_collection()
        Person(name="Test").save()

        qs = Person.objects.order_by('name')

        with query_counter() as q:

            if qs:
                pass

            op = q.db.system.profile.find({"ns":
                                           {"$ne": "%s.system.indexes" % q.db.name}})[0]

            self.assertFalse('$orderby' in op['query'],
                             'BaseQuerySet cannot use orderby in if stmt')

        with query_counter() as p:

            for x in qs:
                pass

            op = p.db.system.profile.find({"ns":
                                           {"$ne": "%s.system.indexes" % q.db.name}})[0]

            self.assertTrue('$orderby' in op['query'],
                            'BaseQuerySet cannot remove orderby in for loop')

    def test_bool_with_ordering_from_meta_dict(self):

        class Person(Document):
            name = StringField()
            meta = {
                'ordering': ['name']
            }

        Person.drop_collection()

        Person(name="B").save()
        Person(name="C").save()
        Person(name="A").save()

        with query_counter() as q:

            if Person.objects:
                pass

            op = q.db.system.profile.find({"ns":
                                           {"$ne": "%s.system.indexes" % q.db.name}})[0]

            self.assertFalse('$orderby' in op['query'],
                             'BaseQuerySet must remove orderby from meta in boolen test')

            self.assertEqual(Person.objects.first().name, 'A')
            self.assertTrue(Person.objects._has_data(),
                            'Cursor has data and returned False')

    @needs_mongodb_v26
    def test_queryset_aggregation_framework(self):
        class Person(Document):
            name = StringField()
            age = IntField()

        Person.drop_collection()

        p1 = Person(name="Isabella Luanna", age=16)
        p1.save()

        p2 = Person(name="Wilson Junior", age=21)
        p2.save()

        p3 = Person(name="Sandra Mara", age=37)
        p3.save()

        data = Person.objects(age__lte=22).aggregate(
            {'$project': {'name': {'$toUpper': '$name'}}}
        )

        self.assertEqual(list(data), [
            {'_id': p1.pk, 'name': "ISABELLA LUANNA"},
            {'_id': p2.pk, 'name': "WILSON JUNIOR"}
        ])

        data = Person.objects(age__lte=22).order_by('-name').aggregate(
            {'$project': {'name': {'$toUpper': '$name'}}}
        )

        self.assertEqual(list(data), [
            {'_id': p2.pk, 'name': "WILSON JUNIOR"},
            {'_id': p1.pk, 'name': "ISABELLA LUANNA"}
        ])

        data = Person.objects(age__gte=17, age__lte=40).order_by('-age').aggregate({
            '$group': {
                '_id': None,
                'total': {'$sum': 1},
                'avg': {'$avg': '$age'}
            }
        })
        self.assertEqual(list(data), [
            {'_id': None, 'avg': 29, 'total': 2}
        ])

    def test_delete_count(self):
        [self.Person(name="User {0}".format(i), age=i * 10).save() for i in range(1, 4)]
        self.assertEqual(self.Person.objects().delete(), 3)  # test ordinary QuerySey delete count

        [self.Person(name="User {0}".format(i), age=i * 10).save() for i in range(1, 4)]

        self.assertEqual(self.Person.objects().skip(1).delete(), 2)  # test Document delete with existing documents

        self.Person.objects().delete()
        self.assertEqual(self.Person.objects().skip(1).delete(), 0)  # test Document delete without existing documents

    def test_max_time_ms(self):
        # 778: max_time_ms can get only int or None as input
        self.assertRaises(TypeError,
                          self.Person.objects(name="name").max_time_ms,
                          'not a number')

    def test_subclass_field_query(self):
        class Animal(Document):
            is_mamal = BooleanField()
            meta = {'allow_inheritance': True}

        class Cat(Animal):
            whiskers_length = FloatField()

        class ScottishCat(Cat):
            folded_ears = BooleanField()

        Animal.drop_collection()

        Animal(is_mamal=False).save()
        Cat(is_mamal=True, whiskers_length=5.1).save()
        ScottishCat(is_mamal=True, folded_ears=True).save()
        self.assertEquals(Animal.objects(folded_ears=True).count(), 1)
        self.assertEquals(Animal.objects(whiskers_length=5.1).count(), 1)

    def test_loop_over_invalid_id_does_not_crash(self):
        class Person(Document):
            name = StringField()

        Person.drop_collection()

        Person._get_collection().insert({'name': 'a', 'id': ''})
        for p in Person.objects():
            self.assertEqual(p.name, 'a')

    def test_len_during_iteration(self):
        """Tests that calling len on a queyset during iteration doesn't
        stop paging.
        """
        class Data(Document):
            pass

        for i in range(300):
            Data().save()

        records = Data.objects.limit(250)

        # This should pull all 250 docs from mongo and populate the result
        # cache
        len(records)

        # Assert that iterating over documents in the qs touches every
        # document even if we call len(qs) midway through the iteration.
        for i, r in enumerate(records):
            if i == 58:
                len(records)
        self.assertEqual(i, 249)

        # Assert the same behavior is true even if we didn't pre-populate the
        # result cache.
        records = Data.objects.limit(250)
        for i, r in enumerate(records):
            if i == 58:
                len(records)
        self.assertEqual(i, 249)

    def test_iteration_within_iteration(self):
        """You should be able to reliably iterate over all the documents
        in a given queryset even if there are multiple iterations of it
        happening at the same time.
        """
        class Data(Document):
            pass

        for i in range(300):
            Data().save()

        qs = Data.objects.limit(250)
        for i, doc in enumerate(qs):
            for j, doc2 in enumerate(qs):
                pass

        self.assertEqual(i, 249)
        self.assertEqual(j, 249)

    def test_in_operator_on_non_iterable(self):
        """Ensure that using the `__in` operator on a non-iterable raises an
        error.
        """
        class User(Document):
            name = StringField()

        class BlogPost(Document):
            content = StringField()
            authors = ListField(ReferenceField(User))

        User.drop_collection()
        BlogPost.drop_collection()

        author = User.objects.create(name='Test User')
        post = BlogPost.objects.create(content='Had a good coffee today...',
                                       authors=[author])

        # Make sure using `__in` with a list works
        blog_posts = BlogPost.objects(authors__in=[author])
        self.assertEqual(list(blog_posts), [post])

        # Using `__in` with a non-iterable should raise a TypeError
        self.assertRaises(TypeError, BlogPost.objects(authors__in=author.pk).count)

        # Using `__in` with a `Document` (which is seemingly iterable but not
        # in a way we'd expect) should raise a TypeError, too
        self.assertRaises(TypeError, BlogPost.objects(authors__in=author).count)


if __name__ == '__main__':
    unittest.main()
