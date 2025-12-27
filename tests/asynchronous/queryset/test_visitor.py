import datetime
import re
import unittest

import pytest
from bson import ObjectId

from mongoengine import *
from mongoengine.asynchronous import async_connect, async_disconnect
from mongoengine.common import _async_queryset_to_values
from mongoengine.errors import InvalidQueryError
from mongoengine.base.queryset import Q
from mongoengine.registry import _CollectionRegistry
from tests.asynchronous.utils import reset_async_connections


class TestQ(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        await async_connect(db="mongoenginetest")

        class Person(Document):
            name = StringField()
            age = IntField()
            meta = {"allow_inheritance": True}

        await Person.adrop_collection()
        self.Person = Person

    async def asyncTearDown(self):
        await async_disconnect()
        await reset_async_connections()
        _CollectionRegistry.clear()

    async def test_empty_q(self):
        """Ensure that empty Q objects won't hurt."""
        q1 = Q()
        q2 = Q(age__gte=18)
        q3 = Q()
        q4 = Q(name="test")
        q5 = Q()

        class Person(Document):
            name = StringField()
            age = IntField()

        query = {"$or": [{"age": {"$gte": 18}}, {"name": "test"}]}
        assert (q1 | q2 | q3 | q4 | q5).to_query(Person) == query

        query = {"age": {"$gte": 18}, "name": "test"}
        assert (q1 & q2 & q3 & q4 & q5).to_query(Person) == query

    async def test_q_with_dbref(self):
        """Ensure Q objects handle DBRefs correctly"""

        class User(Document):
            pass

        class Post(Document):
            created_user = ReferenceField(User)

        user = await User.aobjects.create()
        await Post.aobjects.create(created_user=user)

        assert await Post.aobjects.filter(created_user=user).count() == 1
        assert await Post.aobjects.filter(Q(created_user=user)).count() == 1

    async def test_and_combination(self):
        """Ensure that Q-objects correctly AND together."""

        class TestDoc(Document):
            x = IntField()
            y = StringField()

        query = (Q(x__lt=7) & Q(x__lt=3)).to_query(TestDoc)
        assert query == {"$and": [{"x": {"$lt": 7}}, {"x": {"$lt": 3}}]}

        query = (Q(y="a") & Q(x__lt=7) & Q(x__lt=3)).to_query(TestDoc)
        assert query == {"$and": [{"y": "a"}, {"x": {"$lt": 7}}, {"x": {"$lt": 3}}]}

        # Check normal cases work without an error
        query = Q(x__lt=7) & Q(x__gt=3)

        q1 = Q(x__lt=7)
        q2 = Q(x__gt=3)
        query = (q1 & q2).to_query(TestDoc)
        assert query == {"x": {"$lt": 7, "$gt": 3}}

        # More complex nested example
        query = Q(x__lt=100) & Q(y__ne="NotMyString")
        query &= Q(y__in=["a", "b", "c"]) & Q(x__gt=-100)
        mongo_query = {
            "x": {"$lt": 100, "$gt": -100},
            "y": {"$ne": "NotMyString", "$in": ["a", "b", "c"]},
        }
        assert query.to_query(TestDoc) == mongo_query

    async def test_or_combination(self):
        """Ensure that Q-objects correctly OR together."""

        class TestDoc(Document):
            x = IntField()

        q1 = Q(x__lt=3)
        q2 = Q(x__gt=7)
        query = (q1 | q2).to_query(TestDoc)
        assert query == {"$or": [{"x": {"$lt": 3}}, {"x": {"$gt": 7}}]}

    async def test_and_or_combination(self):
        """Ensure that Q-objects handle ANDing ORed components."""

        class TestDoc(Document):
            x = IntField()
            y = BooleanField()

        await TestDoc.adrop_collection()

        query = Q(x__gt=0) | Q(x__exists=False)
        query &= Q(x__lt=100)
        assert query.to_query(TestDoc) == {
            "$and": [
                {"$or": [{"x": {"$gt": 0}}, {"x": {"$exists": False}}]},
                {"x": {"$lt": 100}},
            ]
        }

        q1 = Q(x__gt=0) | Q(x__exists=False)
        q2 = Q(x__lt=100) | Q(y=True)
        query = (q1 & q2).to_query(TestDoc)

        await TestDoc(x=101).asave()
        await TestDoc(x=10).asave()
        await TestDoc(y=True).asave()

        assert query == {
            "$and": [
                {"$or": [{"x": {"$gt": 0}}, {"x": {"$exists": False}}]},
                {"$or": [{"x": {"$lt": 100}}, {"y": True}]},
            ]
        }
        assert 2 == await TestDoc.aobjects(q1 & q2).count()

    async def test_or_and_or_combination(self):
        """Ensure that Q-objects handle ORing ANDed ORed components. :)"""

        class TestDoc(Document):
            x = IntField()
            y = BooleanField()

        await TestDoc.adrop_collection()
        await TestDoc(x=-1, y=True).asave()
        await TestDoc(x=101, y=True).asave()
        await TestDoc(x=99, y=False).asave()
        await TestDoc(x=101, y=False).asave()

        q1 = Q(x__gt=0) & (Q(y=True) | Q(y__exists=False))
        q2 = Q(x__lt=100) & (Q(y=False) | Q(y__exists=False))
        query = (q1 | q2).to_query(TestDoc)

        assert query == {
            "$or": [
                {
                    "$and": [
                        {"x": {"$gt": 0}},
                        {"$or": [{"y": True}, {"y": {"$exists": False}}]},
                    ]
                },
                {
                    "$and": [
                        {"x": {"$lt": 100}},
                        {"$or": [{"y": False}, {"y": {"$exists": False}}]},
                    ]
                },
            ]
        }
        assert 2 == await TestDoc.aobjects(q1 | q2).count()

    async def test_multiple_occurence_in_field(self):
        class Test(Document):
            name = StringField(max_length=40)
            title = StringField(max_length=40)

        q1 = Q(name__contains="te") | Q(title__contains="te")
        q2 = Q(name__contains="12") | Q(title__contains="12")

        q3 = q1 & q2

        query = await _async_queryset_to_values(q3.to_query(Test))
        assert query["$and"][0] == await _async_queryset_to_values(q1.to_query(Test))
        assert query["$and"][1] == await _async_queryset_to_values(q2.to_query(Test))

    async def test_q_clone(self):
        class TestDoc(Document):
            x = IntField()

        await TestDoc.adrop_collection()
        for i in range(1, 101):
            t = TestDoc(x=i)
            await t.asave()

        # Check normal cases work without an error
        test = TestDoc.aobjects(Q(x__lt=7) & Q(x__gt=3))

        assert await test.count() == 3

        test2 = test.clone()
        assert await test2.count() == 3
        assert test2 != test

        test3 = test2.filter(x=6)
        assert await test3.count() == 1
        assert await test.count() == 3

    async def test_q(self):
        """Ensure that Q objects may be used to query for documents."""

        class BlogPost(Document):
            title = StringField()
            publish_date = DateTimeField()
            published = BooleanField()

        await BlogPost.adrop_collection()

        post1 = BlogPost(
            title="Test 1", publish_date=datetime.datetime(2010, 1, 8), published=False
        )
        await post1.asave()

        post2 = BlogPost(
            title="Test 2", publish_date=datetime.datetime(2010, 1, 15), published=True
        )
        await post2.asave()

        post3 = BlogPost(title="Test 3", published=True)
        await post3.asave()

        post4 = BlogPost(title="Test 4", publish_date=datetime.datetime(2010, 1, 8))
        await post4.asave()

        post5 = BlogPost(title="Test 1", publish_date=datetime.datetime(2010, 1, 15))
        await post5.asave()

        post6 = BlogPost(title="Test 1", published=False)
        await post6.asave()

        # Check ObjectId lookup works
        obj = await BlogPost.aobjects(id=post1.id).first()
        assert obj == post1

        # Check Q object combination with one does not exist
        q = BlogPost.aobjects(Q(title="Test 5") | Q(published=True))
        posts = [post.id async for post in q]

        published_posts = (post2, post3)
        assert all(obj.id in posts for obj in published_posts)

        q = BlogPost.aobjects(Q(title="Test 1") | Q(published=True))
        posts = [post.id async for post in q]
        published_posts = (post1, post2, post3, post5, post6)
        assert all(obj.id in posts for obj in published_posts)

        # Check Q object combination
        date = datetime.datetime(2010, 1, 10)
        q = BlogPost.aobjects(Q(publish_date__lte=date) | Q(published=True))
        posts = [post.id async for post in q]

        published_posts = (post1, post2, post3, post4)
        assert all(obj.id in posts for obj in published_posts)

        assert not any(obj.id in posts for obj in [post5, post6])

        await BlogPost.adrop_collection()

        # Check the 'in' operator
        await self.Person(name="user1", age=20).asave()
        await self.Person(name="user2", age=20).asave()
        await self.Person(name="user3", age=30).asave()
        await self.Person(name="user4", age=40).asave()

        assert await self.Person.aobjects(Q(age__in=[20])).count() == 2
        assert await self.Person.aobjects(Q(age__in=[20, 30])).count() == 3

        # Test invalid query objs
        with pytest.raises(InvalidQueryError):
            self.Person.aobjects("user1")

        # filter should fail, too
        with pytest.raises(InvalidQueryError):
            self.Person.aobjects.filter("user1")

    async def test_q_regex(self):
        """Ensure that Q objects can be queried using regexes."""
        person = self.Person(name="Guido van Rossum")
        await person.asave()

        obj = await self.Person.aobjects(Q(name=re.compile("^Gui"))).first()
        assert obj == person
        obj = await self.Person.aobjects(Q(name=re.compile("^gui"))).first()
        assert obj is None

        obj = await self.Person.aobjects(Q(name=re.compile("^gui", re.I))).first()
        assert obj == person

        obj = await self.Person.aobjects(Q(name__not=re.compile("^bob"))).first()
        assert obj == person

        obj = await self.Person.aobjects(Q(name__not=re.compile("^Gui"))).first()
        assert obj is None

    async def test_q_repr(self):
        assert repr(Q()) == "Q(**{})"
        assert repr(Q(name="test")) == "Q(**{'name': 'test'})"

        assert (
                repr(Q(name="test") & Q(age__gte=18))
                == "(Q(**{'name': 'test'}) & Q(**{'age__gte': 18}))"
        )

        assert (
                repr(Q(name="test") | Q(age__gte=18))
                == "(Q(**{'name': 'test'}) | Q(**{'age__gte': 18}))"
        )

    async def test_q_lists(self):
        """Ensure that Q objects query ListFields correctly."""

        class BlogPost(Document):
            tags = ListField(StringField())

        await BlogPost.adrop_collection()

        await BlogPost(tags=["python", "mongo"]).asave()
        await BlogPost(tags=["python"]).asave()

        assert await BlogPost.aobjects(Q(tags="mongo")).count() == 1
        assert await BlogPost.aobjects(Q(tags="python")).count() == 2

        await BlogPost.adrop_collection()

    async def test_q_merge_queries_edge_case(self):
        class User(Document):
            email = EmailField(required=False)
            name = StringField()

        await User.adrop_collection()
        pk = ObjectId()
        await User(email="example@example.com", pk=pk).asave()

        assert (
                1
                == await User.aobjects.filter(Q(email="example@example.com") | Q(name="John Doe"))
                .limit(2)
                .filter(pk=pk)
                .count()
        )

    async def test_chained_q_or_filtering(self):
        class Post(EmbeddedDocument):
            name = StringField(required=True)

        class Item(Document):
            postables = ListField(EmbeddedDocumentField(Post))

        await Item.adrop_collection()

        await Item(postables=[Post(name="a"), Post(name="b")]).asave()
        await Item(postables=[Post(name="a"), Post(name="c")]).asave()
        await Item(postables=[Post(name="a"), Post(name="b"), Post(name="c")]).asave()

        assert (
                await Item.aobjects(Q(postables__name="a") & Q(postables__name="b")).count() == 2
        )
        assert (
                await Item.aobjects.filter(postables__name="a").filter(postables__name="b").count()
                == 2
        )

    async def test_equality(self):
        assert Q(name="John") == Q(name="John")
        assert Q() == Q()

    async def test_inequality(self):
        assert Q(name="John") != Q(name="Ralph")

    async def test_operation_equality(self):
        q1 = Q(name="John") | Q(title="Sir") & Q(surname="Paul")
        q2 = Q(name="John") | Q(title="Sir") & Q(surname="Paul")
        assert q1 == q2

    async def test_operation_inequality(self):
        q1 = Q(name="John") | Q(title="Sir")
        q2 = Q(title="Sir") | Q(name="John")
        assert q1 != q2

    async def test_combine_and_empty(self):
        q = Q(x=1)
        assert q & Q() == q
        assert Q() & q == q

    async def test_combine_and_both_empty(self):
        assert Q() & Q() == Q()

    async def test_combine_or_empty(self):
        q = Q(x=1)
        assert q | Q() == q
        assert Q() | q == q

    async def test_combine_or_both_empty(self):
        assert Q() | Q() == Q()

    async def test_q_bool(self):
        assert Q(name="John")
        assert not Q()

    async def test_combine_bool(self):
        assert not Q() & Q()
        assert Q() & Q(name="John")
        assert Q(name="John") & Q()
        assert Q() | Q(name="John")
        assert Q(name="John") | Q()


if __name__ == "__main__":
    unittest.main()
