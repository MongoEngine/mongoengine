import datetime
import unittest
import uuid
from decimal import Decimal

import pymongo
import pytest
from bson import DBRef, ObjectId
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.read_preferences import ReadPreference
from pymongo.results import UpdateResult

from mongoengine import *
from mongoengine.base import LazyReference
from mongoengine.context_managers import async_query_counter, switch_db
from mongoengine.errors import InvalidQueryError
from mongoengine.mongodb_support import (
    async_get_mongodb_version,
)
from mongoengine.pymongo_support import PYMONGO_VERSION
from mongoengine.base.queryset import (
    QuerySetManager,
    queryset_manager, CASCADE, NULLIFY, DENY, PULL,
)
from mongoengine.registry import _CollectionRegistry
from tests.asynchronous.utils import (
    async_db_ops_tracker,
    async_get_as_pymongo,
    reset_async_connections,
)


def get_key_compat(mongo_ver):
    ORDER_BY_KEY = "sort"
    CMD_QUERY_KEY = "command"
    return ORDER_BY_KEY, CMD_QUERY_KEY


class TestQueryset(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await async_connect(db="mongoenginetest")
        await async_connect(db="mongoenginetest2", alias="test2")

        class PersonMeta(EmbeddedDocument):
            weight = IntField()

        class Person(Document):
            name = StringField()
            age = IntField()
            person_meta = EmbeddedDocumentField(PersonMeta)
            meta = {"allow_inheritance": True}

        await Person.adrop_collection()

        self.PersonMeta = PersonMeta
        self.Person = Person

        self.mongodb_version = await async_get_mongodb_version()

    async def asyncTearDown(self):
        await async_disconnect(alias="default")
        await async_disconnect(alias="test2")
        await reset_async_connections()
        _CollectionRegistry.clear()

    async def test_initialisation(self):
        """Ensure that a QuerySet is correctly initialised by AsyncQuerySetManager."""
        assert isinstance(self.Person.aobjects, AsyncQuerySet)
        assert (
                (await self.Person.aobjects._collection).name == self.Person._get_collection_name()
        )
        assert isinstance(
            await self.Person.aobjects._collection, AsyncCollection
        )

    async def test_can_perform_joins_references(self):
        class BlogPost(Document):
            author = ReferenceField(self.Person)
            author2 = GenericReferenceField(choices=(self.Person,))

        await BlogPost.adrop_collection()
        await self.Person.adrop_collection()

        person = await self.Person(name="test").asave()
        await BlogPost(author=person, author2=person).asave()

        # SHOULD NOT raise
        await BlogPost.aobjects(author__name="test").to_list()
        await BlogPost.aobjects(author2__name="test").to_list()

    async def test_find(self):
        """Ensure that a query returns a valid set of results."""
        user_a = await self.Person.aobjects.create(name="User A", age=20)
        user_b = await self.Person.aobjects.create(name="User B", age=30)

        # Find all people in the collection
        people = self.Person.aobjects
        assert await people.count() == 2
        results = await people.to_list()

        assert isinstance(results[0], self.Person)
        assert isinstance(results[0].id, ObjectId)

        assert results[0] == user_a
        assert results[0].name == "User A"
        assert results[0].age == 20

        assert results[1] == user_b
        assert results[1].name == "User B"
        assert results[1].age == 30

        # Filter people by age
        people = self.Person.aobjects(age=20)
        assert await people.count() == 1
        person = await anext(people)
        assert person == user_a
        assert person.name == "User A"
        assert person.age == 20

    async def test_slicing_sets_empty_limit_skip(self):
        await self.Person.aobjects.insert(
            [self.Person(name=f"User {i}", age=i) for i in range(5)],
            load_bulk=False,
        )

        await self.Person.aobjects.create(name="User B", age=30)
        await self.Person.aobjects.create(name="User C", age=40)

        qs = self.Person.aobjects().skip(1).limit(1)
        assert (qs._skip, qs._limit) == (1, 1)
        assert len(await qs.to_list()) == 1

        # Test edge case of [1:1] which should return nothing
        # and require a hack so that it doesn't clash with limit(0)
        qs = self.Person.aobjects().skip(1).limit(0)
        assert (qs._skip, qs._limit) == (1, 0)

        qs2 = qs.skip(1).limit(4)  # Make sure that further slicing resets _empty
        assert (qs2._skip, qs2._limit) == (1, 4)
        assert len(await qs2.to_list()) == 4

    async def test_limit_0_returns_all_documents(self):
        await self.Person.aobjects.create(name="User A", age=20)
        await self.Person.aobjects.create(name="User B", age=30)

        n_docs = await self.Person.aobjects().count()

        persons = await self.Person.aobjects().limit(0).to_list()
        assert len(persons) == 2 == n_docs

    async def test_limit_0(self):
        """Ensure that QuerySet.limit works as expected."""
        await self.Person.aobjects.create(name="User A", age=20)

        # Test limit with 0 as parameter
        qs = self.Person.aobjects.limit(0)
        assert await qs.count() == 0

    async def test_limit(self):
        """Ensure that QuerySet.limit works as expected."""
        user_a = await self.Person.aobjects.create(name="User A", age=20)
        _ = await self.Person.aobjects.create(name="User B", age=30)

        # Test limit on a new queryset
        people = await self.Person.aobjects.limit(1).to_list()
        assert len(people) == 1
        assert people[0] == user_a

        # Test limit on an existing queryset
        people = self.Person.aobjects
        assert len(await people.to_list()) == 2
        people2 = await people.limit(1).to_list()
        assert len(await people.to_list()) == 2
        assert len(people2) == 1
        assert people2[0] == user_a

        # Test limit with 0 as parameter
        people = self.Person.aobjects.limit(0)
        assert await people.count(with_limit_and_skip=True) == 2
        assert len(await people.to_list()) == 2

        # Test chaining of only after limit
        person = await self.Person.aobjects().limit(1).only("name").first()
        assert person == user_a
        assert person.name == "User A"
        assert person.age is None

    async def test_skip(self):
        """Ensure that QuerySet.skip works as expected."""
        user_a = await self.Person.aobjects.create(name="User A", age=20)
        user_b = await self.Person.aobjects.create(name="User B", age=30)

        # Test skip on a new queryset
        people = await self.Person.aobjects.skip(0).to_list()
        assert len(people) == 2
        assert people[0] == user_a
        assert people[1] == user_b

        people = await self.Person.aobjects.skip(1).to_list()
        assert len(people) == 1
        assert people[0] == user_b

        # Test skip on an existing queryset
        people = self.Person.aobjects
        assert len(await people.to_list()) == 2
        people2 = await people.skip(1).to_list()
        assert len(await people.to_list()) == 2
        assert len(people2) == 1
        assert people2[0] == user_b

        # Test chaining of only after skip
        person = await self.Person.aobjects().skip(1).only("name").first()
        assert person == user_b
        assert person.name == "User B"
        assert person.age is None

    async def test___getitem___invalid_index(self):
        """Ensure slicing a queryset works as expected."""
        with pytest.raises(TypeError):
            await self.Person.aobjects().to_list()["a"]

    async def test_find_one(self):
        """Ensure that a query using find_one returns a valid result."""
        person1 = self.Person(name="User A", age=20)
        await person1.asave()
        person2 = self.Person(name="User B", age=30)
        await person2.asave()

        # Retrieve the first person from the database
        person = await self.Person.aobjects.first()
        assert isinstance(person, self.Person)
        assert person.name == "User A"
        assert person.age == 20

        # Use a query to filter the people found to just person2
        person = await self.Person.aobjects(age=30).first()
        assert person.name == "User B"

        person = await self.Person.aobjects(age__lt=30).first()
        assert person.name == "User A"

        # Find a document using just the object id
        person = await self.Person.aobjects.with_id(person1.id)
        assert person.name == "User A"

        with pytest.raises(InvalidQueryError):
            await self.Person.aobjects(name="User A").with_id(person1.id)

    async def test_get_no_document_exists_raises_doesnotexist(self):
        assert await self.Person.aobjects.count() == 0
        # Try retrieving when no objects exist
        with pytest.raises(DoesNotExist):
            await self.Person.aobjects.get()
        with pytest.raises(DoesNotExist):
            await self.Person.aobjects.get()

    async def test_get_multiple_match_raises_multipleobjectsreturned(self):
        """Ensure that a query using ``get`` returns at most one result."""
        assert await self.Person.aobjects().count() == 0

        person1 = self.Person(name="User A", age=20)
        await person1.asave()

        p = await self.Person.aobjects.get()
        assert p == person1

        person2 = self.Person(name="User B", age=20)
        await person2.asave()

        person3 = self.Person(name="User C", age=30)
        await person3.asave()

        # .get called without argument
        with pytest.raises(MultipleObjectsReturned):
            await self.Person.aobjects.get()
        with pytest.raises(MultipleObjectsReturned):
            await self.Person.aobjects.get()

        # check filtering
        with pytest.raises(MultipleObjectsReturned):
            await self.Person.aobjects.get(age__lt=30)
        with pytest.raises(MultipleObjectsReturned) as exc_info:
            await self.Person.aobjects(age__lt=30).get()
        assert "2 or more items returned, instead of 1" == str(exc_info.value)

        # Use a query to filter the people found to just person2
        person = await self.Person.aobjects.get(age=30)
        assert person == person3

    async def test_find_array_position(self):
        """Ensure that query by array position works."""

        class Comment(EmbeddedDocument):
            name = StringField()

        class Post(EmbeddedDocument):
            comments = ListField(EmbeddedDocumentField(Comment))

        class Blog(Document):
            tags = ListField(StringField())
            posts = ListField(EmbeddedDocumentField(Post))

        await Blog.adrop_collection()

        await Blog.aobjects.create(tags=["a", "b"])
        assert await Blog.aobjects(tags__0="a").count() == 1
        assert await Blog.aobjects(tags__0="b").count() == 0
        assert await Blog.aobjects(tags__1="a").count() == 0
        assert await Blog.aobjects(tags__1="b").count() == 1

        await Blog.adrop_collection()

        comment1 = Comment(name="testa")
        comment2 = Comment(name="testb")
        post1 = Post(comments=[comment1, comment2])
        post2 = Post(comments=[comment2, comment2])
        blog1 = await Blog.aobjects.create(posts=[post1, post2])
        blog2 = await Blog.aobjects.create(posts=[post2, post1])

        blog = await Blog.aobjects(posts__0__comments__0__name="testa").get()
        assert blog == blog1

        blog = await Blog.aobjects(posts__0__comments__0__name="testb").get()
        assert blog == blog2

        query = Blog.aobjects(posts__1__comments__1__name="testb")
        assert await query.count() == 2

        query = Blog.aobjects(posts__1__comments__1__name="testa")
        assert await query.count() == 0

        query = Blog.aobjects(posts__0__comments__1__name="testa")
        assert await query.count() == 0

        await Blog.adrop_collection()

    async def test_none(self):
        class A(Document):
            s = StringField()

        await A.adrop_collection()
        await A().asave()

        # validate collection not empty
        assert await A.aobjects.count() == 1

        # update operations
        assert await A.aobjects.none().update(s="1") == 0
        assert await A.aobjects.none().update_one(s="1") == 0
        assert await A.aobjects.none().modify(s="1") is None

        # validate noting change by update operations
        assert await A.aobjects(s="1").count() == 0

        # fetch queries
        assert await A.aobjects.none().first() is None
        assert await A.aobjects.none().to_list() == []
        assert await A.aobjects.none().all().to_list() == []
        assert await A.aobjects.none().limit(1).to_list() == []
        assert await A.aobjects.none().skip(1).to_list() == []
        assert await A.aobjects.none().limit(5).to_list() == []

    async def test_chaining(self):
        class A(Document):
            s = StringField()

        class B(Document):
            ref = ReferenceField(A)
            boolfield = BooleanField(default=False)

        await A.adrop_collection()
        await B.adrop_collection()

        a1 = await A(s="test1").asave()
        a2 = await A(s="test2").asave()

        await B(ref=a1, boolfield=True).asave()

        # Works
        q1 = B.aobjects.filter(ref__in=[a1, a2], ref=a1)._query

        # Doesn't work
        q2 = B.aobjects.filter(ref__in=[a1, a2])
        q2 = q2.filter(ref=a1)._query
        assert q1 == q2

        a_objects = A.aobjects(s="test1")
        query = B.aobjects(ref__in=a_objects)
        query = query.filter(boolfield=True)
        assert await query.count() == 1

    async def test_batch_size(self):
        """Ensure that batch_size works."""

        class A(Document):
            s = StringField()

        await A.adrop_collection()

        await A.aobjects.insert([A(s=str(i)) for i in range(100)], load_bulk=True)

        # test iterating over the result set
        cnt = 0
        async for _ in A.aobjects.batch_size(10):
            cnt += 1
        assert cnt == 100

        # test chaining
        qs = A.aobjects.all()
        qs = qs.limit(10).batch_size(20).skip(91)
        cnt = 0
        async for _ in qs:
            cnt += 1
        assert cnt == 9

        # test invalid batch size
        qs = A.aobjects.batch_size(-1)
        with pytest.raises(ValueError):
            await qs.to_list()

    def test_batch_size_cloned(self):
        class A(Document):
            s = StringField()

        # test that batch size gets cloned
        qs = A.aobjects.batch_size(5)
        assert qs._batch_size == 5
        qs_clone = qs.clone()
        assert qs_clone._batch_size == 5

    async def test_update_write_concern(self):
        """Test that passing write_concern works"""
        await self.Person.adrop_collection()

        write_concern = {"fsync": True}
        author = await self.Person.aobjects.create(name="Test User")
        await author.asave(write_concern=write_concern)

        # Ensure no regression of #1958
        author = self.Person(name="Test User2")
        await author.asave(write_concern=None)  # will default to {w: 1}

        result = await self.Person.aobjects.update(set__name="Ross", write_concern={"w": 1})

        assert result == 2
        result = await self.Person.aobjects.update(set__name="Ross", write_concern={"w": 0})
        assert result is None

        result = await self.Person.aobjects.update_one(
            set__name="Test User", write_concern={"w": 1}
        )
        assert result == 1
        result = await self.Person.aobjects.update_one(
            set__name="Test User", write_concern={"w": 0}
        )
        assert result is None

    async def test_update_update_has_a_value(self):
        """Test to ensure that update is passed a value to update to"""
        await self.Person.adrop_collection()

        author = await self.Person.aobjects.create(name="Test User")

        with pytest.raises(OperationError):
            await self.Person.aobjects(pk=author.pk).update({})

        with pytest.raises(OperationError):
            await self.Person.aobjects(pk=author.pk).update_one({})

    async def test_update_array_position(self):
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

        await Blog.adrop_collection()

        comment1 = Comment(name="testa")
        comment2 = Comment(name="testb")
        post1 = Post(comments=[comment1, comment2])
        post2 = Post(comments=[comment2, comment2])
        await Blog.aobjects.create(posts=[post1, post2])
        await Blog.aobjects.create(posts=[post2, post1])

        # Update all of the first comments of second posts of all blogs
        await Blog.aobjects().update(set__posts__1__comments__0__name="testc")
        testc_blogs = Blog.aobjects(posts__1__comments__0__name="testc")
        assert await testc_blogs.count() == 2

        await Blog.adrop_collection()
        await Blog.aobjects.create(posts=[post1, post2])
        await Blog.aobjects.create(posts=[post2, post1])

        # Update only the first blog returned by the query
        await Blog.aobjects().update_one(set__posts__1__comments__1__name="testc")
        testc_blogs = Blog.aobjects(posts__1__comments__1__name="testc")
        assert await testc_blogs.count() == 1

        # Check that using this indexing syntax on a non-list fails
        with pytest.raises(InvalidQueryError):
            await Blog.aobjects().update(set__posts__1__comments__0__name__1="asdf")

        await Blog.adrop_collection()

    async def test_update_array_filters(self):
        """Ensure that updating by array_filters works."""

        class Comment(EmbeddedDocument):
            comment_tags = ListField(StringField())

        class Blog(Document):
            tags = ListField(StringField())
            comments = EmbeddedDocumentField(Comment)

        await Blog.adrop_collection()

        # update one
        await Blog.aobjects.create(tags=["test1", "test2", "test3"])

        await Blog.aobjects().update_one(
            __raw__={"$set": {"tags.$[element]": "test11111"}},
            array_filters=[{"element": {"$eq": "test2"}}],
        )
        testc_blogs = Blog.aobjects(tags="test11111")

        assert await testc_blogs.count() == 1

        # modify
        await Blog.adrop_collection()

        # update one
        await Blog.aobjects.create(tags=["test1", "test2", "test3"])

        new_blog = await Blog.aobjects().modify(
            __raw__={"$set": {"tags.$[element]": "test11111"}},
            array_filters=[{"element": {"$eq": "test2"}}],
            new=True,
        )
        testc_blogs = Blog.aobjects(tags="test11111")
        assert new_blog == await testc_blogs.first()

        assert await testc_blogs.count() == 1

        await Blog.adrop_collection()

        # update one inner list
        comments = Comment(comment_tags=["test1", "test2", "test3"])
        await Blog.aobjects.create(comments=comments)

        await Blog.aobjects().update_one(
            __raw__={"$set": {"comments.comment_tags.$[element]": "test11111"}},
            array_filters=[{"element": {"$eq": "test2"}}],
        )
        testc_blogs = Blog.aobjects(comments__comment_tags="test11111")

        assert await testc_blogs.count() == 1

        # update many
        await Blog.adrop_collection()

        await Blog.aobjects.create(tags=["test1", "test2", "test3", "test_all"])
        await Blog.aobjects.create(tags=["test4", "test5", "test6", "test_all"])

        await Blog.aobjects().update(
            __raw__={"$set": {"tags.$[element]": "test11111"}},
            array_filters=[{"element": {"$eq": "test2"}}],
        )
        testc_blogs = Blog.aobjects(tags="test11111")

        assert await testc_blogs.count() == 1

        await Blog.aobjects().update(
            __raw__={"$set": {"tags.$[element]": "test_all1234577"}},
            array_filters=[{"element": {"$eq": "test_all"}}],
        )
        testc_blogs = Blog.aobjects(tags="test_all1234577")

        assert await testc_blogs.count() == 2

    async def test_update_using_positional_operator(self):
        """Ensure that the list fields can be updated using the positional
        operator."""

        class Comment(EmbeddedDocument):
            by = StringField()
            votes = IntField()

        class BlogPost(Document):
            title = StringField()
            comments = ListField(EmbeddedDocumentField(Comment))

        await BlogPost.adrop_collection()

        c1 = Comment(by="joe", votes=3)
        c2 = Comment(by="jane", votes=7)

        await BlogPost(title="ABC", comments=[c1, c2]).asave()

        await BlogPost.aobjects(comments__by="jane").update(inc__comments__S__votes=1)

        post = await BlogPost.aobjects.first()
        assert post.comments[1].by == "jane"
        assert post.comments[1].votes == 8

    async def test_update_using_positional_operator_matches_first(self):
        # Currently the $ operator only applies to the first matched item in
        # the query

        class Simple(Document):
            x = ListField()

        await Simple.adrop_collection()
        await Simple(x=[1, 2, 3, 2]).asave()
        await Simple.aobjects(x=2).update(inc__x__S=1)

        simple = await Simple.aobjects.first()
        assert simple.x == [1, 3, 3, 2]
        await Simple.adrop_collection()

        # You can set multiples
        await Simple.adrop_collection()
        await Simple(x=[1, 2, 3, 4]).asave()
        await Simple(x=[2, 3, 4, 5]).asave()
        await Simple(x=[3, 4, 5, 6]).asave()
        await Simple(x=[4, 5, 6, 7]).asave()
        await Simple.aobjects(x=3).update(set__x__S=0)

        s = await Simple.aobjects().to_list()
        assert s[0].x == [1, 2, 0, 4]
        assert s[1].x == [2, 0, 4, 5]
        assert s[2].x == [0, 4, 5, 6]
        assert s[3].x == [4, 5, 6, 7]

        # Using "$unset" with an expression like this "array.$" will result in
        # the array item becoming None, not being removed.
        await Simple.adrop_collection()
        await Simple(x=[1, 2, 3, 4, 3, 2, 3, 4]).asave()
        await Simple.aobjects(x=3).update(unset__x__S=1)
        simple = await Simple.aobjects.first()
        assert simple.x == [1, 2, None, 4, 3, 2, 3, 4]

        # Nested updates arent supported yet..
        with pytest.raises(OperationError):
            await Simple.adrop_collection()
            await Simple(x=[{"test": [1, 2, 3, 4]}]).asave()
            await Simple.aobjects(x__test=2).update(set__x__S__test__S=3)
            assert simple.x == [1, 2, 3, 4]

    async def test_update_using_positional_operator_embedded_document(self):
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

        await BlogPost.adrop_collection()

        c1 = Comment(by="joe", votes=Vote(score=3))
        c2 = Comment(by="jane", votes=Vote(score=7))

        await BlogPost(title="ABC", comments=[c1, c2]).asave()

        await BlogPost.aobjects(comments__by="joe").update(
            set__comments__S__votes=Vote(score=4)
        )

        post = await BlogPost.aobjects.first()
        assert post.comments[0].by == "joe"
        assert post.comments[0].votes.score == 4

    async def test_update_min_max(self):
        class Scores(Document):
            high_score = IntField()
            low_score = IntField()

        scores = await Scores.aobjects.create(high_score=800, low_score=200)

        await Scores.aobjects(id=scores.id).update(min__low_score=150)
        assert (await Scores.aobjects.get(id=scores.id)).low_score == 150
        await Scores.aobjects(id=scores.id).update(min__low_score=250)
        assert (await Scores.aobjects.get(id=scores.id)).low_score == 150

        await Scores.aobjects(id=scores.id).update(max__high_score=1000)
        assert (await Scores.aobjects.get(id=scores.id)).high_score == 1000
        await Scores.aobjects(id=scores.id).update(max__high_score=500)
        assert (await Scores.aobjects.get(id=scores.id)).high_score == 1000

    async def test_update_multiple(self):
        class Product(Document):
            item = StringField()
            price = FloatField()

        product = await Product.aobjects.create(item="ABC", price=10.99)
        product = await Product.aobjects.create(item="ABC", price=10.99)
        await Product.aobjects(id=product.id).update(mul__price=1.25)
        assert (await Product.aobjects.get(id=product.id)).price == 13.7375
        unknown_product = await Product.aobjects.create(item="Unknown")
        await Product.aobjects(id=unknown_product.id).update(mul__price=100)
        assert (await Product.aobjects.get(id=unknown_product.id)).price == 0

    async def test_updates_can_have_match_operators(self):
        class Comment(EmbeddedDocument):
            content = StringField()
            name = StringField(max_length=120)
            vote = IntField()

        class Post(Document):
            title = StringField(required=True)
            tags = ListField(StringField())
            comments = ListField(EmbeddedDocumentField("Comment"))

        await Post.adrop_collection()

        comm1 = Comment(content="very funny indeed", name="John S", vote=1)
        comm2 = Comment(content="kind of funny", name="Mark P", vote=0)

        await Post(
            title="Fun with MongoEngine",
            tags=["mongodb", "mongoengine"],
            comments=[comm1, comm2],
        ).asave()

        await Post.aobjects().update_one(pull__comments__vote__lt=1)

        assert 1 == len((await Post.aobjects.first()).comments)

    async def test_mapfield_update(self):
        """Ensure that the MapField can be updated."""

        class Member(EmbeddedDocument):
            gender = StringField()
            age = IntField()

        class Club(Document):
            members = MapField(EmbeddedDocumentField(Member))

        await Club.adrop_collection()

        club = Club()
        club.members["John"] = Member(gender="M", age=13)
        await club.asave()

        await Club.aobjects().update(set__members={"John": Member(gender="F", age=14)})

        club = await Club.aobjects().first()
        assert club.members["John"].gender == "F"
        assert club.members["John"].age == 14

    async def test_dictfield_update(self):
        """Ensure that the DictField can be updated."""

        class Club(Document):
            members = DictField()

        club = Club()
        club.members["John"] = {"gender": "M", "age": 13}
        await club.asave()

        await Club.aobjects().update(set__members={"John": {"gender": "F", "age": 14}})

        club = await Club.aobjects().first()
        assert club.members["John"]["gender"] == "F"
        assert club.members["John"]["age"] == 14

    async def test_update_results(self):
        await self.Person.adrop_collection()

        result = await self.Person(name="Bob", age=25).aupdate(upsert=True, full_result=True)
        assert isinstance(result, UpdateResult)
        assert "upserted" in result.raw_result
        assert not result.raw_result["updatedExisting"]

        bob = await self.Person.aobjects.first()
        result = await bob.aupdate(set__age=30, full_result=True)
        assert isinstance(result, UpdateResult)
        assert result.raw_result["updatedExisting"]

        await self.Person(name="Bob", age=20).asave()
        result = await self.Person.aobjects(name="Bob").update(set__name="bobby", multi=True)
        assert result == 2

    async def test_update_validate(self):
        class EmDoc(EmbeddedDocument):
            str_f = StringField()

        class Doc(Document):
            str_f = StringField()
            dt_f = DateTimeField()
            cdt_f = ComplexDateTimeField()
            ed_f = EmbeddedDocumentField(EmDoc)

        with pytest.raises(ValidationError):
            await Doc.aobjects().update(str_f=1, upsert=True)
        with pytest.raises(ValidationError):
            await Doc.aobjects().update(dt_f="datetime", upsert=True)
        with pytest.raises(ValidationError):
            await Doc.aobjects().update(ed_f__str_f=1, upsert=True)

    async def test_update_related_models(self):
        class TestPerson(Document):
            name = StringField()

        class TestOrganization(Document):
            name = StringField()
            owner = ReferenceField(TestPerson)

        await TestPerson.adrop_collection()
        await TestOrganization.adrop_collection()

        p = TestPerson(name="p1")
        await p.asave()
        o = TestOrganization(name="o1")
        await o.asave()

        o.owner = p
        p.name = "p2"

        assert o._get_changed_fields() == ["owner"]
        assert p._get_changed_fields() == ["name"]

        await o.asave()

        assert o._get_changed_fields() == []
        assert p._get_changed_fields() == ["name"]  # Fails; it's empty

        # This will do NOTHING at all, even though we changed the name
        await p.asave()

        await p.areload()

        assert p.name == "p2"  # Fails; it's still `p1`

    async def test_upsert(self):
        await self.Person.adrop_collection()

        await self.Person.aobjects(pk=ObjectId(), name="Bob", age=30).update(upsert=True)

        bob = await self.Person.aobjects.first()
        assert "Bob" == bob.name
        assert 30 == bob.age

    async def test_upsert_one(self):
        await self.Person.adrop_collection()

        bob = await self.Person.aobjects(name="Bob", age=30).upsert_one()

        assert "Bob" == bob.name
        assert 30 == bob.age

        bob.name = "Bobby"
        await bob.asave()

        bobby = await self.Person.aobjects(name="Bobby", age=30).upsert_one()

        assert "Bobby" == bobby.name
        assert 30 == bobby.age
        assert bob.id == bobby.id

    async def test_set_on_insert(self):
        await self.Person.adrop_collection()

        await self.Person.aobjects(pk=ObjectId()).update(
            set__name="Bob", set_on_insert__age=30, upsert=True
        )

        bob = await self.Person.aobjects.first()
        assert "Bob" == bob.name
        assert 30 == bob.age

    async def test_rename(self):
        await self.Person.adrop_collection()
        await self.Person.aobjects.create(name="Foo", age=11)

        bob = await self.Person.aobjects.as_pymongo().first()
        assert "age" in bob
        assert bob["age"] == 11

        await self.Person.aobjects(name="Foo").update(rename__age="person_age")

        bob = await self.Person.aobjects.as_pymongo().first()
        assert "age" not in bob
        assert "person_age" in bob
        assert bob["person_age"] == 11

    async def test_save_and_only_on_fields_with_default(self):
        class Embed(EmbeddedDocument):
            field = IntField()

        class B(Document):
            meta = {"collection": "b"}

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
        await record.asave()

        # Checking it was saved correctly
        await record.areload()
        assert record.field == 2
        assert record.embed_no_default.field == 2
        assert record.embed.field == 2

        # Request only the _id field and save
        clone = await B.aobjects().only("id").first()
        await clone.asave()

        # Reload the record and see that the embed data is not lost
        await record.areload()
        assert record.field == 2
        assert record.embed_no_default.field == 2
        assert record.embed.field == 2

    async def test_bulk_insert(self):  # todo
        """Ensure that bulk insert works"""

        class Comment(EmbeddedDocument):
            name = StringField()

        class Post(EmbeddedDocument):
            comments = ListField(EmbeddedDocumentField(Comment))

        class Blog(Document):
            title = StringField(unique=True)
            tags = ListField(StringField())
            posts = ListField(EmbeddedDocumentField(Post))

        await Blog.adrop_collection()

        # Recreates the collection
        assert 0 == await Blog.aobjects.count()

        comment1 = Comment(name="testa")
        comment2 = Comment(name="testb")
        post1 = Post(comments=[comment1, comment2])
        post2 = Post(comments=[comment2, comment2])

        # Check bulk insert using load_bulk=False
        blogs = [Blog(title="%s" % i, posts=[post1, post2]) for i in range(99)]
        async with async_query_counter() as q:
            assert await q.eq(0)
            await Blog.aobjects.insert(blogs, load_bulk=False)
            assert await q.eq(1)  # 1 entry containing the list of inserts

        assert await Blog.aobjects.count() == len(blogs)

        await Blog.adrop_collection()
        await Blog.aensure_indexes()

        # Check bulk insert using load_bulk=True
        blogs = [Blog(title="%s" % i, posts=[post1, post2]) for i in range(99)]
        async with async_query_counter() as q:
            assert await q.eq(0)
            await Blog.aobjects.insert(blogs)
            assert await q.eq(2)  # 1 for insert 1 for fetch

        await Blog.adrop_collection()

        comment1 = Comment(name="testa")
        comment2 = Comment(name="testb")
        post1 = Post(comments=[comment1, comment2])
        post2 = Post(comments=[comment2, comment2])
        blog1 = Blog(title="code", posts=[post1, post2])
        blog2 = Blog(title="mongodb", posts=[post2, post1])
        blog1, blog2 = await Blog.aobjects.insert([blog1, blog2])
        assert blog1.title == "code"
        assert blog2.title == "mongodb"

        assert await Blog.aobjects.count() == 2

        # test inserting an existing document (shouldn't be allowed)
        with pytest.raises(OperationError) as exc_info:
            blog = await Blog.aobjects.first()
            await Blog.aobjects.insert(blog)
        assert (
                str(exc_info.value)
                == "Some documents have ObjectIds, use doc.aupdate() instead"
        )

        # test inserting a query set
        with pytest.raises(OperationError) as exc_info:
            blogs_qs = Blog.aobjects
            await Blog.aobjects.insert(blogs_qs)
        assert (
                str(exc_info.value)
                == "Some documents have ObjectIds, use doc.aupdate() instead"
        )

        # insert 1 new doc
        new_post = Blog(title="code123", id=ObjectId())
        await Blog.aobjects.insert(new_post)

        await Blog.adrop_collection()

        blog1 = Blog(title="code", posts=[post1, post2])
        blog1 = await Blog.aobjects.insert(blog1)
        assert blog1.title == "code"
        assert await Blog.aobjects.count() == 1

        await Blog.adrop_collection()
        blog1 = Blog(title="code", posts=[post1, post2])
        obj_id = await Blog.aobjects.insert(blog1, load_bulk=False)
        assert isinstance(obj_id, ObjectId)

        await Blog.adrop_collection()
        post3 = Post(comments=[comment1, comment1])
        blog1 = Blog(title="foo", posts=[post1, post2])
        blog2 = Blog(title="bar", posts=[post2, post3])
        await Blog.aobjects.insert([blog1, blog2])

        with pytest.raises(NotUniqueError):
            await Blog.aobjects.insert(Blog(title=blog2.title))

        assert await Blog.aobjects.count() == 2

    async def test_bulk_insert_different_class_fails(self):
        class Blog(Document):
            pass

        class Author(Document):
            pass

        # try inserting a different document class
        with pytest.raises(OperationError):
            await Blog.aobjects.insert(Author())

    async def test_bulk_insert_with_wrong_type(self):
        class Blog(Document):
            name = StringField()

        await Blog.adrop_collection()
        await Blog(name="test").asave()

        with pytest.raises(OperationError):
            await Blog.aobjects.insert("HELLO WORLD")

        with pytest.raises(OperationError):
            await Blog.aobjects.insert({"name": "garbage"})

    async def test_bulk_insert_update_input_document_ids(self):
        class Comment(Document):
            idx = IntField()

        await Comment.adrop_collection()

        # Test with bulk
        comments = [Comment(idx=idx) for idx in range(20)]
        for com in comments:
            assert com.id is None

        returned_comments = await Comment.aobjects.insert(comments, load_bulk=True)

        for com in comments:
            assert isinstance(com.id, ObjectId)

        input_mapping = {com.id: com.idx for com in comments}
        saved_mapping = {com.id: com.idx for com in returned_comments}
        assert input_mapping == saved_mapping

        await Comment.adrop_collection()

        # Test with just one
        comment = Comment(idx=0)
        inserted_comment_id = await Comment.aobjects.insert(comment, load_bulk=False)
        assert comment.id == inserted_comment_id

    async def test_bulk_insert_accepts_doc_with_ids(self):
        class Comment(Document):
            id = IntField(primary_key=True)

        await Comment.adrop_collection()

        com1 = Comment(id=0)
        com2 = Comment(id=1)
        await Comment.aobjects.insert([com1, com2])

    async def test_insert_raise_if_duplicate_in_constraint(self):
        class Comment(Document):
            id = IntField(primary_key=True)

        await Comment.adrop_collection()

        com1 = Comment(id=0)

        await Comment.aobjects.insert(com1)

        with pytest.raises(NotUniqueError):
            await Comment.aobjects.insert(com1)

    async def test_get_changed_fields_query_count(self):
        """Make sure we don't perform unnecessary db operations when
        none of document's fields were updated.
        """

        class Project(Document):
            name = StringField()

        class Person(Document):
            name = StringField()
            owns = ListField(ReferenceField("Organization"))
            projects = ListField(ReferenceField("Project"))

        class Organization(Document):
            name = StringField()
            owner = ReferenceField(Person)
            employees = ListField(ReferenceField(Person))

        await Person.adrop_collection()
        await Organization.adrop_collection()
        await Project.adrop_collection()

        r1 = await Project(name="r1").asave()
        r2 = await Project(name="r2").asave()
        r3 = await Project(name="r3").asave()
        p1 = await Person(name="p1", projects=[r1, r2]).asave()
        p2 = await Person(name="p2", projects=[r2, r3]).asave()
        o1 = await Organization(name="o1", employees=[p1]).asave()

        async with async_query_counter() as q:
            assert q.eq(0)

            # Fetching a document should result in a query.
            org = await Organization.aobjects.get(id=o1.id)
            assert await q.eq(1)

            # Checking changed fields of a newly fetched document should not
            # result in a query.
            org._get_changed_fields()
            assert await q.eq(1)

        # Saving a doc without changing any of its fields should not result
        # in a query (with or without cascade=False).
        org = await Organization.aobjects.get(id=o1.id)
        async with async_query_counter() as q:
            await org.asave()
            assert await q.eq(0)

        org = await Organization.aobjects.get(id=o1.id)
        async with async_query_counter() as q:
            await org.asave(cascade=False)
            assert await q.eq(0)

        # Saving a doc after you append a reference to it should result in
        org = await Organization.aobjects.get(id=o1.id)
        async with async_query_counter() as q:
            employees = await org.employees
            employees.append(p2)
            org.employees = employees
            await org.asave()  # saves the org
            assert await q.eq(1)

    async def test_repeated_iteration(self):
        """Ensure that QuerySet rewinds itself one iteration finishes."""
        await self.Person(name="Person 1").asave()
        await self.Person(name="Person 2").asave()

        queryset = self.Person.aobjects
        people1 = [person async for person in queryset]
        people2 = [person async for person in queryset]

        # Check that it still works even if iteration is interrupted.
        async for _person in queryset:
            break
        people3 = [person async for person in queryset]

        assert people1 == people2
        assert people1 == people3

    async def test_regex_query_shortcuts(self):
        """Ensure that contains, startswith, endswith, etc work."""
        person = self.Person(name="Guido van Rossum")
        await person.asave()

        # Test contains
        obj = await self.Person.aobjects(name__contains="van").first()
        assert obj == person
        obj = await self.Person.aobjects(name__contains="Van").first()
        assert obj is None

        # Test icontains
        obj = await self.Person.aobjects(name__icontains="Van").first()
        assert obj == person

        # Test startswith
        obj = await self.Person.aobjects(name__startswith="Guido").first()
        assert obj == person
        obj = await self.Person.aobjects(name__startswith="guido").first()
        assert obj is None

        # Test istartswith
        obj = await self.Person.aobjects(name__istartswith="guido").first()
        assert obj == person

        # Test endswith
        obj = await self.Person.aobjects(name__endswith="Rossum").first()
        assert obj == person
        obj = await self.Person.aobjects(name__endswith="rossuM").first()
        assert obj is None

        # Test iendswith
        obj = await self.Person.aobjects(name__iendswith="rossuM").first()
        assert obj == person

        # Test exact
        obj = await self.Person.aobjects(name__exact="Guido van Rossum").first()
        assert obj == person
        obj = await self.Person.aobjects(name__exact="Guido van rossum").first()
        assert obj is None
        obj = await self.Person.aobjects(name__exact="Guido van Rossu").first()
        assert obj is None

        # Test iexact
        obj = await self.Person.aobjects(name__iexact="gUIDO VAN rOSSUM").first()
        assert obj == person
        obj = await self.Person.aobjects(name__iexact="gUIDO VAN rOSSU").first()
        assert obj is None

        # Test wholeword
        obj = await self.Person.aobjects(name__wholeword="Guido").first()
        assert obj == person
        obj = await self.Person.aobjects(name__wholeword="rossum").first()
        assert obj is None
        obj = await self.Person.aobjects(name__wholeword="Rossu").first()
        assert obj is None

        # Test iwholeword
        obj = await self.Person.aobjects(name__iwholeword="rOSSUM").first()
        assert obj == person
        obj = await self.Person.aobjects(name__iwholeword="rOSSU").first()
        assert obj is None

        # Test regex
        obj = await self.Person.aobjects(name__regex="^[Guido].*[Rossum]$").first()
        assert obj == person
        obj = await self.Person.aobjects(name__regex="^[guido].*[rossum]$").first()
        assert obj is None
        obj = await self.Person.aobjects(name__regex="^[uido].*[Rossum]$").first()
        assert obj is None

        # Test iregex
        obj = await self.Person.aobjects(name__iregex="^[guido].*[rossum]$").first()
        assert obj == person
        obj = await self.Person.aobjects(name__iregex="^[Uido].*[Rossum]$").first()
        assert obj is None

        # Test unsafe expressions
        person = self.Person(name="Guido van Rossum [.'Geek']")
        await person.asave()

        obj = await self.Person.aobjects(name__icontains="[.'Geek").first()
        assert obj == person

    async def test_not(self):
        """Ensure that the __not operator works as expected."""
        alice = self.Person(name="Alice", age=25)
        await alice.asave()

        obj = await self.Person.aobjects(name__iexact="alice").first()
        assert obj == alice

        obj = await self.Person.aobjects(name__not__iexact="alice").first()
        assert obj is None

    async def test_filter_chaining(self):
        """Ensure filters can be chained together."""

        class Blog(Document):
            id = StringField(primary_key=True)

        class BlogPost(Document):
            blog = ReferenceField(Blog)
            title = StringField()
            is_published = BooleanField()
            published_date = DateTimeField()

            @queryset_manager(queryset=AsyncQuerySet)
            def published(doc_cls, queryset):
                return queryset(is_published=True)

        await Blog.adrop_collection()
        await BlogPost.adrop_collection()

        blog_1 = Blog(id="1")
        blog_2 = Blog(id="2")
        blog_3 = Blog(id="3")

        await blog_1.asave()
        await blog_2.asave()
        await blog_3.asave()

        await BlogPost.aobjects.create(
            blog=blog_1,
            title="Blog Post #1",
            is_published=True,
            published_date=datetime.datetime(2010, 1, 5, 0, 0, 0),
        )
        await BlogPost.aobjects.create(
            blog=blog_2,
            title="Blog Post #2",
            is_published=True,
            published_date=datetime.datetime(2010, 1, 6, 0, 0, 0),
        )
        await BlogPost.aobjects.create(
            blog=blog_3,
            title="Blog Post #3",
            is_published=True,
            published_date=datetime.datetime(2010, 1, 7, 0, 0, 0),
        )

        # find all published blog posts before 2010-01-07
        published_posts = BlogPost.published()
        published_posts = published_posts.filter(
            published_date__lt=datetime.datetime(2010, 1, 7, 0, 0, 0)
        )
        assert await published_posts.count() == 2

        blog_posts = BlogPost.aobjects
        blog_posts = blog_posts.filter(blog__in=[blog_1, blog_2])
        blog_posts = blog_posts.filter(blog=blog_3)
        assert await blog_posts.count() == 0

        await BlogPost.adrop_collection()
        await Blog.adrop_collection()

    async def test_filter_chaining_with_regex(self):
        person = self.Person(name="Guido van Rossum")
        await person.asave()

        people = self.Person.aobjects
        people = (
            people.filter(name__startswith="Gui")
            .filter(name__not__endswith="tum")
            .filter(name__icontains="VAN")
            .filter(name__regex="^Guido")
            .filter(name__wholeword="Guido")
            .filter(name__wholeword="van")
        )
        assert await people.count() == 1

    async def assertSequence(self, qs, expected):
        qs = await qs.to_list()
        expected = list(expected)
        assert len(qs) == len(expected)
        for i in range(len(qs)):
            assert qs[i] == expected[i]

    async def test_ordering(self):
        """Ensure default ordering is applied and can be overridden."""

        class BlogPost(Document):
            title = StringField()
            published_date = DateTimeField()

            meta = {"ordering": ["-published_date"]}

        await BlogPost.adrop_collection()

        blog_post_1 = await BlogPost.aobjects.create(
            title="Blog Post #1", published_date=datetime.datetime(2010, 1, 5, 0, 0, 0)
        )
        blog_post_2 = await BlogPost.aobjects.create(
            title="Blog Post #2", published_date=datetime.datetime(2010, 1, 6, 0, 0, 0)
        )
        blog_post_3 = await BlogPost.aobjects.create(
            title="Blog Post #3", published_date=datetime.datetime(2010, 1, 7, 0, 0, 0)
        )

        # get the "first" BlogPost using default ordering
        # from BlogPost.meta.ordering
        expected = [blog_post_3, blog_post_2, blog_post_1]
        await self.assertSequence(BlogPost.aobjects.all(), expected)

        # override default ordering, order BlogPosts by "published_date"
        qs = BlogPost.aobjects.order_by("+published_date")
        expected = [blog_post_1, blog_post_2, blog_post_3]
        await self.assertSequence(qs, expected)

    async def test_clear_ordering(self):
        """Ensure that the default ordering can be cleared by calling
        order_by() w/o any arguments.
        """
        ORDER_BY_KEY, CMD_QUERY_KEY = get_key_compat(self.mongodb_version)

        class BlogPost(Document):
            title = StringField()
            published_date = DateTimeField()

            meta = {"ordering": ["-published_date"]}

        await BlogPost.adrop_collection()

        # default ordering should be used by default
        async with async_db_ops_tracker() as q:
            await BlogPost.aobjects.filter(title="whatever").first()
            assert len(await q.get_ops()) == 1
            assert (await q.get_ops())[0][CMD_QUERY_KEY][ORDER_BY_KEY] == {"published_date": -1}

        # calling order_by() should clear the default ordering
        async with async_db_ops_tracker() as q:
            await BlogPost.aobjects.filter(title="whatever").order_by().first()
            assert len(await q.get_ops()) == 1
            assert ORDER_BY_KEY not in (await q.get_ops())[0][CMD_QUERY_KEY]

        # calling an explicit order_by should use a specified sort
        async with async_db_ops_tracker() as q:
            await BlogPost.aobjects.filter(title="whatever").order_by("published_date").first()
            assert len(await q.get_ops()) == 1
            assert (await q.get_ops())[0][CMD_QUERY_KEY][ORDER_BY_KEY] == {"published_date": 1}

        # calling order_by() after an explicit sort should clear it
        async with async_db_ops_tracker() as q:
            qs = BlogPost.aobjects.filter(title="whatever").order_by("published_date")
            await qs.order_by().first()
            assert len(await q.get_ops()) == 1
            assert ORDER_BY_KEY not in (await q.get_ops())[0][CMD_QUERY_KEY]

    async def test_no_ordering_for_get(self):
        """Ensure that Doc.aobjects.get doesn't use any ordering."""
        ORDER_BY_KEY, CMD_QUERY_KEY = get_key_compat(self.mongodb_version)

        class BlogPost(Document):
            title = StringField()
            published_date = DateTimeField()

            meta = {"ordering": ["-published_date"]}

        await BlogPost.adrop_collection()
        await BlogPost.aobjects.create(
            title="whatever", published_date=datetime.datetime.now(datetime.UTC)
        )

        async with async_db_ops_tracker() as q:
            await BlogPost.aobjects.get(title="whatever")
            assert len(await q.get_ops()) == 1
            assert ORDER_BY_KEY not in (await q.get_ops())[0][CMD_QUERY_KEY]

        # Ordering should be ignored for .get even if we set it explicitly
        async with async_db_ops_tracker() as q:
            await BlogPost.aobjects.order_by("-title").get(title="whatever")
            assert len(await q.get_ops()) == 1
            assert ORDER_BY_KEY not in (await q.get_ops())[0][CMD_QUERY_KEY]

    async def test_find_embedded(self):
        """Ensure that an embedded document is properly returned from
        different manners of querying.
        """

        class User(EmbeddedDocument):
            name = StringField()

        class BlogPost(Document):
            content = StringField()
            author = EmbeddedDocumentField(User)

        await BlogPost.adrop_collection()

        user = User(name="Test User")
        await BlogPost.aobjects.create(author=user, content="Had a good coffee today...")

        result = await BlogPost.aobjects.first()
        assert isinstance(result.author, User)
        assert result.author.name == "Test User"

        result = await BlogPost.aobjects.get(author__name=user.name)
        assert isinstance(result.author, User)
        assert result.author.name == "Test User"

        result = await BlogPost.aobjects.get(author={"name": user.name})
        assert isinstance(result.author, User)
        assert result.author.name == "Test User"

        # Fails, since the string is not a type that is able to represent the
        # author's document structure (should be dict)
        with pytest.raises(InvalidQueryError):
            await BlogPost.aobjects.get(author=user.name)

    async def test_find_empty_embedded(self):
        """Ensure that you can save and find an empty embedded document."""

        class User(EmbeddedDocument):
            name = StringField()

        class BlogPost(Document):
            content = StringField()
            author = EmbeddedDocumentField(User)

        await BlogPost.adrop_collection()

        await BlogPost.aobjects.create(content="Anonymous post...")

        result = await BlogPost.aobjects.get(author=None)
        assert result.author is None

    async def test_find_dict_item(self):
        """Ensure that DictField items may be found."""

        class BlogPost(Document):
            info = DictField()

        await BlogPost.adrop_collection()

        post = BlogPost(info={"title": "test"})
        await post.asave()

        post_obj = await BlogPost.aobjects(info__title="test").first()
        assert post_obj.id == post.id

        await BlogPost.adrop_collection()

    async def test_delete(self):
        """Ensure that documents are properly deleted from the database."""
        await self.Person(name="User A", age=20).asave()
        await self.Person(name="User B", age=30).asave()
        await self.Person(name="User C", age=40).asave()

        assert await self.Person.aobjects.count() == 3

        await self.Person.aobjects(age__lt=30).delete()
        assert await self.Person.aobjects.count() == 2

        await self.Person.aobjects.delete()
        assert await self.Person.aobjects.count() == 0

    async def test_reverse_delete_rule_cascade(self):
        """Ensure cascading deletion of referring documents from the database."""

        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(self.Person, reverse_delete_rule=CASCADE)

        await BlogPost.adrop_collection()

        me = self.Person(name="Test User")
        await me.asave()
        someoneelse = self.Person(name="Some-one Else")
        await someoneelse.asave()

        await BlogPost(content="Watching TV", author=me).asave()
        await BlogPost(content="Chilling out", author=me).asave()
        await BlogPost(content="Pro Testing", author=someoneelse).asave()

        assert 3 == await BlogPost.aobjects.count()
        await self.Person.aobjects(name="Test User").delete()
        assert 1 == await BlogPost.aobjects.count()

    async def test_reverse_delete_rule_cascade_on_abstract_document(self):
        """Ensure cascading deletion of referring documents from the database
        does not fail on abstract document.
        """

        class AbstractBlogPost(Document):
            meta = {"abstract": True}
            author = ReferenceField(self.Person, reverse_delete_rule=CASCADE)

        class BlogPost(AbstractBlogPost):
            content = StringField()

        await BlogPost.adrop_collection()

        me = self.Person(name="Test User")
        await me.asave()
        someoneelse = self.Person(name="Some-one Else")
        await someoneelse.asave()

        await BlogPost(content="Watching TV", author=me).asave()
        await BlogPost(content="Chilling out", author=me).asave()
        await BlogPost(content="Pro Testing", author=someoneelse).asave()

        assert 3 == await BlogPost.aobjects.count()
        await self.Person.aobjects(name="Test User").delete()
        assert 1 == await BlogPost.aobjects.count()

    async def test_reverse_delete_rule_cascade_cycle(self):
        """Ensure reference cascading doesn't loop if reference graph isn't
        a tree
        """

        class Dummy(Document):
            reference = ReferenceField("self", reverse_delete_rule=CASCADE)

        base = await Dummy().asave()
        other = await Dummy(reference=base).asave()
        base.reference = other
        await base.asave()

        await base.adelete()

        with pytest.raises(DoesNotExist):
            await base.areload()
        with pytest.raises(DoesNotExist):
            await other.areload()

    async def test_reverse_delete_rule_cascade_complex_cycle(self):
        """Ensure reference cascading doesn't loop if reference graph isn't
        a tree
        """

        class Category(Document):
            name = StringField()

        class Dummy(Document):
            reference = ReferenceField("self", reverse_delete_rule=CASCADE)
            cat = ReferenceField(Category, reverse_delete_rule=CASCADE)

        cat = await Category(name="cat").asave()
        base = await Dummy(cat=cat).asave()
        other = await Dummy(reference=base).asave()
        other2 = await Dummy(reference=other).asave()
        base.reference = other
        base.asave()

        await cat.adelete()

        with pytest.raises(DoesNotExist):
            await base.areload()
        with pytest.raises(DoesNotExist):
            await other.areload()
        with pytest.raises(DoesNotExist):
            await other2.areload()

    async def test_reverse_delete_rule_cascade_self_referencing(self):
        """Ensure self-referencing CASCADE deletes do not result in infinite
        loop
        """

        class Category(Document):
            name = StringField()
            parent = ReferenceField("self", reverse_delete_rule=CASCADE)

        await Category.adrop_collection()

        num_children = 3
        base = Category(name="Root")
        await base.asave()

        # Create a simple parent-child tree
        for i in range(num_children):
            child_name = "Child-%i" % i
            child = Category(name=child_name, parent=base)
            await child.asave()

            for i in range(num_children):
                child_child_name = "Child-Child-%i" % i
                child_child = Category(name=child_child_name, parent=child)
                await child_child.asave()

        tree_size = 1 + num_children + (num_children * num_children)
        assert tree_size == await Category.aobjects.count()
        assert num_children == await Category.aobjects(parent=base).count()

        # The delete should effectively wipe out the Category collection
        # without resulting in infinite parent-child cascade recursion
        await base.adelete()
        assert 0 == await Category.aobjects.count()

    async def test_reverse_delete_rule_nullify(self):
        """Ensure nullification of references to deleted documents."""

        class Category(Document):
            name = StringField()

        class BlogPost(Document):
            content = StringField()
            category = ReferenceField(Category, reverse_delete_rule=NULLIFY)

        await BlogPost.adrop_collection()
        await Category.adrop_collection()

        lameness = Category(name="Lameness")
        await lameness.asave()

        post = BlogPost(content="Watching TV", category=lameness)
        await post.asave()

        assert await BlogPost.aobjects.count() == 1
        blog = await BlogPost.aobjects.select_related("category").first()
        assert (blog.category).name == "Lameness"
        await Category.aobjects.delete()
        assert await BlogPost.aobjects.count() == 1
        assert (await BlogPost.aobjects.select_related("category").first()).category is None

    async def test_reverse_delete_rule_nullify_on_abstract_document(self):
        """Ensure nullification of references to deleted documents when
        reference is on an abstract document.
        """

        class AbstractBlogPost(Document):
            meta = {"abstract": True}
            author = ReferenceField(self.Person, reverse_delete_rule=NULLIFY)

        class BlogPost(AbstractBlogPost):
            content = StringField()

        await BlogPost.adrop_collection()

        me = self.Person(name="Test User")
        await me.asave()
        someoneelse = self.Person(name="Some-one Else")
        await someoneelse.asave()

        await BlogPost(content="Watching TV", author=me).asave()

        assert await BlogPost.aobjects.count() == 1
        assert (await BlogPost.aobjects.first()).author == me
        await self.Person.aobjects(name="Test User").delete()
        assert await BlogPost.aobjects.count() == 1
        assert (await BlogPost.aobjects.first()).author is None

    async def test_reverse_delete_rule_deny(self):
        """Ensure deletion gets denied on documents that still have references
        to them.
        """

        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(self.Person, reverse_delete_rule=DENY)

        await BlogPost.adrop_collection()
        await self.Person.adrop_collection()

        me = self.Person(name="Test User")
        await me.asave()

        post = BlogPost(content="Watching TV", author=me)
        await post.asave()

        with pytest.raises(OperationError):
            await self.Person.aobjects.delete()

    async def test_reverse_delete_rule_deny_on_abstract_document(self):
        """Ensure deletion gets denied on documents that still have references
        to them, when reference is on an abstract document.
        """

        class AbstractBlogPost(Document):
            meta = {"abstract": True}
            author = ReferenceField(self.Person, reverse_delete_rule=DENY)

        class BlogPost(AbstractBlogPost):
            content = StringField()

        await BlogPost.adrop_collection()

        me = self.Person(name="Test User")
        await me.asave()

        await BlogPost(content="Watching TV", author=me).asave()

        assert 1 == await BlogPost.aobjects.count()
        with pytest.raises(OperationError):
            await self.Person.aobjects.delete()

    async def test_reverse_delete_rule_pull(self):
        """Ensure pulling of references to deleted documents."""

        class BlogPost(Document):
            content = StringField()
            authors = ListField(ReferenceField(self.Person, reverse_delete_rule=PULL))

        await BlogPost.adrop_collection()
        await self.Person.adrop_collection()

        me = self.Person(name="Test User")
        await me.asave()

        someoneelse = self.Person(name="Some-one Else")
        await someoneelse.asave()

        post = BlogPost(content="Watching TV", authors=[me, someoneelse])
        await post.asave()

        another = BlogPost(content="Chilling Out", authors=[someoneelse])
        await another.asave()

        await someoneelse.adelete()
        await post.areload()
        await another.areload()

        assert await post.authors == [me]
        assert await another.authors == []

    async def test_reverse_delete_rule_pull_on_abstract_documents(self):
        """Ensure pulling of references to deleted documents when reference
        is defined on an abstract document..
        """

        class AbstractBlogPost(Document):
            meta = {"abstract": True}
            authors = ListField(ReferenceField(self.Person, reverse_delete_rule=PULL))

        class BlogPost(AbstractBlogPost):
            content = StringField()

        await BlogPost.adrop_collection()
        await self.Person.adrop_collection()

        me = self.Person(name="Test User")
        await me.asave()

        someoneelse = self.Person(name="Some-one Else")
        await someoneelse.asave()

        post = BlogPost(content="Watching TV", authors=[me, someoneelse])
        await post.asave()

        another = BlogPost(content="Chilling Out", authors=[someoneelse])
        await another.asave()

        await someoneelse.adelete()
        await post.areload()
        await another.areload()

        assert await post.authors == [me]
        assert await another.authors == []

    async def test_delete_with_limits(self):
        class Log(Document):
            pass

        await Log.adrop_collection()

        for i in range(10):
            await Log().asave()

        await Log.aobjects().skip(3).limit(2).delete()
        assert 8 == await Log.aobjects.count()

    async def test_delete_with_limit_handles_delete_rules(self):
        """Ensure cascading deletion of referring documents from the database."""

        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(self.Person, reverse_delete_rule=CASCADE)

        await BlogPost.adrop_collection()

        me = self.Person(name="Test User")
        await me.asave()
        someoneelse = self.Person(name="Some-one Else")
        await someoneelse.asave()

        await BlogPost(content="Watching TV", author=me).asave()
        await BlogPost(content="Chilling out", author=me).asave()
        await BlogPost(content="Pro Testing", author=someoneelse).asave()

        assert 3 == await BlogPost.aobjects.count()
        await self.Person.aobjects().limit(1).delete()
        assert 1 == await BlogPost.aobjects.count()

    async def test_delete_edge_case_with_write_concern_0_return_None(self):
        """Return None if the delete operation is unacknowledged.

        If we use an unack'd write concern, we don't really know how many
        documents have been deleted.
        """
        p1 = await self.Person(name="User Z", age=20).asave()
        del_result = await p1.adelete(w=0)
        assert del_result is None

    async def test_reference_field_find(self):
        """Ensure cascading deletion of referring documents from the database."""

        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(self.Person)

        await BlogPost.adrop_collection()
        await self.Person.adrop_collection()

        me = await self.Person(name="Test User").asave()
        await BlogPost(content="test 123", author=me).asave()

        assert 1 == await BlogPost.aobjects(author=me).count()
        assert 1 == await BlogPost.aobjects(author=me.pk).count()
        assert 1 == await BlogPost.aobjects(author="%s" % me.pk).count()

        assert 1 == await BlogPost.aobjects(author__in=[me]).count()
        assert 1 == await BlogPost.aobjects(author__in=[me.pk]).count()
        assert 1 == await BlogPost.aobjects(author__in=["%s" % me.pk]).count()

    async def test_reference_field_find_dbref(self):
        """Ensure cascading deletion of referring documents from the database."""

        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(self.Person, dbref=True)

        await BlogPost.adrop_collection()
        await self.Person.adrop_collection()

        me = await self.Person(name="Test User").asave()
        await BlogPost(content="test 123", author=me).asave()

        assert 1 == await BlogPost.aobjects(author=me).count()
        assert 1 == await BlogPost.aobjects(author=me.pk).count()
        assert 1 == await BlogPost.aobjects(author="%s" % me.pk).count()

        assert 1 == await BlogPost.aobjects(author__in=[me]).count()
        assert 1 == await BlogPost.aobjects(author__in=[me.pk]).count()
        assert 1 == await BlogPost.aobjects(author__in=["%s" % me.pk]).count()

    async def test_update_intfield_operator(self):
        class BlogPost(Document):
            hits = IntField()

        await BlogPost.adrop_collection()

        post = BlogPost(hits=5)
        await post.asave()

        await BlogPost.aobjects.update_one(set__hits=10)
        await post.areload()
        assert post.hits == 10

        await BlogPost.aobjects.update_one(inc__hits=1)
        await post.areload()
        assert post.hits == 11

        await BlogPost.aobjects.update_one(dec__hits=1)
        await post.areload()
        assert post.hits == 10

        # Negative dec operator is equal to a positive inc operator
        await BlogPost.aobjects.update_one(dec__hits=-1)
        await post.areload()
        assert post.hits == 11

    async def test_update_decimalfield_operator(self):
        class BlogPost(Document):
            review = DecimalField()

        await BlogPost.adrop_collection()

        post = BlogPost(review=3.5)
        await post.asave()

        await BlogPost.aobjects.update_one(inc__review=0.1)  # test with floats
        await post.areload()
        assert float(post.review) == 3.6

        await BlogPost.aobjects.update_one(dec__review=0.1)
        await post.areload()
        assert float(post.review) == 3.5

        await BlogPost.aobjects.update_one(inc__review=Decimal(0.12))  # test with Decimal
        await post.areload()
        assert float(post.review) == 3.62

        await BlogPost.aobjects.update_one(dec__review=Decimal(0.12))
        await post.areload()
        assert float(post.review) == 3.5

    async def test_update_decimalfield_operator_not_working_with_force_string(self):
        class BlogPost(Document):
            review = DecimalField(force_string=True)

        await BlogPost.adrop_collection()

        post = BlogPost(review=3.5)
        await post.asave()

        with pytest.raises(OperationError):
            await BlogPost.aobjects.update_one(inc__review=0.1)  # test with floats

    async def test_update_listfield_operator(self):
        """Ensure that atomic updates work properly."""

        class BlogPost(Document):
            tags = ListField(StringField())

        await BlogPost.adrop_collection()

        post = BlogPost(tags=["test"])
        await post.asave()

        # ListField operator
        await BlogPost.aobjects.update(push__tags="mongo")
        await post.areload()
        assert "mongo" in post.tags

        await BlogPost.aobjects.update_one(push_all__tags=["db", "nosql"])
        await post.areload()
        assert "db" in post.tags
        assert "nosql" in post.tags

        tags = post.tags[:-1]
        await BlogPost.aobjects.update(pop__tags=1)
        await post.areload()
        assert post.tags == tags

        await BlogPost.aobjects.update_one(add_to_set__tags="unique")
        await BlogPost.aobjects.update_one(add_to_set__tags="unique")
        await post.areload()
        assert post.tags.count("unique") == 1

        await BlogPost.adrop_collection()

    async def test_update_unset(self):
        class BlogPost(Document):
            title = StringField()

        await BlogPost.adrop_collection()

        post = await BlogPost(title="garbage").asave()

        assert post.title is not None
        await BlogPost.aobjects.update_one(unset__title=1)
        await post.areload()
        assert post.title is None
        pymongo_doc = await BlogPost.aobjects.as_pymongo().first()
        assert "title" not in pymongo_doc

    async def test_update_push_with_position(self):
        """Ensure that the 'push' update with position works properly."""

        class BlogPost(Document):
            slug = StringField()
            tags = ListField(StringField())

        await BlogPost.adrop_collection()

        post = await BlogPost.aobjects.create(slug="test")

        await BlogPost.aobjects.filter(id=post.id).update(push__tags="code")
        await BlogPost.aobjects.filter(id=post.id).update(push__tags__0=["mongodb", "python"])
        await post.areload()
        assert post.tags == ["mongodb", "python", "code"]

        await BlogPost.aobjects.filter(id=post.id).update(set__tags__2="java")
        await post.areload()
        assert post.tags == ["mongodb", "python", "java"]

        # test push with singular value
        await BlogPost.aobjects.filter(id=post.id).update(push__tags__0="scala")
        await post.areload()
        assert post.tags == ["scala", "mongodb", "python", "java"]

    async def test_update_push_list_of_list(self):
        """Ensure that the 'push' update operation works in the list of list"""

        class BlogPost(Document):
            slug = StringField()
            tags = ListField()

        await BlogPost.adrop_collection()

        post = await BlogPost(slug="test").asave()

        await BlogPost.aobjects.filter(slug="test").update(push__tags=["value1", 123])
        await post.areload()
        assert post.tags == [["value1", 123]]

    async def test_update_push_and_pull_add_to_set(self):
        """Ensure that the 'pull' update operation works correctly."""

        class BlogPost(Document):
            slug = StringField()
            tags = ListField(StringField())

        await BlogPost.adrop_collection()

        post = BlogPost(slug="test")
        await post.asave()

        await BlogPost.aobjects.filter(id=post.id).update(push__tags="code")
        await post.areload()
        assert post.tags == ["code"]

        await BlogPost.aobjects.filter(id=post.id).update(push_all__tags=["mongodb", "code"])
        await post.areload()
        assert post.tags == ["code", "mongodb", "code"]

        await BlogPost.aobjects(slug="test").update(pull__tags="code")
        await post.areload()
        assert post.tags == ["mongodb"]

        await BlogPost.aobjects(slug="test").update(pull_all__tags=["mongodb", "code"])
        await post.areload()
        assert post.tags == []

        await BlogPost.aobjects(slug="test").update(
            __raw__={"$addToSet": {"tags": {"$each": ["code", "mongodb", "code"]}}}
        )
        await post.areload()
        assert post.tags == ["code", "mongodb"]

    async def test_aggregation_update(self):
        """Ensure that the 'aggregation_update' update works correctly."""

        class BlogPost(Document):
            slug = StringField()
            tags = ListField(StringField())

        await BlogPost.adrop_collection()

        post = BlogPost(slug="test")
        await post.asave()

        await BlogPost.aobjects(slug="test").update(
            __raw__=[{"$set": {"slug": {"$concat": ["$slug", " ", "$slug"]}}}],
        )
        await post.areload()
        assert post.slug == "test test"

        await BlogPost.aobjects(slug="test test").update(
            __raw__=[
                {"$set": {"slug": {"$concat": ["$slug", " ", "it"]}}},  # test test it
                {
                    "$set": {"slug": {"$concat": ["When", " ", "$slug"]}}
                },  # When test test it
            ],
        )
        await post.areload()
        assert post.slug == "When test test it"

    async def test_combination_of_mongoengine_and__raw__(self):
        """Ensure that the '__raw__' update/query works in combination with mongoengine syntax correctly."""

        class BlogPost(Document):
            slug = StringField()
            foo = StringField()
            tags = ListField(StringField())

        await BlogPost.adrop_collection()

        post = BlogPost(slug="test", foo="bar")
        await post.asave()

        await BlogPost.aobjects(slug="test").update(
            foo="baz",
            __raw__={"$set": {"slug": "test test"}},
        )
        await post.areload()
        assert post.slug == "test test"
        assert post.foo == "baz"

        assert await BlogPost.aobjects(foo="baz", __raw__={"slug": "test test"}).count() == 1
        assert (
                await BlogPost.aobjects(foo__ne="bar", __raw__={"slug": {"$ne": "test"}}).count()
                == 1
        )
        assert (
                await BlogPost.aobjects(foo="baz", __raw__={"slug": {"$ne": "test test"}}).count()
                == 0
        )
        assert (
                await BlogPost.aobjects(foo__ne="baz", __raw__={"slug": "test test"}).count() == 0
        )
        assert (
                await BlogPost.aobjects(
                    foo__ne="baz", __raw__={"slug": {"$ne": "test test"}}
                ).count()
                == 0
        )

    async def test_add_to_set_each(self):
        class Item(Document):
            name = StringField(required=True)
            description = StringField(max_length=50)
            parents = ListField(ReferenceField("self"))

        await Item.adrop_collection()

        item = await Item(name="test item").asave()
        parent_1 = await Item(name="parent 1").asave()
        parent_2 = await Item(name="parent 2").asave()

        await item.aupdate(add_to_set__parents=[parent_1, parent_2, parent_1])
        await item.areload()

        assert [parent_1, parent_2] == item.parents

    async def test_pull_nested(self):
        class Collaborator(EmbeddedDocument):
            user = StringField()

            def __unicode__(self):
                return "%s" % self.user

        class Site(Document):
            name = StringField(max_length=75, unique=True, required=True)
            collaborators = ListField(EmbeddedDocumentField(Collaborator))

        await Site.adrop_collection()

        c = Collaborator(user="Esteban")
        s = await Site(name="test", collaborators=[c]).asave()

        await Site.aobjects(id=s.id).update_one(pull__collaborators__user="Esteban")
        assert (await Site.aobjects.first()).collaborators == []

        with pytest.raises(InvalidQueryError):
            await Site.aobjects(id=s.id).update_one(pull_all__collaborators__user=["Ross"])

    async def test_pull_from_nested_embedded(self):
        class User(EmbeddedDocument):
            name = StringField()

            def __unicode__(self):
                return "%s" % self.name

        class Collaborator(EmbeddedDocument):
            helpful = ListField(EmbeddedDocumentField(User))
            unhelpful = ListField(EmbeddedDocumentField(User))

        class Site(Document):
            name = StringField(max_length=75, unique=True, required=True)
            collaborators = EmbeddedDocumentField(Collaborator)

        await Site.adrop_collection()

        c = User(name="Esteban")
        f = User(name="Frank")
        s = await Site(
            name="test", collaborators=Collaborator(helpful=[c], unhelpful=[f])
        ).asave()

        await Site.aobjects(id=s.id).update_one(pull__collaborators__helpful=c)
        assert (await Site.aobjects.first()).collaborators["helpful"] == []

        await Site.aobjects(id=s.id).update_one(
            pull__collaborators__unhelpful={"name": "Frank"}
        )
        assert (await Site.aobjects.first()).collaborators["unhelpful"] == []

        with pytest.raises(InvalidQueryError):
            await Site.aobjects(id=s.id).update_one(
                pull_all__collaborators__helpful__name=["Ross"]
            )

    async def test_pull_from_nested_embedded_using_in_nin(self):
        """Ensure that the 'pull' update operation works on embedded documents using 'in' and 'nin' operators."""

        class User(EmbeddedDocument):
            name = StringField()

            def __unicode__(self):
                return "%s" % self.name

        class Collaborator(EmbeddedDocument):
            helpful = ListField(EmbeddedDocumentField(User))
            unhelpful = ListField(EmbeddedDocumentField(User))

        class Site(Document):
            name = StringField(max_length=75, unique=True, required=True)
            collaborators = EmbeddedDocumentField(Collaborator)

        await Site.adrop_collection()

        a = User(name="Esteban")
        b = User(name="Frank")
        x = User(name="Harry")
        y = User(name="John")

        s = await Site(
            name="test", collaborators=Collaborator(helpful=[a, b], unhelpful=[x, y])
        ).asave()

        await Site.aobjects(id=s.id).update_one(
            pull__collaborators__helpful__name__in=["Esteban"]
        )  # Pull a
        assert (await Site.aobjects.first()).collaborators["helpful"] == [b]

        await Site.aobjects(id=s.id).update_one(
            pull__collaborators__unhelpful__name__nin=["John"]
        )  # Pull x
        assert (await Site.aobjects.first()).collaborators["unhelpful"] == [y]

    async def test_pull_from_nested_mapfield(self):
        class Collaborator(EmbeddedDocument):
            user = StringField()

            def __unicode__(self):
                return "%s" % self.user

        class Site(Document):
            name = StringField(max_length=75, unique=True, required=True)
            collaborators = MapField(ListField(EmbeddedDocumentField(Collaborator)))

        await Site.adrop_collection()

        c = Collaborator(user="Esteban")
        f = Collaborator(user="Frank")
        s = Site(name="test", collaborators={"helpful": [c], "unhelpful": [f]})
        await s.asave()

        await Site.aobjects(id=s.id).update_one(pull__collaborators__helpful__user="Esteban")
        assert (await Site.aobjects.first()).collaborators["helpful"] == []

        await Site.aobjects(id=s.id).update_one(
            pull__collaborators__unhelpful={"user": "Frank"}
        )
        assert (await Site.aobjects.first()).collaborators["unhelpful"] == []

        with pytest.raises(InvalidQueryError):
            await Site.aobjects(id=s.id).update_one(
                pull_all__collaborators__helpful__user=["Ross"]
            )

    async def test_pull_in_genericembedded_field(self):
        class Foo(EmbeddedDocument):
            name = StringField()

        class Bar(Document):
            foos = ListField(GenericEmbeddedDocumentField(choices=[Foo]))

        await Bar.adrop_collection()

        foo = Foo(name="bar")
        bar = await Bar(foos=[foo]).asave()
        await Bar.aobjects(id=bar.id).update(pull__foos=foo)
        await bar.areload()
        assert len(bar.foos) == 0

    async def test_update_one_check_return_with_full_result(self):
        class BlogTag(Document):
            name = StringField(required=True)

        await BlogTag.adrop_collection()

        await BlogTag(name="garbage").asave()
        default_update = await BlogTag.aobjects.update_one(name="new")
        assert default_update == 1

        full_result_update = await BlogTag.aobjects.update_one(name="new", full_result=True)
        assert isinstance(full_result_update, UpdateResult)

    async def test_update_one_pop_generic_reference(self):
        class BlogTag(Document):
            name = StringField(required=True)

        class BlogPost(Document):
            slug = StringField()
            tags = ListField(ReferenceField(BlogTag), required=True)

        await BlogPost.adrop_collection()
        await BlogTag.adrop_collection()

        tag_1 = BlogTag(name="code")
        await tag_1.asave()
        tag_2 = BlogTag(name="mongodb")
        await tag_2.asave()

        post = BlogPost(slug="test", tags=[tag_1])
        await post.asave()

        post = BlogPost(slug="test-2", tags=[tag_1, tag_2])
        await post.asave()
        assert len(await post.tags) == 2

        await BlogPost.aobjects(slug="test-2").update_one(pop__tags=-1)

        await post.areload()
        assert len(await post.tags) == 1

        await BlogPost.adrop_collection()
        await BlogTag.adrop_collection()

    async def test_editting_embedded_objects(self):
        class BlogTag(EmbeddedDocument):
            name = StringField(required=True)

        class BlogPost(Document):
            slug = StringField()
            tags = ListField(EmbeddedDocumentField(BlogTag), required=True)

        await BlogPost.adrop_collection()

        tag_1 = BlogTag(name="code")
        tag_2 = BlogTag(name="mongodb")

        post = BlogPost(slug="test", tags=[tag_1])
        await post.asave()

        post = BlogPost(slug="test-2", tags=[tag_1, tag_2])
        await post.asave()
        assert len(post.tags) == 2

        await BlogPost.aobjects(slug="test-2").update_one(set__tags__0__name="python")
        await post.areload()
        assert post.tags[0].name == "python"

        await BlogPost.aobjects(slug="test-2").update_one(pop__tags=-1)
        await post.areload()
        assert len(post.tags) == 1

        await BlogPost.adrop_collection()

    async def test_set_list_embedded_documents(self):
        class Author(EmbeddedDocument):
            name = StringField()

        class Message(Document):
            title = StringField()
            authors = ListField(EmbeddedDocumentField("Author"))

        await Message.adrop_collection()

        message = Message(title="hello", authors=[Author(name="Harry")])
        await message.asave()

        await Message.aobjects(authors__name="Harry").update_one(
            set__authors__S=Author(name="Ross")
        )

        message = await message.areload()
        assert message.authors[0].name == "Ross"

        await Message.aobjects(authors__name="Ross").update_one(
            set__authors=[
                Author(name="Harry"),
                Author(name="Ross"),
                Author(name="Adam"),
            ]
        )

        message = await message.areload()
        assert message.authors[0].name == "Harry"
        assert message.authors[1].name == "Ross"
        assert message.authors[2].name == "Adam"

    async def test_set_generic_embedded_documents(self):
        class Bar(EmbeddedDocument):
            name = StringField()

        class User(Document):
            username = StringField()
            bar = GenericEmbeddedDocumentField(choices=[Bar])

        await User.adrop_collection()

        await User(username="abc").asave()
        await User.aobjects(username="abc").update(set__bar=Bar(name="test"), upsert=True)

        user = await User.aobjects(username="abc").first()
        assert user.bar.name == "test"

    async def test_reload_embedded_docs_instance(self):
        class SubDoc(EmbeddedDocument):
            val = IntField()

        class Doc(Document):
            embedded = EmbeddedDocumentField(SubDoc)

        doc = await Doc(embedded=SubDoc(val=0)).asave()
        await doc.areload()

        assert doc.pk == doc.embedded._instance.pk

    async def test_reload_list_embedded_docs_instance(self):
        class SubDoc(EmbeddedDocument):
            val = IntField()

        class Doc(Document):
            embedded = ListField(EmbeddedDocumentField(SubDoc))

        doc = await Doc(embedded=[SubDoc(val=0)]).asave()
        await doc.areload()

        assert doc.pk == doc.embedded[0]._instance.pk

    async def test_order_by(self):
        """Ensure that QuerySets may be ordered."""
        await self.Person(name="User B", age=40).asave()
        await self.Person(name="User A", age=20).asave()
        await self.Person(name="User C", age=30).asave()

        names = [p.name async for p in self.Person.aobjects.order_by("-age")]
        assert names == ["User B", "User C", "User A"]

        names = [p.name async for p in self.Person.aobjects.order_by("+age")]
        assert names == ["User A", "User C", "User B"]

        names = [p.name async for p in self.Person.aobjects.order_by("age")]
        assert names == ["User A", "User C", "User B"]

        ages = [p.age async for p in self.Person.aobjects.order_by("-name")]
        assert ages == [30, 40, 20]

        ages = [p.age async for p in self.Person.aobjects.order_by()]
        assert ages == [40, 20, 30]

        ages = [p.age async for p in self.Person.aobjects.order_by("")]
        assert ages == [40, 20, 30]

    async def test_order_by_optional(self):
        class BlogPost(Document):
            title = StringField()
            published_date = DateTimeField(required=False)

        await BlogPost.adrop_collection()

        blog_post_3 = await BlogPost.aobjects.create(
            title="Blog Post #3", published_date=datetime.datetime(2010, 1, 6, 0, 0, 0)
        )
        blog_post_2 = await BlogPost.aobjects.create(
            title="Blog Post #2", published_date=datetime.datetime(2010, 1, 5, 0, 0, 0)
        )
        blog_post_4 = await BlogPost.aobjects.create(
            title="Blog Post #4", published_date=datetime.datetime(2010, 1, 7, 0, 0, 0)
        )
        blog_post_1 = await BlogPost.aobjects.create(title="Blog Post #1", published_date=None)

        expected = [blog_post_1, blog_post_2, blog_post_3, blog_post_4]
        await self.assertSequence(BlogPost.aobjects.order_by("published_date"), expected)
        await self.assertSequence(BlogPost.aobjects.order_by("+published_date"), expected)
        expected.reverse()
        await self.assertSequence(BlogPost.aobjects.order_by("-published_date"), expected)

    async def test_order_by_list(self):
        class BlogPost(Document):
            title = StringField()
            published_date = DateTimeField(required=False)

        await BlogPost.adrop_collection()

        blog_post_1 = await BlogPost.aobjects.create(
            title="A", published_date=datetime.datetime(2010, 1, 6, 0, 0, 0)
        )
        blog_post_2 = await BlogPost.aobjects.create(
            title="B", published_date=datetime.datetime(2010, 1, 6, 0, 0, 0)
        )
        blog_post_3 = await BlogPost.aobjects.create(
            title="C", published_date=datetime.datetime(2010, 1, 7, 0, 0, 0)
        )

        qs = BlogPost.aobjects.order_by("published_date", "title")
        expected = [blog_post_1, blog_post_2, blog_post_3]
        await self.assertSequence(qs, expected)

        qs = BlogPost.aobjects.order_by("-published_date", "-title")
        expected.reverse()
        await self.assertSequence(qs, expected)

    async def test_order_by_chaining(self):
        """Ensure that an order_by query chains properly and allows .only()"""
        await self.Person(name="User B", age=40).asave()
        await self.Person(name="User A", age=20).asave()
        await self.Person(name="User C", age=30).asave()

        only_age = self.Person.aobjects.order_by("-age").only("age")

        names = [p.name async for p in only_age]
        ages = [p.age async for p in only_age]

        # The .only('age') clause should mean that all names are None
        assert names == [None, None, None]
        assert ages == [40, 30, 20]

        qs = self.Person.aobjects.all().order_by("-age")
        qs = qs.limit(10)
        ages = [p.age async for p in qs]
        assert ages == [40, 30, 20]

        qs = self.Person.aobjects.all().limit(10)
        qs = qs.order_by("-age")

        ages = [p.age async for p in qs]
        assert ages == [40, 30, 20]

        qs = self.Person.aobjects.all().skip(0)
        qs = qs.order_by("-age")
        ages = [p.age async for p in qs]
        assert ages == [40, 30, 20]

    async def test_order_by_using_raw(self):
        person_a = self.Person(name="User A", age=20)
        await person_a.asave()
        person_b = self.Person(name="User B", age=30)
        await person_b.asave()
        person_c = self.Person(name="User B", age=25)
        await person_c.asave()
        person_d = self.Person(name="User C", age=40)
        await person_d.asave()

        qs = self.Person.aobjects.order_by(__raw__=[("name", pymongo.DESCENDING)])
        assert qs._ordering == [("name", pymongo.DESCENDING)]
        names = [p.name async for p in qs]
        assert names == ["User C", "User B", "User B", "User A"]

        names = [
            (p.name, p.age)
            async for p in self.Person.aobjects.order_by(__raw__=[("name", pymongo.ASCENDING)])
        ]
        assert names == [("User A", 20), ("User B", 30), ("User B", 25), ("User C", 40)]

        if PYMONGO_VERSION >= (4, 4):
            # Pymongo >= 4.4 allow to mix single key with tuples inside the list
            qs = self.Person.aobjects.order_by(
                __raw__=["name", ("age", pymongo.ASCENDING)]
            )
            names = [(p.name, p.age) async for p in qs]
            assert names == [
                ("User A", 20),
                ("User B", 25),
                ("User B", 30),
                ("User C", 40),
            ]

    async def test_order_by_using_raw_and_keys_raises_exception(self):
        with pytest.raises(OperationError):
            self.Person.aobjects.order_by("-name", __raw__=[("age", pymongo.ASCENDING)])

    async def test_confirm_order_by_reference_wont_work(self):
        """Ordering by reference is not possible.  Use map / reduce.. or
        denormalise"""

        class Author(Document):
            author = ReferenceField(self.Person)

        await Author.adrop_collection()

        person_a = self.Person(name="User A", age=20)
        await person_a.asave()
        person_b = self.Person(name="User B", age=40)
        await person_b.asave()
        person_c = self.Person(name="User C", age=30)
        await person_c.asave()

        await Author(author=person_a).asave()
        await Author(author=person_b).asave()
        await Author(author=person_c).asave()

        names = [a.author.name async for a in Author.aobjects.select_related("author").order_by("-author__age")]
        assert names == ["User B", "User C", "User A"]

    async def test_comment(self):
        """Make sure adding a comment to the query gets added to the query"""
        MONGO_VER = self.mongodb_version
        _, CMD_QUERY_KEY = get_key_compat(MONGO_VER)
        QUERY_KEY = "filter"
        COMMENT_KEY = "comment"

        class User(Document):
            age = IntField()

        async with async_db_ops_tracker() as q:
            await User.aobjects.filter(age__gte=18).comment("looking for an adult").first()
            await User.aobjects.comment("looking for an adult").filter(age__gte=18).first()

            ops = await q.get_ops()
            assert len(ops) == 2
            for op in ops:
                assert op[CMD_QUERY_KEY][QUERY_KEY] == {"age": {"$gte": 18}}
                assert op[CMD_QUERY_KEY][COMMENT_KEY] == "looking for an adult"

    async def test_map_reduce(self):
        """Ensure map/reduce is both mapping and reducing."""

        class BlogPost(Document):
            title = StringField()
            tags = ListField(StringField(), db_field="post-tag-list")

        await BlogPost.adrop_collection()

        await BlogPost(title="Post #1", tags=["music", "film", "print"]).asave()
        await BlogPost(title="Post #2", tags=["music", "film"]).asave()
        await BlogPost(title="Post #3", tags=["film", "photography"]).asave()

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
        results = await BlogPost.aobjects.map_reduce(map_f, reduce_f, "myresults")
        results = list(results)
        assert len(results) == 4

        music = list(filter(lambda r: r.key == "music", results))[0]
        assert music.value == 2

        film = list(filter(lambda r: r.key == "film", results))[0]
        assert film.value == 3

        await BlogPost.adrop_collection()

    async def test_map_reduce_with_custom_object_ids(self):
        """Ensure that QuerySet.map_reduce works properly with custom
        primary keys.
        """

        class BlogPost(Document):
            title = StringField(primary_key=True)
            tags = ListField(StringField())

        await BlogPost.adrop_collection()

        post1 = BlogPost(title="Post #1", tags=["mongodb", "mongoengine"])
        post2 = BlogPost(title="Post #2", tags=["django", "mongodb"])
        post3 = BlogPost(title="Post #3", tags=["hitchcock films"])

        await post1.asave()
        await post2.asave()
        await post3.asave()

        assert BlogPost._fields["title"].db_field == "_id"
        assert BlogPost._meta["id_field"] == "title"

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

        results = await BlogPost.aobjects.order_by("_id").map_reduce(
            map_f, reduce_f, "myresults2"
        )

        assert len(results) == 3
        assert (await results[0].aobject).id == post1.id
        assert (await results[1].aobject).id == post2.id
        assert (await results[2].aobject).id == post3.id

        await BlogPost.adrop_collection()

    async def test_map_reduce_custom_output(self):
        """
        Test map/reduce custom output
        """

        class Family(Document):
            id = IntField(primary_key=True)
            log = StringField()

        class Person(Document):
            id = IntField(primary_key=True)
            name = StringField()
            age = IntField()
            family = ReferenceField(Family)

        await Family.adrop_collection()
        await Person.adrop_collection()

        # creating first family
        f1 = Family(id=1, log="Trav 02 de Julho")
        await f1.asave()

        # persons of first family
        await Person(id=1, family=f1, name="Wilson Jr", age=21).asave()
        await Person(id=2, family=f1, name="Wilson Father", age=45).asave()
        await Person(id=3, family=f1, name="Eliana Costa", age=40).asave()
        await Person(id=4, family=f1, name="Tayza Mariana", age=17).asave()

        # creating second family
        f2 = Family(id=2, log="Av prof frasc brunno")
        await f2.asave()

        # persons of second family
        await Person(id=5, family=f2, name="Isabella Luanna", age=16).asave()
        await Person(id=6, family=f2, name="Sandra Mara", age=36).asave()
        await Person(id=7, family=f2, name="Igor Gabriel", age=10).asave()

        # creating third family
        f3 = Family(id=3, log="Av brazil")
        await f3.asave()

        # persons of thrird family
        await Person(id=8, family=f3, name="Arthur WA", age=30).asave()
        await Person(id=9, family=f3, name="Paula Leonel", age=25).asave()

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
                        family.persons.sort((a, b) => (a.age > b.age))
                    }
                });

                return family;
            }
        """
        await Family.aobjects.map_reduce(
            map_f=map_family,
            reduce_f=reduce_f,
            output={"replace": "family_map", "db_alias": "test2"},
        )

        await Person.aobjects.map_reduce(
            map_f=map_person,
            reduce_f=reduce_f,
            output={"reduce": "family_map", "db_alias": "test2"},
        )

        collection = (await async_get_db("test2")).family_map

        assert await collection.find_one({"_id": 1}) == {
            "_id": 1,
            "value": {
                "persons": [
                    {"age": 17, "name": "Tayza Mariana"},
                    {"age": 21, "name": "Wilson Jr"},
                    {"age": 40, "name": "Eliana Costa"},
                    {"age": 45, "name": "Wilson Father"},
                ],
                "totalAge": 123,
            },
        }

        assert await collection.find_one({"_id": 2}) == {
            "_id": 2,
            "value": {
                "persons": [
                    {"age": 10, "name": "Igor Gabriel"},
                    {"age": 16, "name": "Isabella Luanna"},
                    {"age": 36, "name": "Sandra Mara"},
                ],
                "totalAge": 62,
            },
        }

        assert await collection.find_one({"_id": 3}) == {
            "_id": 3,
            "value": {
                "persons": [
                    {"age": 25, "name": "Paula Leonel"},
                    {"age": 30, "name": "Arthur WA"},
                ],
                "totalAge": 55,
            },
        }

    async def test_map_reduce_finalize(self):
        """Ensure that map, reduce, and finalize run and introduce "scope"
        by simulating "hotness" ranking with Reddit algorithm.
        """
        from time import mktime

        class Link(Document):
            title = StringField(db_field="bpTitle")
            up_votes = IntField()
            down_votes = IntField()
            submitted = DateTimeField(db_field="sTime")

        await Link.adrop_collection()

        now = datetime.datetime.utcnow()

        # Note: Test data taken from a custom Reddit homepage on
        # Fri, 12 Feb 2010 14:36:00 -0600. Link ordering should
        # reflect order of insertion below, but is not influenced
        # by insertion order.
        await Link(
            title="Google Buzz auto-followed a woman's abusive ex ...",
            up_votes=1079,
            down_votes=553,
            submitted=now - datetime.timedelta(hours=4),
        ).asave()
        await Link(
            title="We did it! Barbie is a computer engineer.",
            up_votes=481,
            down_votes=124,
            submitted=now - datetime.timedelta(hours=2),
        ).asave()
        await Link(
            title="This Is A Mosquito Getting Killed By A Laser",
            up_votes=1446,
            down_votes=530,
            submitted=now - datetime.timedelta(hours=13),
        ).asave()
        await Link(
            title="Arabic flashcards land physics student in jail.",
            up_votes=215,
            down_votes=105,
            submitted=now - datetime.timedelta(hours=6),
        ).asave()
        await Link(
            title="The Burger Lab: Presenting, the Flood Burger",
            up_votes=48,
            down_votes=17,
            submitted=now - datetime.timedelta(hours=5),
        ).asave()
        await Link(
            title="How to see polarization with the naked eye",
            up_votes=74,
            down_votes=13,
            submitted=now - datetime.timedelta(hours=10),
        ).asave()

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
        scope = {"reddit_epoch": reddit_epoch}

        # run a map/reduce operation across all links. ordering is set
        # to "-value", which orders the "weight" value returned from
        # "finalize_f" in descending order.
        results = Link.aobjects.order_by("-value")
        results = await results.map_reduce(
            map_f, reduce_f, "myresults", finalize_f=finalize_f, scope=scope
        )

        # assert troublesome Buzz article is ranked 1st
        assert (await results[0].aobject).title.startswith("Google Buzz")

        # assert laser vision is ranked last
        assert (await results[-1].aobject).title.startswith("How to see")

        await Link.adrop_collection()

    async def test_item_frequencies(self):
        """Ensure that item frequencies are properly generated from lists."""

        class BlogPost(Document):
            hits = IntField()
            tags = ListField(StringField(), db_field="blogTags")

        await BlogPost.adrop_collection()

        await BlogPost(hits=1, tags=["music", "film", "actors", "watch"]).asave()
        await BlogPost(hits=2, tags=["music", "watch"]).asave()
        await BlogPost(hits=2, tags=["music", "actors"]).asave()

        def test_assertions(f):
            f = {key: int(val) for key, val in f.items()}
            assert {"music", "film", "actors", "watch"} == set(f.keys())
            assert f["music"] == 3
            assert f["actors"] == 2
            assert f["watch"] == 2
            assert f["film"] == 1

        exec_js = await BlogPost.aobjects.item_frequencies("tags")
        map_reduce = await BlogPost.aobjects.item_frequencies("tags", map_reduce=True)
        test_assertions(exec_js)
        test_assertions(map_reduce)

        # Ensure query is taken into account
        def test_assertions(f):
            f = {key: int(val) for key, val in f.items()}
            assert {"music", "actors", "watch"} == set(f.keys())
            assert f["music"] == 2
            assert f["actors"] == 1
            assert f["watch"] == 1

        exec_js = await BlogPost.aobjects(hits__gt=1).item_frequencies("tags")
        map_reduce = await BlogPost.aobjects(hits__gt=1).item_frequencies(
            "tags", map_reduce=True
        )
        test_assertions(exec_js)
        test_assertions(map_reduce)

        # Check that normalization works
        def test_assertions(f):
            assert round(abs(f["music"] - 3.0 / 8.0), 7) == 0
            assert round(abs(f["actors"] - 2.0 / 8.0), 7) == 0
            assert round(abs(f["watch"] - 2.0 / 8.0), 7) == 0
            assert round(abs(f["film"] - 1.0 / 8.0), 7) == 0

        exec_js = await BlogPost.aobjects.item_frequencies("tags", normalize=True)
        map_reduce = await BlogPost.aobjects.item_frequencies(
            "tags", normalize=True, map_reduce=True
        )
        test_assertions(exec_js)
        test_assertions(map_reduce)

        # Check item_frequencies works for non-list fields
        def test_assertions(f):
            assert {1, 2} == set(f.keys())
            assert f[1] == 1
            assert f[2] == 2

        exec_js = await BlogPost.aobjects.item_frequencies("hits")
        map_reduce = await BlogPost.aobjects.item_frequencies("hits", map_reduce=True)
        test_assertions(exec_js)
        test_assertions(map_reduce)

        await BlogPost.adrop_collection()

    async def test_item_frequencies_on_embedded(self):
        """Ensure that item frequencies are properly generated from lists."""

        class Phone(EmbeddedDocument):
            number = StringField()

        class Person(Document):
            name = StringField()
            phone = EmbeddedDocumentField(Phone)

        await Person.adrop_collection()

        doc = Person(name="Guido")
        doc.phone = Phone(number="62-3331-1656")
        await doc.asave()

        doc = Person(name="Marr")
        doc.phone = Phone(number="62-3331-1656")
        await doc.asave()

        doc = Person(name="WP Junior")
        doc.phone = Phone(number="62-3332-1656")
        await doc.asave()

        def test_assertions(f):
            f = {key: int(val) for key, val in f.items()}
            assert {"62-3331-1656", "62-3332-1656"} == set(f.keys())
            assert f["62-3331-1656"] == 2
            assert f["62-3332-1656"] == 1

        exec_js = await Person.aobjects.item_frequencies("phone.number")
        map_reduce = await Person.aobjects.item_frequencies("phone.number", map_reduce=True)
        test_assertions(exec_js)
        test_assertions(map_reduce)

        # Ensure query is taken into account
        def test_assertions(f):
            f = {key: int(val) for key, val in f.items()}
            assert {"62-3331-1656"} == set(f.keys())
            assert f["62-3331-1656"] == 2

        exec_js = await Person.aobjects(phone__number="62-3331-1656").item_frequencies(
            "phone.number"
        )
        map_reduce = await Person.aobjects(phone__number="62-3331-1656").item_frequencies(
            "phone.number", map_reduce=True
        )
        test_assertions(exec_js)
        test_assertions(map_reduce)

        # Check that normalization works
        def test_assertions(f):
            assert f["62-3331-1656"] == 2.0 / 3.0
            assert f["62-3332-1656"] == 1.0 / 3.0

        exec_js = await Person.aobjects.item_frequencies("phone.number", normalize=True)
        map_reduce = await Person.aobjects.item_frequencies(
            "phone.number", normalize=True, map_reduce=True
        )
        test_assertions(exec_js)
        test_assertions(map_reduce)

    async def test_item_frequencies_null_values(self):
        class Person(Document):
            name = StringField()
            city = StringField()

        await Person.adrop_collection()

        await Person(name="Wilson Snr", city="CRB").asave()
        await Person(name="Wilson Jr").asave()

        freq = await Person.aobjects.item_frequencies("city")
        assert freq == {"CRB": 1.0, None: 1.0}
        freq = await Person.aobjects.item_frequencies("city", normalize=True)
        assert freq == {"CRB": 0.5, None: 0.5}

        freq = await Person.aobjects.item_frequencies("city", map_reduce=True)
        assert freq == {"CRB": 1.0, None: 1.0}
        freq = await Person.aobjects.item_frequencies("city", normalize=True, map_reduce=True)
        assert freq == {"CRB": 0.5, None: 0.5}

    async def test_average(self):
        """Ensure that field can be averaged correctly."""
        await self.Person(name="person", age=0).asave()
        assert int(await self.Person.aobjects.average("age")) == 0

        ages = [23, 54, 12, 94, 27]
        for i, age in enumerate(ages):
            await self.Person(name="test%s" % i, age=age).asave()

        avg = float(sum(ages)) / (len(ages) + 1)  # take into account the 0
        assert round(abs(int(await self.Person.aobjects.average("age")) - avg), 7) == 0

        await self.Person(name="ageless person").asave()
        assert int(await self.Person.aobjects.average("age")) == avg

        # dot notation
        await self.Person(name="person meta", person_meta=self.PersonMeta(weight=0)).asave()
        assert (
                round(abs(int(await self.Person.aobjects.average("person_meta.weight")) - 0), 7)
                == 0
        )

        for i, weight in enumerate(ages):
            await self.Person(
                name=f"test meta{i}", person_meta=self.PersonMeta(weight=weight)
            ).asave()

        assert (
                round(abs(int(await self.Person.aobjects.average("person_meta.weight")) - avg), 7)
                == 0
        )

        await self.Person(name="test meta none").asave()
        assert int(await self.Person.aobjects.average("person_meta.weight")) == avg

        # test summing over a filtered queryset
        over_50 = [a for a in ages if a >= 50]
        avg = float(sum(over_50)) / len(over_50)
        assert await self.Person.aobjects.filter(age__gte=50).average("age") == avg

    async def test_sum(self):
        """Ensure that field can be summed over correctly."""
        ages = [23, 54, 12, 94, 27]
        for i, age in enumerate(ages):
            await self.Person(name="test%s" % i, age=age).asave()

        assert await self.Person.aobjects.sum("age") == sum(ages)

        await self.Person(name="ageless person").asave()
        assert await self.Person.aobjects.sum("age") == sum(ages)

        for i, age in enumerate(ages):
            await self.Person(
                name="test meta%s" % i, person_meta=self.PersonMeta(weight=age)
            ).asave()

        assert await self.Person.aobjects.sum("person_meta.weight") == sum(ages)

        await self.Person(name="weightless person").asave()
        assert await self.Person.aobjects.sum("age") == sum(ages)

        # test summing over a filtered queryset
        assert await self.Person.aobjects.filter(age__gte=50).sum("age") == sum(
            a for a in ages if a >= 50
        )

    async def test_sum_over_db_field(self):
        """Ensure that a field mapped to a db field with a different name
        can be summed over correctly.
        """

        class UserVisit(Document):
            num_visits = IntField(db_field="visits")

        await UserVisit.adrop_collection()

        await UserVisit.aobjects.create(num_visits=10)
        await UserVisit.aobjects.create(num_visits=5)

        assert await UserVisit.aobjects.sum("num_visits") == 15

    async def test_average_over_db_field(self):
        """Ensure that a field mapped to a db field with a different name
        can have its average computed correctly.
        """

        class UserVisit(Document):
            num_visits = IntField(db_field="visits")

        await UserVisit.adrop_collection()

        await UserVisit.aobjects.create(num_visits=20)
        await UserVisit.aobjects.create(num_visits=10)

        assert await UserVisit.aobjects.average("num_visits") == 15

    async def test_embedded_average(self):
        class Pay(EmbeddedDocument):
            value = DecimalField()

        class Doc(Document):
            name = StringField()
            pay = EmbeddedDocumentField(Pay)

        await Doc.adrop_collection()

        await Doc(name="Wilson Junior", pay=Pay(value=150)).asave()
        await Doc(name="Isabella Luanna", pay=Pay(value=530)).asave()
        await Doc(name="Tayza mariana", pay=Pay(value=165)).asave()
        await Doc(name="Eliana Costa", pay=Pay(value=115)).asave()

        assert await Doc.aobjects.average("pay.value") == 240

    async def test_embedded_array_average(self):
        class Pay(EmbeddedDocument):
            values = ListField(DecimalField())

        class Doc(Document):
            name = StringField()
            pay = EmbeddedDocumentField(Pay)

        await Doc.adrop_collection()

        await Doc(name="Wilson Junior", pay=Pay(values=[150, 100])).asave()
        await Doc(name="Isabella Luanna", pay=Pay(values=[530, 100])).asave()
        await Doc(name="Tayza mariana", pay=Pay(values=[165, 100])).asave()
        await Doc(name="Eliana Costa", pay=Pay(values=[115, 100])).asave()

        assert await Doc.aobjects.average("pay.values") == 170

    async def test_array_average(self):
        class Doc(Document):
            values = ListField(DecimalField())

        await Doc.adrop_collection()

        await Doc(values=[150, 100]).asave()
        await Doc(values=[530, 100]).asave()
        await Doc(values=[165, 100]).asave()
        await Doc(values=[115, 100]).asave()

        assert await Doc.aobjects.average("values") == 170

    async def test_embedded_sum(self):
        class Pay(EmbeddedDocument):
            value = DecimalField()

        class Doc(Document):
            name = StringField()
            pay = EmbeddedDocumentField(Pay)

        await Doc.adrop_collection()

        await Doc(name="Wilson Junior", pay=Pay(value=150)).asave()
        await Doc(name="Isabella Luanna", pay=Pay(value=530)).asave()
        await Doc(name="Tayza mariana", pay=Pay(value=165)).asave()
        await Doc(name="Eliana Costa", pay=Pay(value=115)).asave()

        assert await Doc.aobjects.sum("pay.value") == 960

    async def test_embedded_array_sum(self):
        class Pay(EmbeddedDocument):
            values = ListField(DecimalField())

        class Doc(Document):
            name = StringField()
            pay = EmbeddedDocumentField(Pay)

        await Doc.adrop_collection()

        await Doc(name="Wilson Junior", pay=Pay(values=[150, 100])).asave()
        await Doc(name="Isabella Luanna", pay=Pay(values=[530, 100])).asave()
        await Doc(name="Tayza mariana", pay=Pay(values=[165, 100])).asave()
        await Doc(name="Eliana Costa", pay=Pay(values=[115, 100])).asave()

        assert await Doc.aobjects.sum("pay.values") == 1360

    async def test_array_sum(self):
        class Doc(Document):
            values = ListField(DecimalField())

        await Doc.adrop_collection()

        await Doc(values=[150, 100]).asave()
        await Doc(values=[530, 100]).asave()
        await Doc(values=[165, 100]).asave()
        await Doc(values=[115, 100]).asave()

        assert await Doc.aobjects.sum("values") == 1360

    async def test_distinct(self):
        """Ensure that the QuerySet.distinct method works."""
        await self.Person(name="Mr Orange", age=20).asave()
        await self.Person(name="Mr White", age=20).asave()
        await self.Person(name="Mr Orange", age=30).asave()
        await self.Person(name="Mr Pink", age=30).asave()
        assert set(await self.Person.aobjects.distinct("name")) == {
            "Mr Orange",
            "Mr White",
            "Mr Pink",
        }
        assert set(await self.Person.aobjects.distinct("age")) == {20, 30}
        assert set(await self.Person.aobjects(age=30).distinct("name")) == {
            "Mr Orange",
            "Mr Pink",
        }

    async def test_distinct_handles_references(self):
        class Bar(Document):
            text = StringField()

        class Foo(Document):
            bar = ReferenceField("Bar")

        await Bar.adrop_collection()
        await Foo.adrop_collection()

        bar = Bar(text="hi")
        await bar.asave()

        foo = Foo(bar=bar)
        await foo.asave()

        assert await Foo.aobjects.select_related("bar").distinct("bar") == [bar]
        assert await Foo.aobjects.distinct("bar") == [bar.pk]

    async def test_base_queryset_iter_raise_not_implemented(self):
        class Tmp(Document):
            pass

        qs = AsyncBaseQuerySet(document=Tmp)
        with pytest.raises(NotImplementedError):
            _ = list(qs)

    async def test_search_text_raise_if_called_2_times(self):
        class News(Document):
            title = StringField()
            content = StringField()
            is_active = BooleanField(default=True)

        await News.adrop_collection()
        with pytest.raises(OperationError):
            await News.aobjects.search_text("t1", language="portuguese").search_text(
                "t2", language="french"
            )

    async def test_search_text(self):
        class News(Document):
            title = StringField()
            content = StringField()
            is_active = BooleanField(default=True)

            meta = {
                "indexes": [
                    {
                        "fields": ["$title", "$content"],
                        "default_language": "portuguese",
                        "weights": {"title": 10, "content": 2},
                    }
                ]
            }

        await News.adrop_collection()
        info = await (await News.aobjects._collection).index_information()
        assert "title_text_content_text" in info
        assert "textIndexVersion" in info["title_text_content_text"]

        await News(
            title="Neymar quebrou a vertebra",
            content="O Brasil sofre com a perda de Neymar",
        ).asave()

        await News(
            title="Brasil passa para as quartas de finais",
            content="Com o brasil nas quartas de finais teremos um "
                    "jogo complicado com a alemanha",
        ).asave()

        count = await News.aobjects.search_text("neymar", language="portuguese").count()

        assert count == 1

        count = await News.aobjects.search_text("brasil -neymar").count()

        assert count == 1

        await News(
            title="As eleições no Brasil já estão em planejamento",
            content="A candidata dilma roussef já começa o teu planejamento",
            is_active=False,
        ).asave()

        new = await News.aobjects(is_active=False).search_text("dilma", language="pt").first()

        query = News.aobjects(is_active=False).search_text("dilma", language="pt")._query

        assert query == {
            "$text": {"$search": "dilma", "$language": "pt"},
            "is_active": False,
        }

        assert not new.is_active
        assert "dilma" in new.content
        assert "planejamento" in new.title

        query = News.aobjects.search_text("candidata", text_score=True)
        assert query._search_text == "candidata"
        new = await query.first()

        assert isinstance(new.get_text_score(), float)

        # count
        query = News.aobjects.search_text("brasil", text_score=True).order_by(
            "$text_score"
        )
        assert query._search_text == "brasil"

        assert await query.count() == 3
        assert query._query == {"$text": {"$search": "brasil"}}
        cursor_args = query._cursor_args
        cursor_args_fields = cursor_args["projection"]
        assert cursor_args_fields == {"_text_score": {"$meta": "textScore"}}

        text_scores = [i.get_text_score() async for i in query]
        assert len(text_scores) == 3

        assert text_scores[0] > text_scores[1]
        assert text_scores[1] > text_scores[2]
        max_text_score = text_scores[0]

        # get item
        item = await News.aobjects.search_text("brasil").order_by("$text_score").first()
        assert item.get_text_score() == max_text_score

        # Verify query reproducibility when text_score is disabled
        # Following wouldn't work for text_score=True  #2759
        for i in range(10):
            qs1 = News.aobjects.search_text("brasil", text_score=False)
            qs2 = News.aobjects.search_text("brasil", text_score=False)
            assert await qs1.to_list() == await qs2.to_list()

    async def test_distinct_handles_references_to_alias(self):
        await async_register_connection("testdb", "mongoenginetest2")

        class Bar(Document):
            text = StringField()
            meta = {"db_alias": "testdb"}

        class Foo(Document):
            bar = ReferenceField("Bar")
            meta = {"db_alias": "testdb"}

        await Bar.adrop_collection()
        await Foo.adrop_collection()

        bar = Bar(text="hi")
        await bar.asave()

        foo = Foo(bar=bar)
        await foo.asave()

        assert await Foo.aobjects.select_related("bar").distinct("bar") == [bar]
        await async_disconnect("testdb")

    async def test_distinct_handles_db_field(self):
        """Ensure that distinct resolves field name to db_field as expected."""

        class Product(Document):
            product_id = IntField(db_field="pid")

        await Product.adrop_collection()

        await Product(product_id=1).asave()
        await Product(product_id=2).asave()
        await Product(product_id=1).asave()

        assert set(await Product.aobjects.distinct("product_id")) == {1, 2}
        assert set(await Product.aobjects.distinct("pid")) == {1, 2}

        await Product.adrop_collection()

    async def test_distinct_ListField_EmbeddedDocumentField(self):
        class Author(EmbeddedDocument):
            name = StringField()

        class Book(Document):
            title = StringField()
            authors = ListField(EmbeddedDocumentField(Author))

        await Book.adrop_collection()

        mark_twain = Author(name="Mark Twain")
        john_tolkien = Author(name="John Ronald Reuel Tolkien")

        await Book.aobjects.create(title="Tom Sawyer", authors=[mark_twain])
        await Book.aobjects.create(title="The Lord of the Rings", authors=[john_tolkien])
        await Book.aobjects.create(title="The Stories", authors=[mark_twain, john_tolkien])

        authors = await Book.aobjects.distinct("authors")
        authors_names = {author.name for author in authors}
        assert authors_names == {mark_twain.name, john_tolkien.name}

    async def test_distinct_ListField_EmbeddedDocumentField_EmbeddedDocumentField(self):
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

        await Book.adrop_collection()

        europe = Continent(continent_name="europe")
        asia = Continent(continent_name="asia")

        scotland = Country(country_name="Scotland", continent=europe)
        tibet = Country(country_name="Tibet", continent=asia)

        mark_twain = Author(name="Mark Twain", country=scotland)
        john_tolkien = Author(name="John Ronald Reuel Tolkien", country=tibet)

        await Book.aobjects.create(title="Tom Sawyer", authors=[mark_twain])
        await Book.aobjects.create(title="The Lord of the Rings", authors=[john_tolkien])
        await Book.aobjects.create(title="The Stories", authors=[mark_twain, john_tolkien])

        country_list = await Book.aobjects.distinct("authors.country")
        assert country_list == [scotland, tibet]

        continent_list = await Book.aobjects.distinct("authors.country.continent")
        continent_list_names = {c.continent_name for c in continent_list}
        assert continent_list_names == {europe.continent_name, asia.continent_name}

    async def test_distinct_ListField_ReferenceField(self):
        class Bar(Document):
            text = StringField()

        class Foo(Document):
            bar = ReferenceField("Bar")
            bar_lst = ListField(ReferenceField("Bar"))

        await Bar.adrop_collection()
        await Foo.adrop_collection()

        bar_1 = Bar(text="hi")
        await bar_1.asave()

        bar_2 = Bar(text="bye")
        await bar_2.asave()

        foo = Foo(bar=bar_1, bar_lst=[bar_1, bar_2])
        await foo.asave()

        assert set(await Foo.aobjects.select_related("bar_lst").distinct("bar_lst")) == {bar_1, bar_2}
        assert set(await Foo.aobjects.distinct("bar_lst")) == {bar_1.pk, bar_2.pk}

    async def test_custom_manager(self):
        """Ensure that custom QuerySetManager instances work as expected."""

        class BlogPost(Document):
            tags = ListField(StringField())
            deleted = BooleanField(default=False)
            date = DateTimeField(default=datetime.datetime.now)

            @queryset_manager(queryset=AsyncQuerySet)
            def objects(cls, qryset):
                opts = {"deleted": False}
                return qryset(**opts)

            @queryset_manager(queryset=AsyncQuerySet)
            def objects_1_arg(qryset):
                opts = {"deleted": False}
                return qryset(**opts)

            @queryset_manager(queryset=AsyncQuerySet)
            def music_posts(doc_cls, queryset, deleted=False):
                return queryset(tags="music", deleted=deleted).order_by("date")

        await BlogPost.adrop_collection()

        post1 = await BlogPost(tags=["music", "film"]).asave()
        post2 = await BlogPost(tags=["music"]).asave()
        post3 = await BlogPost(tags=["film", "actors"]).asave()
        post4 = await BlogPost(tags=["film", "actors", "music"], deleted=True).asave()

        assert [p.id async for p in BlogPost.objects()] == [post1.id, post2.id, post3.id]
        assert [p.id async for p in BlogPost.objects_1_arg()] == [
            post1.id,
            post2.id,
            post3.id,
        ]
        assert [p.id async for p in BlogPost.music_posts()] == [post1.id, post2.id]

        assert [p.id async for p in BlogPost.music_posts(True)] == [post4.id]

        await BlogPost.adrop_collection()

    async def test_custom_manager_overriding_objects_works(self):
        class Foo(Document):
            bar = StringField(default="bar")
            active = BooleanField(default=False)

            @queryset_manager(queryset=AsyncQuerySet)
            def objects(doc_cls, queryset):
                return queryset(active=True)

            @queryset_manager(queryset=AsyncQuerySet)
            def with_inactive(doc_cls, queryset):
                return queryset(active=False)

        await Foo.adrop_collection()

        await Foo(active=True).asave()
        await Foo(active=False).asave()

        assert 1 == await Foo.objects.count()
        assert 1 == await Foo.with_inactive.count()

        await (await Foo.objects.first()).adelete()
        assert 1 == await Foo.with_inactive.count()
        assert 0 == await Foo.objects.count()

    async def test_inherit_objects(self):
        class Foo(Document):
            meta = {"allow_inheritance": True}
            active = BooleanField(default=True)

            @queryset_manager(queryset=AsyncQuerySet)
            def objects(klass, queryset):
                return queryset(active=True)

        class Bar(Foo):
            pass

        await Bar.adrop_collection()
        await  Bar.objects.create(active=False)
        assert 0 == await Bar.objects.count()

    async def test_inherit_objects_override(self):
        class Foo(Document):
            meta = {"allow_inheritance": True}
            active = BooleanField(default=True)

            @queryset_manager(queryset=AsyncQuerySet)
            def objects(klass, queryset):
                return queryset(active=True)

        class Bar(Foo):
            @queryset_manager(queryset=AsyncQuerySet)
            def objects(klass, queryset):
                return queryset(active=False)

        await Bar.adrop_collection()
        await Bar.objects.create(active=False)
        assert 0 == await Foo.objects.count()
        assert 1 == await Bar.objects.count()

    async def test_query_value_conversion(self):
        """Ensure that query values are properly converted when necessary."""

        class BlogPost(Document):
            author = ReferenceField(self.Person)

        await BlogPost.adrop_collection()

        person = self.Person(name="test", age=30)
        await person.asave()

        post = BlogPost(author=person)
        await post.asave()

        # Test that query may be performed by providing a document as a value
        # while using a ReferenceField's name - the document should be
        # converted to an DBRef, which is legal, unlike a Document object
        post_obj = await BlogPost.aobjects(author=person).first()
        assert post.id == post_obj.id

        # Test that lists of values work when using the 'in', 'nin' and 'all'
        post_obj = await BlogPost.aobjects(author__in=[person]).first()
        assert post.id == post_obj.id

        await BlogPost.adrop_collection()

    async def test_update_value_conversion(self):
        """Ensure that values used in updates are converted before use."""

        class Group(Document):
            members = ListField(ReferenceField(self.Person))

        await Group.adrop_collection()

        user1 = self.Person(name="user1")
        await user1.asave()
        user2 = self.Person(name="user2")
        await user2.asave()

        group = Group()
        await group.asave()
        await group.asave()

        await Group.aobjects(id=group.id).update(set__members=[user1, user2], )
        await group.aselect_related("members")
        members = group.members
        assert len(members) == 2
        assert members[0].name == user1.name
        assert members[1].name == user2.name

        await Group.adrop_collection()

    async def test_bulk(self):
        """Ensure bulk querying by object id returns a proper dict."""

        class BlogPost(Document):
            title = StringField()

        await BlogPost.adrop_collection()

        post_1 = BlogPost(title="Post #1")
        post_2 = BlogPost(title="Post #2")
        post_3 = BlogPost(title="Post #3")
        post_4 = BlogPost(title="Post #4")
        post_5 = BlogPost(title="Post #5")

        await post_1.asave()
        await post_2.asave()
        await post_3.asave()
        await post_4.asave()
        await post_5.asave()

        ids = [post_1.id, post_2.id, post_5.id]
        objects = await BlogPost.aobjects.in_bulk(ids)

        assert len(objects) == 3

        assert post_1.id in objects
        assert post_2.id in objects
        assert post_5.id in objects

        assert objects[post_1.id].title == post_1.title
        assert objects[post_2.id].title == post_2.title
        assert objects[post_5.id].title == post_5.title

        objects = await BlogPost.aobjects.as_pymongo().in_bulk(ids)
        assert len(objects) == 3
        assert isinstance(objects[post_1.id], dict)

        await BlogPost.adrop_collection()

    async def tearDown(self):
        await self.Person.adrop_collection()

    async def test_custom_querysets(self):
        """Ensure that custom QuerySet classes may be used."""

        class CustomQuerySet(AsyncQuerySet):
            async def not_empty(self):
                return await self.count() > 0

        class Post(Document):
            meta = {"queryset_class": CustomQuerySet}

        await Post.adrop_collection()

        assert isinstance(Post.aobjects, CustomQuerySet)
        assert not await Post.aobjects.not_empty()

        await Post().asave()
        assert Post.aobjects.not_empty()

        await Post.adrop_collection()

    async def test_custom_querysets_set_manager_directly(self):
        """Ensure that custom QuerySet classes may be used."""

        class CustomQuerySet(AsyncQuerySet):
            async def not_empty(self):
                return await self.count() > 0

        class CustomQuerySetManager(QuerySetManager):
            queryset_class = CustomQuerySet

        class Post(Document):
            objects = CustomQuerySetManager()

        await Post.adrop_collection()

        assert isinstance(Post.aobjects, CustomQuerySet)
        assert not await Post.aobjects.not_empty()

        await Post().asave()
        assert await Post.aobjects.not_empty()

        await Post.adrop_collection()

    async def test_custom_querysets_set_manager_methods(self):
        """Ensure that custom QuerySet classes methods may be used."""

        class CustomQuerySet(AsyncQuerySet):
            async def delete(self, *args, **kwargs):
                """Example of method when one want to change default behaviour of it"""
                return 0

        class CustomQuerySetManager(QuerySetManager):
            queryset_class = CustomQuerySet

        class Post(Document):
            objects = CustomQuerySetManager()

        await Post.adrop_collection()

        assert isinstance(Post.aobjects, CustomQuerySet)
        assert await Post.aobjects.delete() == 0

        post = Post()
        await post.asave()
        assert await Post.aobjects.count() == 1
        await post.adelete()
        assert await Post.aobjects.count() == 1

        await Post.adrop_collection()

    async def test_custom_querysets_managers_directly(self):
        """Ensure that custom QuerySet classes may be used."""

        class CustomQuerySetManager(QuerySetManager):
            @staticmethod
            def get_queryset(doc_cls, queryset):
                return queryset(is_published=True)

        class Post(Document):
            is_published = BooleanField(default=False)
            published = CustomQuerySetManager(default=AsyncQuerySet)

        await Post.adrop_collection()

        await Post().asave()
        await Post(is_published=True).asave()
        assert await Post.aobjects.count() == 2
        assert await Post.published.count() == 1

        await Post.adrop_collection()

    async def test_custom_querysets_inherited(self):
        """Ensure that custom QuerySet classes may be used."""

        class CustomQuerySet(AsyncQuerySet):
            async def not_empty(self):
                return await self.count() > 0

        class Base(Document):
            meta = {"abstract": True, "queryset_class": CustomQuerySet}

        class Post(Base):
            pass

        await Post.adrop_collection()
        assert isinstance(Post.aobjects, CustomQuerySet)
        assert not await Post.aobjects.not_empty()

        await Post().asave()
        assert await Post.aobjects.not_empty()

        await Post.adrop_collection()

    async def test_custom_querysets_inherited_direct(self):
        """Ensure that custom QuerySet classes may be used."""

        class CustomQuerySet(AsyncQuerySet):
            async def not_empty(self):
                return await self.count() > 0

        class CustomQuerySetManager(QuerySetManager):
            queryset_class = CustomQuerySet

        class Base(Document):
            meta = {"abstract": True}
            objects = CustomQuerySetManager()

        class Post(Base):
            pass

        await Post.adrop_collection()
        assert isinstance(Post.aobjects, CustomQuerySet)
        assert not await Post.aobjects.not_empty()

        await Post().asave()
        assert await Post.aobjects.not_empty()

        await Post.adrop_collection()

    async def test_count_limit_and_skip(self):
        class Post(Document):
            title = StringField()

        await Post.adrop_collection()

        for i in range(10):
            await Post(title="Post %s" % i).asave()

        assert 5 == await Post.aobjects.limit(5).skip(5).count(with_limit_and_skip=True)

        assert 10 == await Post.aobjects.limit(5).skip(5).count(with_limit_and_skip=False)

    async def test_count_and_none(self):
        """Test count works with None()"""

        class MyDoc(Document):
            pass

        await MyDoc.adrop_collection()
        for i in range(0, 10):
            await MyDoc().asave()

        assert await MyDoc.aobjects.count() == 10
        assert await MyDoc.aobjects.none().count() == 0

    async def test_count_list_embedded(self):
        class B(EmbeddedDocument):
            c = StringField()

        class A(Document):
            b = ListField(EmbeddedDocumentField(B))

        assert await A.aobjects(b=[{"c": "c"}]).count() == 0

    async def test_call_after_limits_set(self):
        """Ensure that re-filtering after slicing works"""

        class Post(Document):
            title = StringField()

        await Post.adrop_collection()

        await Post(title="Post 1").asave()
        await Post(title="Post 2").asave()

        posts = Post.aobjects.all().skip(0).limit(1)
        assert len(await posts().to_list()) == 1

        await Post.adrop_collection()

    async def test_order_then_filter(self):
        """Ensure that ordering still works after filtering."""

        class Number(Document):
            n = IntField()

        await Number.adrop_collection()

        n2 = await Number.aobjects.create(n=2)
        n1 = await Number.aobjects.create(n=1)

        assert await Number.aobjects.to_list() == [n2, n1]
        assert await Number.aobjects.order_by("n").to_list() == [n1, n2]
        assert await Number.aobjects.order_by("n").filter().to_list() == [n1, n2]

        await Number.adrop_collection()

    async def test_clone(self):
        """Ensure that cloning clones complex querysets"""

        class Number(Document):
            n = IntField()

        await Number.adrop_collection()

        for i in range(1, 101):
            t = Number(n=i)
            await t.asave()

        test = Number.aobjects
        test2 = test.clone()
        assert test != test2
        assert await test.count() == await test2.count()

        test = test.filter(n__gt=11)
        test2 = test.clone()
        assert test != test2
        assert await test.count() == await test2.count()

        test = test.limit(10)
        test2 = test.clone()
        assert test != test2
        assert await test.count() == await test2.count()

        await Number.adrop_collection()

    async def test_clone_retains_settings(self):
        """Ensure that cloning retains the read_preference and read_concern"""

        class Number(Document):
            n = IntField()

        await Number.adrop_collection()

        qs = Number.aobjects
        qs_clone = qs.clone()
        assert qs._read_preference == qs_clone._read_preference
        assert qs._read_concern == qs_clone._read_concern

        qs = Number.aobjects.read_preference(ReadPreference.PRIMARY_PREFERRED)
        qs_clone = qs.clone()
        assert qs._read_preference == ReadPreference.PRIMARY_PREFERRED
        assert qs._read_preference == qs_clone._read_preference

        qs = Number.aobjects.read_concern({"level": "majority"})
        qs_clone = qs.clone()
        assert qs._read_concern.document == {"level": "majority"}
        assert qs._read_concern == qs_clone._read_concern

        await Number.adrop_collection()

    async def test_using(self):
        """Ensure that switching databases for a queryset is possible"""

        class Number2(Document):
            n = IntField()

        await Number2.adrop_collection()
        async with switch_db(Number2, "test2") as Number2:
            await Number2.adrop_collection()

        for i in range(1, 10):
            t = Number2(n=i)
            t.switch_db("test2")
            await t.asave()

        assert await Number2.aobjects.using("test2").count() == 9

    async def test_unset_reference(self):
        class Comment(Document):
            text = StringField()

        class Post(Document):
            comment = ReferenceField(Comment)

        await Comment.adrop_collection()
        await Post.adrop_collection()

        comment = await Comment.aobjects.create(text="test")
        post = await Post.aobjects.create(comment=comment)

        assert post.comment == comment
        await Post.aobjects.update(unset__comment=1)
        await post.areload()
        assert post.comment is None

        await Comment.adrop_collection()
        await Post.adrop_collection()

    async def test_order_works_with_custom_db_field_names(self):
        class Number(Document):
            n = IntField(db_field="number")

        await Number.adrop_collection()

        n2 = await Number.aobjects.create(n=2)
        n1 = await Number.aobjects.create(n=1)

        assert await Number.aobjects.to_list() == [n2, n1]
        assert await Number.aobjects.order_by("n").to_list() == [n1, n2]

        await Number.adrop_collection()

    async def test_order_works_with_primary(self):
        """Ensure that order_by and primary work."""

        class Number(Document):
            n = IntField(primary_key=True)

        await Number.adrop_collection()

        await Number(n=1).asave()
        await Number(n=2).asave()
        await Number(n=3).asave()

        numbers = [n.n async for n in Number.aobjects.order_by("-n")]
        assert [3, 2, 1] == numbers

        numbers = [n.n async for n in Number.aobjects.order_by("+n")]
        assert [1, 2, 3] == numbers
        await Number.adrop_collection()

    async def test_create_index(self):
        """Ensure that manual creation of indexes works."""

        class Comment(Document):
            message = StringField()
            meta = {"allow_inheritance": True}

        await Comment.acreate_index("message")

        info = await (await Comment.aobjects._collection).index_information()
        info = [
            (value["key"], value.get("unique", False), value.get("sparse", False))
            for key, value in info.items()
        ]
        assert ([("_cls", 1), ("message", 1)], False, False) in info

    async def test_where_query(self):
        """Ensure that where clauses work."""

        class IntPair(Document):
            fielda = IntField()
            fieldb = IntField()

        await IntPair.adrop_collection()

        a = IntPair(fielda=1, fieldb=1)
        b = IntPair(fielda=1, fieldb=2)
        c = IntPair(fielda=2, fieldb=1)
        await a.asave()
        await b.asave()
        await c.asave()

        query = IntPair.aobjects.where("this[~fielda] >= this[~fieldb]")
        assert 'this["fielda"] >= this["fieldb"]' == query._where_clause
        results = await query.to_list()
        assert 2 == len(results)
        assert a in results
        assert c in results

        query = IntPair.aobjects.where("this[~fielda] == this[~fieldb]")
        results = await query.to_list()
        assert 1 == len(results)
        assert a in results

        query = IntPair.aobjects.where(
            "function() { return this[~fielda] >= this[~fieldb] }"
        )
        assert (
                'function() { return this["fielda"] >= this["fieldb"] }'
                == query._where_clause
        )
        results = await query.to_list()
        assert 2 == len(results)
        assert a in results
        assert c in results

        with pytest.raises(TypeError):
            await IntPair.aobjects.where(fielda__gte=3).to_list()

    async def test_where_query_field_name_subs(self):
        class DomainObj(Document):
            field_1 = StringField(db_field="field_2")

        await DomainObj.adrop_collection()

        await DomainObj(field_1="test").asave()

        obj = DomainObj.aobjects.where("this[~field_1] == 'NOTMATCHING'")
        assert not await obj.to_list()

        obj = DomainObj.aobjects.where("this[~field_1] == 'test'")
        assert await obj.to_list()

    async def test_where_modify(self):
        class DomainObj(Document):
            field = StringField()

        await DomainObj.adrop_collection()

        await DomainObj(field="test").asave()

        obj = DomainObj.aobjects.where("this[~field] == 'NOTMATCHING'")
        assert not await obj.to_list()

        obj = DomainObj.aobjects.where("this[~field] == 'test'")
        assert await obj.to_list()

        qs = await DomainObj.aobjects.where("this[~field] == 'NOTMATCHING'").modify(
            field="new"
        )
        assert not qs

        qs = await DomainObj.aobjects.where("this[~field] == 'test'").modify(field="new")
        assert qs

    async def test_where_modify_field_name_subs(self):
        class DomainObj(Document):
            field_1 = StringField(db_field="field_2")

        await DomainObj.adrop_collection()

        await DomainObj(field_1="test").asave()

        obj = await DomainObj.aobjects.where("this[~field_1] == 'NOTMATCHING'").modify(
            field_1="new"
        )
        assert not obj

        obj = await DomainObj.aobjects.where("this[~field_1] == 'test'").modify(field_1="new")
        assert obj

        assert await async_get_as_pymongo(obj) == {"_id": obj.id, "field_2": "new"}

    async def test_scalar(self):
        class Organization(Document):
            name = StringField()

        class User(Document):
            name = StringField()
            organization = ObjectIdField()

        await User.adrop_collection()
        await Organization.adrop_collection()

        whitehouse = Organization(name="White House")
        await whitehouse.asave()
        await User(name="Bob Dole", organization=whitehouse.id).asave()

        # Efficient way to get all unique organization names for a given
        # set of users (Pretend this has additional filtering.)
        user_orgs = set(await User.aobjects.scalar("organization").to_list())
        orgs = Organization.aobjects(id__in=user_orgs).scalar("name")
        assert await orgs.to_list() == ["White House"]

        # Efficient for generating listings, too.
        orgs = await Organization.aobjects.scalar("name").in_bulk(list(user_orgs))
        user_map = User.aobjects.scalar("name", "organization")
        user_listing = [(user, orgs[org]) async for user, org in user_map]
        assert [("Bob Dole", "White House")] == user_listing

    async def test_scalar_simple(self):
        class TestDoc(Document):
            x = IntField()
            y = BooleanField()

        await TestDoc.adrop_collection()

        await TestDoc(x=10, y=True).asave()
        await TestDoc(x=20, y=False).asave()
        await TestDoc(x=30, y=True).asave()

        plist = await TestDoc.aobjects.scalar("x", "y").to_list()

        assert len(plist) == 3
        assert plist[0] == (10, True)
        assert plist[1] == (20, False)
        assert plist[2] == (30, True)

        class UserDoc(Document):
            name = StringField()
            age = IntField()

        await UserDoc.adrop_collection()

        await UserDoc(name="Wilson Jr", age=19).asave()
        await UserDoc(name="Wilson", age=43).asave()
        await UserDoc(name="Eliana", age=37).asave()
        await UserDoc(name="Tayza", age=15).asave()

        ulist = await UserDoc.aobjects.scalar("name", "age").to_list()

        assert ulist == [
            ("Wilson Jr", 19),
            ("Wilson", 43),
            ("Eliana", 37),
            ("Tayza", 15),
        ]

        ulist = await UserDoc.aobjects.scalar("name").order_by("age").to_list()

        assert ulist == [("Tayza"), ("Wilson Jr"), ("Eliana"), ("Wilson")]

    async def test_scalar_embedded(self):
        class Profile(EmbeddedDocument):
            name = StringField()
            age = IntField()

        class Locale(EmbeddedDocument):
            city = StringField()
            country = StringField()

        class Person(Document):
            profile = EmbeddedDocumentField(Profile)
            locale = EmbeddedDocumentField(Locale)

        await Person.adrop_collection()

        await Person(
            profile=Profile(name="Wilson Jr", age=19),
            locale=Locale(city="Corumba-GO", country="Brazil"),
        ).asave()

        await Person(
            profile=Profile(name="Gabriel Falcao", age=23),
            locale=Locale(city="New York", country="USA"),
        ).asave()

        await Person(
            profile=Profile(name="Lincoln de souza", age=28),
            locale=Locale(city="Belo Horizonte", country="Brazil"),
        ).asave()

        await Person(
            profile=Profile(name="Walter cruz", age=30),
            locale=Locale(city="Brasilia", country="Brazil"),
        ).asave()

        assert await Person.aobjects.order_by("profile__age").scalar("profile__name").to_list() == ["Wilson Jr",
                                                                                                    "Gabriel Falcao",
                                                                                                    "Lincoln de souza",
                                                                                                    "Walter cruz"]

        ulist = await (
            Person.aobjects.order_by("locale.city").scalar(
                "profile__name", "profile__age", "locale__city"
            ).to_list()
        )
        assert ulist == [
            ("Lincoln de souza", 28, "Belo Horizonte"),
            ("Walter cruz", 30, "Brasilia"),
            ("Wilson Jr", 19, "Corumba-GO"),
            ("Gabriel Falcao", 23, "New York"),
        ]

    async def test_scalar_decimal(self):
        from decimal import Decimal

        class Person(Document):
            name = StringField()
            rating = DecimalField()

        await Person.adrop_collection()
        await Person(name="Wilson Jr", rating=Decimal("1.0")).asave()

        ulist = await Person.aobjects.scalar("name", "rating").to_list()
        assert ulist == [("Wilson Jr", Decimal("1.0"))]

    async def test_scalar_reference_field(self):
        class State(Document):
            name = StringField()

        class Person(Document):
            name = StringField()
            state = ReferenceField(State)

        await State.adrop_collection()
        await Person.adrop_collection()

        s1 = State(name="Goias")
        await s1.asave()

        await Person(name="Wilson JR", state=s1).asave()

        plist = await Person.aobjects.scalar("name", "state").to_list()
        assert [(plist[0][0], plist[0][1])] == [("Wilson JR", s1)]

    async def test_scalar_generic_reference_field(self):
        class State(Document):
            name = StringField()

        class Person(Document):
            name = StringField()
            state = GenericReferenceField(choices=(State,))

        await State.adrop_collection()
        await Person.adrop_collection()

        s1 = State(name="Goias")
        await s1.asave()

        await Person(name="Wilson JR", state=s1).asave()

        plist = await Person.aobjects.select_related("state").scalar("name", "state").to_list()
        assert [(plist[0][0], plist[0][1])] == [("Wilson JR", s1)]

    async def test_generic_reference_field_with_only_and_as_pymongo(self):
        class TestPerson(Document):
            name = StringField()

        class TestActivity(Document):
            name = StringField()
            owner = GenericReferenceField(choices=(TestPerson,))

        await TestPerson.adrop_collection()
        await TestActivity.adrop_collection()

        person = TestPerson(name="owner")
        await person.asave()

        a1 = TestActivity(name="a1", owner=person)
        await a1.asave()

        activity = await (
            TestActivity.aobjects(owner=person).select_related("owner")
            .scalar("id", "owner")
            .first()
        )
        assert activity[0] == a1.pk
        assert activity[1] == person

        activity = await TestActivity.aobjects(owner=person).select_related("owner").only("id", "owner").first()
        assert activity.pk == a1.pk
        assert activity.owner == person

        activity = await (
            TestActivity.aobjects(owner=person).only("id", "owner").as_pymongo().first()
        )
        assert activity["_id"] == a1.pk
        assert activity["owner"]["_ref"], DBRef("test_person", person.pk)

    async def test_scalar_db_field(self):
        class TestDoc(Document):
            x = IntField()
            y = BooleanField()

        await TestDoc.adrop_collection()

        await TestDoc(x=10, y=True).asave()
        await TestDoc(x=20, y=False).asave()
        await TestDoc(x=30, y=True).asave()

        plist = await TestDoc.aobjects.scalar("x", "y").to_list()
        assert len(plist) == 3
        assert plist[0] == (10, True)
        assert plist[1] == (20, False)
        assert plist[2] == (30, True)

    async def test_scalar_primary_key(self):
        class SettingValue(Document):
            key = StringField(primary_key=True)
            value = StringField()

        await SettingValue.adrop_collection()
        s = SettingValue(key="test", value="test value")
        await s.asave()

        val = await SettingValue.aobjects.scalar("key", "value").to_list()
        assert list(val) == [("test", "test value")]

    async def test_fields(self):
        class Bar(EmbeddedDocument):
            v = StringField()
            z = StringField()

        class Foo(Document):
            x = StringField()
            y = IntField()
            items = EmbeddedDocumentListField(Bar)

        await Foo.adrop_collection()

        await Foo(x="foo1", y=1).asave()
        await Foo(x="foo2", y=2, items=[]).asave()
        await Foo(x="foo3", y=3, items=[Bar(z="a", v="V")]).asave()
        await Foo(
            x="foo4",
            y=4,
            items=[
                Bar(z="a", v="V"),
                Bar(z="b", v="W"),
                Bar(z="b", v="X"),
                Bar(z="c", v="V"),
            ],
        ).asave()
        await Foo(
            x="foo5",
            y=5,
            items=[
                Bar(z="b", v="X"),
                Bar(z="c", v="V"),
                Bar(z="d", v="V"),
                Bar(z="e", v="V"),
            ],
        ).asave()

        foos_with_x = await Foo.aobjects.order_by("y").fields(x=1).to_list()

        assert all(o.x is not None for o in foos_with_x)

        foos_without_y = await Foo.aobjects.order_by("y").fields(y=0).to_list()

        assert all(o.y is None for o in foos_without_y)

        foos_with_sliced_items = await Foo.aobjects.order_by("y").fields(slice__items=1).to_list()

        assert foos_with_sliced_items[0].items == []
        assert foos_with_sliced_items[1].items == []
        assert len(foos_with_sliced_items[2].items) == 1
        assert foos_with_sliced_items[2].items[0].z == "a"
        assert len(foos_with_sliced_items[3].items) == 1
        assert foos_with_sliced_items[3].items[0].z == "a"
        assert len(foos_with_sliced_items[4].items) == 1
        assert foos_with_sliced_items[4].items[0].z == "b"

        foos_with_elem_match_items = await Foo.aobjects.order_by("y").fields(elemMatch__items={"z": "b"}).to_list()

        assert foos_with_elem_match_items[0].items == []
        assert foos_with_elem_match_items[1].items == []
        assert foos_with_elem_match_items[2].items == []
        assert len(foos_with_elem_match_items[3].items) == 1
        assert foos_with_elem_match_items[3].items[0].z == "b"
        assert foos_with_elem_match_items[3].items[0].v == "W"
        assert len(foos_with_elem_match_items[4].items) == 1
        assert foos_with_elem_match_items[4].items[0].z == "b"

    async def test_elem_match(self):
        class Foo(EmbeddedDocument):
            shape = StringField()
            color = StringField()
            thick = BooleanField()
            meta = {"allow_inheritance": False}

        class Bar(Document):
            foo = ListField(EmbeddedDocumentField(Foo))
            meta = {"allow_inheritance": False}

        await Bar.adrop_collection()

        b1 = Bar(
            foo=[
                Foo(shape="square", color="purple", thick=False),
                Foo(shape="circle", color="red", thick=True),
            ]
        )
        await b1.asave()

        b2 = Bar(
            foo=[
                Foo(shape="square", color="red", thick=True),
                Foo(shape="circle", color="purple", thick=False),
            ]
        )
        await b2.asave()

        b3 = Bar(
            foo=[
                Foo(shape="square", thick=True),
                Foo(shape="circle", color="purple", thick=False),
            ]
        )
        await b3.asave()

        ak = await Bar.aobjects(foo__match={"shape": "square", "color": "purple"}).to_list()
        assert [b1] == ak

        ak = await Bar.aobjects(foo__elemMatch={"shape": "square", "color": "purple"}).to_list()
        assert [b1] == ak

        ak = await Bar.aobjects(foo__match=Foo(shape="square", color="purple")).to_list()
        assert [b1] == ak

        ak = await Bar.aobjects(foo__elemMatch={"shape": "square", "color__exists": True}).to_list()

        assert [b1, b2] == ak

        ak = await Bar.aobjects(foo__match={"shape": "square", "color__exists": True}).to_list()
        assert [b1, b2] == ak

        ak = await Bar.aobjects(foo__elemMatch={"shape": "square", "color__exists": False}).to_list()

        assert [b3] == ak

        ak = await Bar.aobjects(foo__match={"shape": "square", "color__exists": False}).to_list()
        assert [b3] == ak

    async def test_upsert_includes_cls(self):
        """Upserts should include _cls information for inheritable classes"""

        class Test(Document):
            test = StringField()

        await Test.adrop_collection()
        await Test.aobjects(test="foo").update_one(upsert=True, set__test="foo")
        assert "_cls" not in await (await Test._aget_collection()).find_one()

        class Test(Document):
            meta = {"allow_inheritance": True}
            test = StringField()

        await Test.adrop_collection()

        await Test.aobjects(test="foo").update_one(upsert=True, set__test="foo")
        assert "_cls" in await (await Test._aget_collection()).find_one()

    async def test_update_upsert_looks_like_a_digit(self):
        class MyDoc(DynamicDocument):
            pass

        await MyDoc.adrop_collection()
        assert 1 == await MyDoc.aobjects.update_one(upsert=True, inc__47=1)
        assert (await MyDoc.aobjects.get())["47"] == 1

    async def test_dictfield_key_looks_like_a_digit(self):
        """Only should work with DictField even if they have numeric keys."""

        class MyDoc(Document):
            test = DictField()

        await MyDoc.adrop_collection()
        doc = MyDoc(test={"47": 1})
        await doc.asave()
        assert (await MyDoc.aobjects.only("test__47").get()).test["47"] == 1

    async def test_clear_cls_query(self):
        class Parent(Document):
            name = StringField()
            meta = {"allow_inheritance": True}

        class Child(Parent):
            age = IntField()

        await Parent.adrop_collection()

        # Default query includes the "_cls" check.
        assert Parent.aobjects._query == {"_cls": {"$in": ("Parent", "Parent.Child")}}

        # Clearing the "_cls" query should work.
        assert Parent.aobjects.clear_cls_query()._query == {}

        # Clearing the "_cls" query should not persist across queryset instances.
        assert Parent.aobjects._query == {"_cls": {"$in": ("Parent", "Parent.Child")}}

        # The rest of the query should not be cleared.
        assert Parent.aobjects.filter(name="xyz").clear_cls_query()._query == {
            "name": "xyz"
        }

        await Parent.aobjects.create(name="foo")
        await Child.aobjects.create(name="bar", age=1)
        assert await Parent.aobjects.clear_cls_query().count() == 2
        assert await Parent.aobjects.count() == 2
        assert await Child.aobjects().count() == 1

        # XXX This isn't really how you'd want to use `clear_cls_query()`, but
        # it's a decent test to validate its behavior nonetheless.
        assert await Child.aobjects.clear_cls_query().count() == 2

    async def test_read_preference(self):
        class Bar(Document):
            txt = StringField()

            meta = {"indexes": ["txt"]}

        await Bar.adrop_collection()
        bar = await Bar.aobjects.create(txt="xyz")

        bars = await Bar.aobjects.read_preference(ReadPreference.PRIMARY).to_list()
        assert bars == [bar]

        bars = Bar.aobjects.read_preference(ReadPreference.SECONDARY_PREFERRED)
        assert bars._read_preference == ReadPreference.SECONDARY_PREFERRED
        assert (
                (await bars._cursor).collection.read_preference
                == ReadPreference.SECONDARY_PREFERRED
        )

        # Make sure that `.read_preference(...)` does accept string values.
        with pytest.raises(TypeError):
            Bar.aobjects.read_preference("Primary")

        async def assert_read_pref(qs, expected_read_pref):
            assert qs._read_preference == expected_read_pref
            assert (await qs._cursor).collection.read_preference == expected_read_pref

        # Make sure read preference is respected after a `.skip(...)`.
        bars = Bar.aobjects.skip(1).read_preference(ReadPreference.SECONDARY_PREFERRED)
        await assert_read_pref(bars, ReadPreference.SECONDARY_PREFERRED)

        # Make sure read preference is respected after a `.limit(...)`.
        bars = Bar.aobjects.limit(1).read_preference(ReadPreference.SECONDARY_PREFERRED)
        await assert_read_pref(bars, ReadPreference.SECONDARY_PREFERRED)

        # Make sure read preference is respected after an `.order_by(...)`.
        bars = Bar.aobjects.order_by("txt").read_preference(
            ReadPreference.SECONDARY_PREFERRED
        )
        await assert_read_pref(bars, ReadPreference.SECONDARY_PREFERRED)

        # Make sure read preference is respected after a `.hint(...)`.
        bars = Bar.aobjects.hint([("txt", 1)]).read_preference(
            ReadPreference.SECONDARY_PREFERRED
        )
        await assert_read_pref(bars, ReadPreference.SECONDARY_PREFERRED)

    async def test_read_concern(self):
        class Bar(Document):
            txt = StringField()

            meta = {"indexes": ["txt"]}

        await Bar.adrop_collection()
        bar = await Bar.aobjects.create(txt="xyz")

        bars = await Bar.aobjects.read_concern(None).to_list()
        assert bars == [bar]

        bars = Bar.aobjects.read_concern({"level": "local"})
        assert bars._read_concern.document == {"level": "local"}
        assert (await bars._cursor).collection.read_concern.document == {"level": "local"}

        # Make sure that `.read_concern(...)` does not accept string values.
        with pytest.raises(TypeError):
            Bar.aobjects.read_concern("local")

        async def assert_read_concern(qs, expected_read_concern):
            assert qs._read_concern.document == expected_read_concern
            assert (await qs._cursor).collection.read_concern.document == expected_read_concern

        # Make sure read concern is respected after a `.skip(...)`.
        bars = Bar.aobjects.skip(1).read_concern({"level": "local"})
        await assert_read_concern(bars, {"level": "local"})

        # Make sure read concern is respected after a `.limit(...)`.
        bars = Bar.aobjects.limit(1).read_concern({"level": "local"})
        await assert_read_concern(bars, {"level": "local"})

        # Make sure read concern is respected after an `.order_by(...)`.
        bars = Bar.aobjects.order_by("txt").read_concern({"level": "local"})
        await assert_read_concern(bars, {"level": "local"})

        # Make sure read concern is respected after a `.hint(...)`.
        bars = Bar.aobjects.hint([("txt", 1)]).read_concern({"level": "majority"})
        await assert_read_concern(bars, {"level": "majority"})

    async def test_json_simple(self):
        class Embedded(EmbeddedDocument):
            string = StringField()

        class Doc(Document):
            string = StringField()
            embedded_field = EmbeddedDocumentField(Embedded)

        await Doc.adrop_collection()
        await Doc(string="Hi", embedded_field=Embedded(string="Hi")).asave()
        await Doc(string="Bye", embedded_field=Embedded(string="Bye")).asave()

        await Doc().asave()
        json_data = await Doc.aobjects.to_json(sort_keys=True, separators=(",", ":"))
        doc_objects = await Doc.aobjects.to_list()

        assert doc_objects == Doc.aobjects.from_json(json_data)

    async def test_json_complex(self):
        class EmbeddedDoc(EmbeddedDocument):
            pass

        class Simple(Document):
            pass

        default_ = await Simple().asave()

        class Doc(Document):
            string_field = StringField(default="1")
            int_field = IntField(default=1)
            float_field = FloatField(default=1.1)
            boolean_field = BooleanField(default=True)
            datetime_field = DateTimeField(default=datetime.datetime.now)
            embedded_document_field = EmbeddedDocumentField(
                EmbeddedDoc, default=lambda: EmbeddedDoc()
            )
            list_field = ListField(default=lambda: [1, 2, 3])
            dict_field = DictField(default=lambda: {"hello": "world"})
            objectid_field = ObjectIdField(default=ObjectId)
            reference_field = ReferenceField(Simple, default=default_)
            map_field = MapField(IntField(), default=lambda: {"simple": 1})
            decimal_field = DecimalField(default=1.0)
            complex_datetime_field = ComplexDateTimeField(default=datetime.datetime.now)
            url_field = URLField(default="http://mongoengine.org")
            dynamic_field = DynamicField(default=1)
            generic_reference_field = GenericReferenceField(
                default=default_, choices=(
                    Simple,
                )
            )
            sorted_list_field = SortedListField(IntField(), default=lambda: [1, 2, 3])
            email_field = EmailField(default="ross@example.com")
            geo_point_field = GeoPointField(default=lambda: [1, 2])
            sequence_field = SequenceField()
            uuid_field = UUIDField(default=uuid.uuid4)
            generic_embedded_document_field = GenericEmbeddedDocumentField(
                default=lambda: EmbeddedDoc()
            )

        await Simple.adrop_collection()
        await Doc.adrop_collection()

        await Doc().asave()
        json_data = await Doc.aobjects.to_json()
        doc_objects = await Doc.aobjects.to_list()
        docs_json = Doc.aobjects.from_json(json_data)
        assert doc_objects[0].pk == docs_json[0].pk

    async def test_as_pymongo(self):
        class LastLogin(EmbeddedDocument):
            location = StringField()
            ip = StringField()

        class User(Document):
            id = StringField(primary_key=True)
            name = StringField()
            age = IntField()
            price = DecimalField()
            last_login = EmbeddedDocumentField(LastLogin)

        await User.adrop_collection()

        await User.aobjects.create(id="Bob", name="Bob Dole", age=89, price=Decimal("1.11"))
        await User.aobjects.create(
            id="Barak",
            name="Barak Obama",
            age=51,
            price=Decimal("2.22"),
            last_login=LastLogin(location="White House", ip="104.107.108.116"),
        )

        results = await User.aobjects.as_pymongo().to_list()
        assert set(results[0].keys()) == {"_id", "name", "age", "price"}
        assert set(results[1].keys()) == {"_id", "name", "age", "price", "last_login"}

        results = await User.aobjects.only("id", "name").as_pymongo().to_list()
        assert set(results[0].keys()) == {"_id", "name"}

        results = await User.aobjects.only("name", "price").as_pymongo().to_list()
        assert isinstance(results[0], dict)
        assert isinstance(results[1], dict)
        assert results[0]["name"] == "Bob Dole"
        assert results[0]["price"] == 1.11
        assert results[1]["name"] == "Barak Obama"
        assert results[1]["price"] == 2.22

        results = await User.aobjects.only("name", "last_login").as_pymongo().to_list()
        assert isinstance(results[0], dict)
        assert isinstance(results[1], dict)
        assert results[0] == {"_id": "Bob", "name": "Bob Dole"}
        assert results[1] == {
            "_id": "Barak",
            "name": "Barak Obama",
            "last_login": {"location": "White House", "ip": "104.107.108.116"},
        }

    async def test_as_pymongo_returns_cls_attribute_when_using_inheritance(self):
        class User(Document):
            name = StringField()
            meta = {"allow_inheritance": True}

        await User.adrop_collection()

        user = await User(name="Bob Dole").asave()
        result = await User.aobjects.as_pymongo().first()
        assert result == {"_cls": "User", "_id": user.id, "name": "Bob Dole"}

    async def test_as_pymongo_json_limit_fields(self):
        class User(Document):
            email = EmailField(unique=True, required=True)
            password_hash = StringField(db_field="password_hash", required=True)
            password_salt = StringField(db_field="password_salt", required=True)

        await User.adrop_collection()
        await User(
            email="ross@example.com", password_salt="SomeSalt", password_hash="SomeHash"
        ).asave()

        # serialized_user = (await User.aobjects.exclude(
        #     "password_salt", "password_hash"
        # ).as_pymongo().to_list())[0]
        # assert {"_id", "email"} == set(serialized_user.keys())
        #
        # serialized_user = await User.aobjects.exclude(
        #     "id", "password_salt", "password_hash"
        # ).to_json()
        # assert '[{"email": "ross@example.com"}]' == serialized_user
        #
        # serialized_user = (await User.aobjects.only("email").as_pymongo().to_list())[0]
        # assert {"_id", "email"} == set(serialized_user.keys())
        #
        # serialized_user = (
        #     (await User.aobjects.exclude("password_salt").only("email").as_pymongo().to_list())[0]
        # )
        # assert {"_id", "email"} == set(serialized_user.keys())

        serialized_user = (
            (await User.aobjects.exclude("password_salt", "id").only("email").as_pymongo().to_list())[0]
        )
        assert {"email"} == set(serialized_user.keys())

        serialized_user = (
            await User.aobjects.exclude("password_salt", "id").only("email").to_json()
        )
        assert '[{"email": "ross@example.com"}]' == serialized_user

    async def test_only_after_count(self):
        """Test that only() works after count()"""

        class User(Document):
            name = StringField()
            age = IntField()
            address = StringField()

        await User.adrop_collection()
        user = await User(name="User", age=50, address="Moscow, Russia").asave()

        user_queryset = User.aobjects(age=50)

        result = await user_queryset.only("name", "age").as_pymongo().first()
        assert result == {"_id": user.id, "name": "User", "age": 50}

        result = await user_queryset.count()
        assert result == 1

        result = await user_queryset.only("name", "age").as_pymongo().first()
        assert result == {"_id": user.id, "name": "User", "age": 50}

    async def test_no_dereference(self):
        class Organization(Document):
            name = StringField()

        class User(Document):
            name = StringField()
            organization = ReferenceField(Organization)

        await User.adrop_collection()
        await Organization.adrop_collection()

        whitehouse = await Organization(name="White House").asave()
        await User(name="Bob Dole", organization=whitehouse).asave()

        qs = User.aobjects()
        qs_user = await qs.first()

        assert isinstance((await qs.first()).organization, DBRef)

        user = await qs.first()
        assert isinstance(user.organization, DBRef)

        assert isinstance(qs_user.organization, DBRef)
        assert isinstance((await qs.select_related("organization").first()).organization, Organization)

    async def test_no_dereference_no_side_effect_on_existing_instance(self):
        # Relates to issue #1677 - ensures no regression of the bug

        class Organization(Document):
            name = StringField()

        class User(Document):
            organization = ReferenceField(Organization)
            organization_gen = GenericReferenceField(choices=(Organization,))

        await User.adrop_collection()
        await Organization.adrop_collection()

        org = await Organization(name="whatever").asave()
        await User(organization=org, organization_gen=org).asave()

        qs = User.aobjects().select_related("organization", "organization_gen")
        user = await qs.first()

        qs_no_deref = User.aobjects()
        user_no_deref = await qs_no_deref.first()

        # ReferenceField
        no_derf_org = user_no_deref.organization
        assert isinstance(no_derf_org, LazyReference)
        assert isinstance(user.organization, Organization)

        # GenericReferenceField
        no_derf_org_gen = user_no_deref.organization_gen
        assert isinstance(no_derf_org_gen, LazyReference)
        assert isinstance(user.organization_gen, Organization)

    async def test_no_dereference_embedded_doc(self):
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
            admins = ListField(ReferenceField(User))

        await Organization.adrop_collection()
        await User.adrop_collection()

        user = User(name="Flash")
        await user.asave()

        member = Member(name="Flash", user=user)

        company = Organization(
            name="Mongo Inc", ceo=user, member=member, admins=[user], members=[member]
        )
        await company.asave()

        org = await Organization.aobjects().first()

        assert id(org._fields["admins"]) == id(Organization.admins)

        admin = org.admins[0]
        assert isinstance(admin, DBRef)
        assert isinstance(org.member.user, DBRef)
        assert isinstance(org.members[0].user, DBRef)

    async def test_cached_queryset(self):
        class Person(Document):
            name = StringField()

        await Person.adrop_collection()

        persons = [Person(name="No: %s" % i) for i in range(100)]
        await Person.aobjects.insert(persons, load_bulk=True)

        async with async_query_counter() as q:
            assert q.eq(0)
            people = Person.aobjects

            [x async for x in people]
            assert 100 == len(people._result_cache)

            import platform

            if platform.python_implementation() != "PyPy":
                # PyPy evaluates __len__ when iterating with list comprehensions while CPython does not.
                # This may be a bug in PyPy (PyPy/#1802) but it does not affect
                # the behavior of MongoEngine.
                assert people._len is None
            assert q.eq(1)

            assert 100 == len(await people.to_list())  # Caused by list calling len
            assert q.eq(1)

            await people.count(with_limit_and_skip=True)  # count is cached
            assert q.eq(1)

    async def test_no_cached_queryset(self):
        class Person(Document):
            name = StringField()

        await Person.adrop_collection()

        persons = [Person(name="No: %s" % i) for i in range(100)]
        await Person.aobjects.insert(persons, load_bulk=True)

        async with async_query_counter() as q:
            assert q.eq(0)
            people = await Person.aobjects.no_cache()

            [x async for x in people]
            assert q.eq(1)

            await Person.aobjects.to_list()
            assert q.eq(2)

            await Person.aobjects.count()
            assert q.eq(3)

    async def test_no_cached_queryset__repr__(self):
        class Person(Document):
            name = StringField()

        await Person.adrop_collection()
        qs = await Person.aobjects.no_cache()
        assert repr(qs) == '<AsyncQuerySetNoCache (repr not supported; use async methods)>'

    async def test_no_cached_on_a_cached_queryset_raise_error(self):
        class Person(Document):
            name = StringField()

        await Person.adrop_collection()
        await Person(name="a").asave()
        qs = Person.aobjects()
        _ = await qs.to_list()
        with pytest.raises(OperationError, match="QuerySet already cached"):
            await qs.no_cache()

    async def test_no_cached_queryset_no_cache_back_to_cache(self):
        class Person(Document):
            name = StringField()

        await Person.adrop_collection()
        qs = Person.aobjects()
        assert isinstance(qs, AsyncQuerySet)
        qs = await qs.no_cache()
        assert isinstance(qs, AsyncQuerySetNoCache)
        qs = await qs.cache()
        assert isinstance(qs, AsyncQuerySet)

    async def test_cache_not_cloned(self):
        class User(Document):
            name = StringField()

            def __unicode__(self):
                return self.name

        await User.adrop_collection()

        await User(name="Alice").asave()
        await User(name="Bob").asave()

        users = User.aobjects.all().order_by("name")
        assert "%s" % await users.to_list() == "[<User: Alice>, <User: Bob>]"
        assert 2 == len(users._result_cache)

        users = users.filter(name="Bob")
        assert "%s" % await users.to_list() == "[<User: Bob>]"
        assert 1 == len(users._result_cache)

    async def test_no_cache(self):
        """Ensure you can add metadata to file"""

        class Noddy(Document):
            fields = DictField()

        await Noddy.adrop_collection()

        noddies = []
        for i in range(100):
            noddy = Noddy()
            for j in range(20):
                noddy.fields["key" + str(j)] = "value " + str(j)
            noddies.append(noddy)
        await Noddy.aobjects.insert(noddies, load_bulk=True)

        docs = await Noddy.aobjects.no_cache()

        counter = len([1 async for i in docs])
        assert counter == 100

        assert len(await docs.to_list()) == 100

        # Can't directly get a length of a no-cache queryset.
        with pytest.raises(TypeError):
            len(docs)

        # Another iteration over the queryset should result in another db op.
        async with async_query_counter() as q:
            await docs.to_list()
            assert q.eq(1)

        # ... and another one to double-check.
        async with async_query_counter() as q:
            await docs.to_list()
            assert q.eq(1)

    async def test_nested_queryset_iterator(self):
        # Try iterating the same queryset twice, nested.
        names = ["Alice", "Bob", "Chuck", "David", "Eric", "Francis", "George"]

        class User(Document):
            name = StringField()

            def __unicode__(self):
                return self.name

        await User.adrop_collection()

        for name in names:
            await User(name=name).asave()

        users = User.aobjects.all().order_by("name")
        outer_count = 0
        inner_count = 0
        inner_total_count = 0

        async with async_query_counter() as q:
            assert q.eq(0)

            assert await users.count(with_limit_and_skip=True) == 7

            for i, outer_user in enumerate(await users.to_list()):
                assert outer_user.name == names[i]
                outer_count += 1
                inner_count = 0

                # Calling len might disrupt the inner loop if there are bugs
                assert await users.count(with_limit_and_skip=True) == 7

                for j, inner_user in enumerate(await users.to_list()):
                    assert inner_user.name == names[j]
                    inner_count += 1
                    inner_total_count += 1

                # inner loop should always be executed seven times
                assert inner_count == 7

            # outer loop should be executed seven times total
            assert outer_count == 7
            # inner loop should be executed fourtynine times total
            assert inner_total_count == 7 * 7

            assert q.eq(2)

    async def test_no_sub_classes(self):
        class A(Document):
            x = IntField()
            y = IntField()

            meta = {"allow_inheritance": True}

        class B(A):
            z = IntField()

        class C(B):
            zz = IntField()

        await A.adrop_collection()

        await A(x=10, y=20).asave()
        await A(x=15, y=30).asave()
        await B(x=20, y=40).asave()
        await B(x=30, y=50).asave()
        await C(x=40, y=60).asave()

        assert await A.aobjects.no_sub_classes().count() == 2
        assert await A.aobjects.count() == 5

        assert await B.aobjects.no_sub_classes().count() == 2
        assert await B.aobjects.count() == 3

        assert await C.aobjects.no_sub_classes().count() == 1
        assert await C.aobjects.count() == 1

        async for obj in A.aobjects.no_sub_classes():
            assert obj.__class__ == A

        async for obj in B.aobjects.no_sub_classes():
            assert obj.__class__ == B

        async for obj in C.aobjects.no_sub_classes():
            assert obj.__class__ == C

    async def test_query_generic_embedded_document(self):
        """Ensure that querying sub field on generic_embedded_field works"""

        class A(EmbeddedDocument):
            a_name = StringField()

        class B(EmbeddedDocument):
            b_name = StringField()

        class Doc(Document):
            document = GenericEmbeddedDocumentField(choices=(A, B))

        await Doc.adrop_collection()
        await Doc(document=A(a_name="A doc")).asave()
        await Doc(document=B(b_name="B doc")).asave()

        # Using raw in filter working fine
        assert await Doc.aobjects(__raw__={"document.a_name": "A doc"}).count() == 1
        assert await Doc.aobjects(__raw__={"document.b_name": "B doc"}).count() == 1
        assert await Doc.aobjects(document__a_name="A doc").count() == 1
        assert await Doc.aobjects(document__b_name="B doc").count() == 1

    async def test_query_reference_to_custom_pk_doc(self):
        class A(Document):
            id = StringField(primary_key=True)

        class B(Document):
            a = ReferenceField(A)

        await A.adrop_collection()
        await B.adrop_collection()

        a = await A.aobjects.create(id="custom_id")
        await B.aobjects.create(a=a)

        assert await B.aobjects.count() == 1
        assert (await B.aobjects.get(a=a)).a == a
        assert (await B.aobjects.get(a=a.id)).a == a

    async def test_cls_query_in_subclassed_docs(self):
        class Animal(Document):
            name = StringField()

            meta = {"allow_inheritance": True}

        class Dog(Animal):
            pass

        class Cat(Animal):
            pass

        assert Animal.aobjects(name="Charlie")._query == {
            "name": "Charlie",
            "_cls": {"$in": ("Animal", "Animal.Dog", "Animal.Cat")},
        }
        assert Dog.aobjects(name="Charlie")._query == {
            "name": "Charlie",
            "_cls": "Animal.Dog",
        }
        assert Cat.aobjects(name="Charlie")._query == {
            "name": "Charlie",
            "_cls": "Animal.Cat",
        }

    async def test_can_have_field_same_name_as_query_operator(self):
        class Size(Document):
            name = StringField()

        class Product(EmbeddedDocument):
            name = StringField()

        class Example(Document):
            size = ReferenceField(Size)
            product = EmbeddedDocumentField(Product)

        await Size.adrop_collection()
        await Example.adrop_collection()

        instance_size = await Size(name="Large").asave()
        product = Product(name="iPhone")
        await Example(size=instance_size, product=Product(name="iPhone")).asave()

        assert await Example.aobjects(size=instance_size).count() == 1
        assert await Example.aobjects(product=product).count() == 1
        assert await Example.aobjects(size__in=[instance_size]).count() == 1
        assert await Example.aobjects(product__in=[product]).count() == 1

    async def test_cursor_in_an_if_stmt(self):
        class Test(Document):
            test_field = StringField()

        await Test.adrop_collection()
        queryset = Test.aobjects

        if await queryset.exists():
            raise AssertionError("Empty cursor returns True")

        test = Test()
        test.test_field = "test"
        await test.asave()

        queryset = Test.aobjects
        if not test:
            raise AssertionError("Cursor has data and returned False")

        anext(queryset)
        if not queryset.exists():
            raise AssertionError(
                "Cursor has data and it must returns True, even in the last item."
            )

    async def test_bool_performance(self):
        class Person(Document):
            name = StringField()

        await Person.adrop_collection()

        persons = [Person(name="No: %s" % i) for i in range(100)]
        await Person.aobjects.insert(persons, load_bulk=True)

        async with async_query_counter() as q:
            if await Person.aobjects.exists():
                pass

            assert q.eq(1)
            cursor = (await q.db).system.profile.find(
                {"ns": {"$ne": f"{(await q.db).name}.system.indexes"}}
            )

            docs = await cursor.to_list(length=1)
            op = docs[0] if docs else None
            assert op["nreturned"] == 1

    async def test_bool_with_ordering(self):
        ORDER_BY_KEY, CMD_QUERY_KEY = get_key_compat(self.mongodb_version)

        class Person(Document):
            name = StringField()

        await Person.adrop_collection()

        await Person(name="Test").asave()

        # Check that bool(queryset) does not uses the orderby
        qs = Person.aobjects.order_by("name")
        async with async_query_counter() as q:
            if await qs.exists():
                pass

            cursor = (await q.db).system.profile.find(
                {"ns": {"$ne": "%s.system.indexes" % (await q.db).name}}
            )
            docs = await cursor.to_list(length=1)
            op = docs[0] if docs else None

            assert ORDER_BY_KEY not in op[CMD_QUERY_KEY]

        # Check that normal query uses orderby
        qs2 = Person.aobjects.order_by("name")

        async with async_query_counter() as q:
            async for x in qs2:
                pass

            cursor = (await q.db).system.profile.find(
                {"ns": {"$ne": "%s.system.indexes" % (await q.db).name}}
            )
            docs = await cursor.to_list(length=1)
            op = docs[0] if docs else None

            # FIX: normal query MUST use ordering
            assert ORDER_BY_KEY in op[CMD_QUERY_KEY]

    async def test_bool_with_ordering_from_meta_dict(self):
        ORDER_BY_KEY, CMD_QUERY_KEY = get_key_compat(self.mongodb_version)

        class Person(Document):
            name = StringField()
            meta = {"ordering": ["name"]}

        await Person.adrop_collection()

        await Person(name="B").asave()
        await Person(name="C").asave()
        await Person(name="A").asave()

        async with async_query_counter() as q:
            if await Person.aobjects.exists():
                pass

            cursor = (await q.db).system.profile.find(
                {"ns": {"$ne": f"{(await q.db).name}.system.indexes"}}
            )

            docs = await cursor.to_list(length=1)
            op = docs[0] if docs else None

            assert (
                    "$orderby" not in op[CMD_QUERY_KEY]
            ), "BaseQuerySet must remove orderby from meta in boolen test"

            assert (await Person.aobjects.first()).name == "A"
            assert await Person.aobjects._has_data(), "Cursor has data and returned False"

    async def test_delete_count(self):
        [await self.Person(name=f"User {i}", age=i * 10).asave() for i in range(1, 4)]
        assert (
                await self.Person.aobjects().delete() == 3
        )  # test ordinary QuerySey delete count

        [await self.Person(name=f"User {i}", age=i * 10).asave() for i in range(1, 4)]

        assert (
                await self.Person.aobjects().skip(1).delete() == 2
        )  # test Document delete with existing documents

        await self.Person.aobjects().delete()
        assert (
                await self.Person.aobjects().skip(1).delete() == 0
        )  # test Document delete without existing documents

    async def test_max_time_ms(self):
        # 778: max_time_ms can get only int or None as input
        with pytest.raises(TypeError):
            await self.Person.aobjects(name="name").max_time_ms("not a number").first()

    async def test_subclass_field_query(self):
        class Animal(Document):
            is_mamal = BooleanField()
            meta = {"allow_inheritance": True}

        class Cat(Animal):
            whiskers_length = FloatField()

        class ScottishCat(Cat):
            folded_ears = BooleanField()

        await Animal.adrop_collection()

        await Animal(is_mamal=False).asave()
        await Cat(is_mamal=True, whiskers_length=5.1).asave()
        await ScottishCat(is_mamal=True, folded_ears=True).asave()
        assert await Animal.aobjects(folded_ears=True).count() == 1
        assert await Animal.aobjects(whiskers_length=5.1).count() == 1

    async def test_loop_over_invalid_id_does_not_crash(self):
        class Person(Document):
            name = StringField()

        await Person.adrop_collection()

        await (await Person._aget_collection()).insert_one({"name": "a", "id": ""})
        async for p in Person.aobjects():
            assert p.name == "a"

    async def test_len_during_iteration(self):
        """Tests that calling len on a queyset during iteration doesn't
        stop paging.
        """

        class Data(Document):
            pass

        for i in range(300):
            await Data().asave()

        records = await Data.aobjects.limit(250).to_list()

        # This should pull all 250 docs from mongo and populate the result
        # cache
        len(records)

        # Assert that iterating over documents in the qs touches every
        # document even if we call len(qs) midway through the iteration.
        for i, r in enumerate(records):
            if i == 58:
                len(records)
        assert i == 249

        # Assert the same behavior is true even if we didn't pre-populate the
        # result cache.
        records = await Data.aobjects.limit(250).to_list()
        for i, r in enumerate(records):
            if i == 58:
                len(records)
        assert i == 249

    async def test_iteration_within_iteration(self):
        """You should be able to reliably iterate over all the documents
        in a given queryset even if there are multiple iterations of it
        happening at the same time.
        """

        class Data(Document):
            pass

        for i in range(300):
            await Data().asave()

        qs = await Data.aobjects.limit(250).to_list()
        for i, doc in enumerate(qs):
            for j, doc2 in enumerate(qs):
                pass

        assert i == 249
        assert j == 249

    async def test_in_operator_on_non_iterable(self):
        """Ensure that using the `__in` operator on a non-iterable raises an
        error.
        """

        class User(Document):
            name = StringField()

        class BlogPost(Document):
            content = StringField()
            authors = ListField(ReferenceField(User))

        await User.adrop_collection()
        await BlogPost.adrop_collection()

        author = await User.aobjects.create(name="Test User")
        post = await BlogPost.aobjects.create(
            content="Had a good coffee today...", authors=[author]
        )

        # Make sure using `__in` with a list works
        blog_posts = await BlogPost.aobjects(authors__in=[author]).to_list()
        assert blog_posts == [post]

        # Using `__in` with a non-iterable should raise a TypeError
        with pytest.raises(TypeError):
            await BlogPost.aobjects(authors__in=author.pk).count()

        # Using `__in` with a `Document` (which is seemingly iterable but not
        # in a way we'd expect) should raise a TypeError, too
        with pytest.raises(TypeError):
            await BlogPost.aobjects(authors__in=author).count()

    async def test_create_count(self):
        await self.Person.adrop_collection()
        await self.Person.aobjects.create(name="Foo")
        await self.Person.aobjects.create(name="Bar")
        await self.Person.aobjects.create(name="Baz")
        assert await self.Person.aobjects.count(with_limit_and_skip=True) == 3

        await self.Person.aobjects.create(name="Foo_1")
        assert await self.Person.aobjects.count(with_limit_and_skip=True) == 4

    async def test_no_cursor_timeout(self):
        qs = self.Person.aobjects()
        assert qs._cursor_args == {}  # ensure no regression of  #2148

        qs = self.Person.aobjects().timeout(True)
        assert qs._cursor_args == {}

        qs = self.Person.aobjects().timeout(False)
        assert qs._cursor_args == {"no_cursor_timeout": True}

    async def test_allow_disk_use(self):
        qs = self.Person.aobjects()
        assert qs._cursor_args == {}

        qs = self.Person.aobjects().allow_disk_use(False)
        assert qs._cursor_args == {}

        qs = self.Person.aobjects().allow_disk_use(True)
        assert qs._cursor_args == {"allow_disk_use": True}

        # Test if allow_disk_use changes the results
        await self.Person.adrop_collection()
        await self.Person.aobjects.create(name="Foo", age=12)
        await self.Person.aobjects.create(name="Baz", age=17)
        await self.Person.aobjects.create(name="Bar", age=13)

        qs_disk = self.Person.aobjects().order_by("age").allow_disk_use(True)
        qs = self.Person.aobjects().order_by("age")

        assert await qs_disk.count() == await qs.count()

        for index in range(await qs_disk.count()):
            assert await qs_disk.skip(index).first() == await qs.skip(index).first()
