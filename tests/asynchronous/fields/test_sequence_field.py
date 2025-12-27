from mongoengine import *
from tests.asynchronous.utils import MongoDBAsyncTestCase


class TestSequenceField(MongoDBAsyncTestCase):
    async def test_sequence_field(self):
        class Person(Document):
            id = SequenceField(primary_key=True)
            name = StringField()

        await self.db["mongoengine.counters"].drop()
        await Person.adrop_collection()

        for x in range(10):
            await Person(name="Person %s" % x).asave()

        c = await self.db["mongoengine.counters"].find_one({"_id": "person.id"})
        assert c["next"] == 10

        ids = [i.id async for i in Person.aobjects]
        assert ids == list(range(1, 11))

        c = await self.db["mongoengine.counters"].find_one({"_id": "person.id"})
        assert c["next"] == 10

        await Person.id.aset_next_value(1000)
        c = await self.db["mongoengine.counters"].find_one({"_id": "person.id"})
        assert c["next"] == 1000

    async def test_sequence_field_get_next_value(self):
        class Person(Document):
            id = SequenceField(primary_key=True)
            name = StringField()

        await self.db["mongoengine.counters"].drop()
        await Person.adrop_collection()

        for x in range(10):
            await Person(name="Person %s" % x).asave()

        assert await Person.id.aget_next_value() == 11
        await self.db["mongoengine.counters"].drop()

        assert await Person.id.aget_next_value() == 1

        class Person(Document):
            id = SequenceField(primary_key=True, value_decorator=str)
            name = StringField()

        await self.db["mongoengine.counters"].drop()
        await Person.adrop_collection()

        for x in range(10):
            await Person(name="Person %s" % x).asave()

        assert await Person.id.aget_next_value() == "11"
        await self.db["mongoengine.counters"].drop()

        assert await Person.id.aget_next_value() == "1"

    async def test_sequence_field_sequence_name(self):
        class Person(Document):
            id = SequenceField(primary_key=True, sequence_name="jelly")
            name = StringField()

        await self.db["mongoengine.counters"].drop()
        await Person.adrop_collection()

        for x in range(10):
            await Person(name="Person %s" % x).asave()

        c = await self.db["mongoengine.counters"].find_one({"_id": "jelly.id"})
        assert c["next"] == 10

        ids = [i.id async for i in Person.aobjects]
        assert ids == list(range(1, 11))

        c = await self.db["mongoengine.counters"].find_one({"_id": "jelly.id"})
        assert c["next"] == 10

        await Person.id.aset_next_value(1000)
        c = await self.db["mongoengine.counters"].find_one({"_id": "jelly.id"})
        assert c["next"] == 1000

    async def test_multiple_sequence_fields(self):
        class Person(Document):
            id = SequenceField(primary_key=True)
            counter = SequenceField()
            name = StringField()

        await self.db["mongoengine.counters"].drop()
        await Person.adrop_collection()

        for x in range(10):
            await Person(name="Person %s" % x).asave()

        c = await self.db["mongoengine.counters"].find_one({"_id": "person.id"})
        assert c["next"] == 10

        ids = [i.id async for i in Person.aobjects]
        assert ids == list(range(1, 11))

        counters = [i.counter async for i in Person.aobjects]
        assert counters == list(range(1, 11))

        c = await self.db["mongoengine.counters"].find_one({"_id": "person.id"})
        assert c["next"] == 10

        await Person.id.aset_next_value(1000)
        c = await self.db["mongoengine.counters"].find_one({"_id": "person.id"})
        assert c["next"] == 1000

        await Person.counter.aset_next_value(999)
        c = await self.db["mongoengine.counters"].find_one({"_id": "person.counter"})
        assert c["next"] == 999

    async def test_sequence_fields_reload(self):
        class Animal(Document):
            counter = SequenceField()
            name = StringField()

        await self.db["mongoengine.counters"].drop()
        await Animal.adrop_collection()

        a = await Animal(name="Boi").asave()

        assert a.counter == 1
        await a.areload()
        assert a.counter == 1

        a.counter = None
        await a.asave()
        assert a.counter == 2

        a = await Animal.aobjects.first()
        assert a.counter == 2
        await a.areload()
        assert a.counter == 2

    async def test_multiple_sequence_fields_on_docs(self):
        class Animal(Document):
            id = SequenceField(primary_key=True)
            name = StringField()

        class Person(Document):
            id = SequenceField(primary_key=True)
            name = StringField()

        await self.db["mongoengine.counters"].drop()
        await Animal.adrop_collection()
        await Person.adrop_collection()

        for x in range(10):
            await Animal(name="Animal %s" % x).asave()
            await Person(name="Person %s" % x).asave()

        c = await self.db["mongoengine.counters"].find_one({"_id": "person.id"})
        assert c["next"] == 10

        c = await self.db["mongoengine.counters"].find_one({"_id": "animal.id"})
        assert c["next"] == 10

        ids = [i.id async for i in Person.aobjects]
        assert ids == list(range(1, 11))

        _id = [i.id async for i in Animal.aobjects]
        assert _id == list(range(1, 11))

        c = await self.db["mongoengine.counters"].find_one({"_id": "person.id"})
        assert c["next"] == 10

        c = await self.db["mongoengine.counters"].find_one({"_id": "animal.id"})
        assert c["next"] == 10

    async def test_sequence_field_value_decorator(self):
        class Person(Document):
            id = SequenceField(primary_key=True, value_decorator=str)
            name = StringField()

        await self.db["mongoengine.counters"].drop()
        await Person.adrop_collection()

        for x in range(10):
            p = Person(name="Person %s" % x)
            await p.asave()

        c = await self.db["mongoengine.counters"].find_one({"_id": "person.id"})
        assert c["next"] == 10

        ids = [i.id async for i in Person.aobjects]
        assert ids == [str(i) for i in range(1, 11)]

        c = await self.db["mongoengine.counters"].find_one({"_id": "person.id"})
        assert c["next"] == 10

    async def test_embedded_sequence_field(self):
        class Comment(EmbeddedDocument):
            id = SequenceField()
            content = StringField(required=True)

        class Post(Document):
            title = StringField(required=True)
            comments = ListField(EmbeddedDocumentField(Comment))

        await self.db["mongoengine.counters"].drop()
        await Post.adrop_collection()

        await Post(
            title="MongoEngine",
            comments=[
                Comment(content="NoSQL Rocks"),
                Comment(content="MongoEngine Rocks"),
            ],
        ).asave()
        c = await self.db["mongoengine.counters"].find_one({"_id": "comment.id"})
        assert c["next"] == 2
        post = await Post.aobjects.first()
        assert 1 == post.comments[0].id
        assert 2 == post.comments[1].id

    async def test_inherited_sequencefield(self):
        class Base(Document):
            name = StringField()
            counter = SequenceField()
            meta = {"abstract": True}

        class Foo(Base):
            pass

        class Bar(Base):
            pass

        bar = Bar(name="Bar")
        await bar.asave()

        foo = Foo(name="Foo")
        await foo.asave()

        assert "base.counter" in await self.db["mongoengine.counters"].find().distinct("_id")
        assert not (
                ("foo.counter" or "bar.counter")
                in await self.db["mongoengine.counters"].find().distinct("_id")
        )
        assert foo.counter != bar.counter
        assert foo._fields["counter"].owner_document == Base
        assert bar._fields["counter"].owner_document == Base

    async def test_no_inherited_sequencefield(self):
        class Base(Document):
            name = StringField()
            meta = {"abstract": True}

        class Foo(Base):
            counter = SequenceField()

        class Bar(Base):
            counter = SequenceField()

        bar = Bar(name="Bar")
        await bar.asave()

        foo = Foo(name="Foo")
        await foo.asave()

        assert "base.counter" not in await self.db["mongoengine.counters"].find().distinct(
            "_id"
        )
        existing_counters = await self.db["mongoengine.counters"].find().distinct("_id")
        assert "foo.counter" in existing_counters
        assert "bar.counter" in existing_counters
        assert foo.counter == bar.counter
        assert foo._fields["counter"].owner_document == Foo
        assert bar._fields["counter"].owner_document == Bar

    async def test_sequence_setattr_not_incrementing_counter(self):
        class Person(DynamicDocument):
            id = SequenceField(primary_key=True)
            name = StringField()

        await self.db["mongoengine.counters"].drop()
        await Person.adrop_collection()

        for x in range(10):
            await Person(name="Person %s" % x).asave()

        c = await self.db["mongoengine.counters"].find_one({"_id": "person.id"})
        assert c["next"] == 10

        # Setting SequenceField field value should not increment counter:
        new_person = Person()
        new_person.id = 1100

        # Counter should still be at 10
        c = await self.db["mongoengine.counters"].find_one({"_id": "person.id"})
        assert c["next"] == 10
