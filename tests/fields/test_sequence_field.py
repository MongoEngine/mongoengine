from mongoengine import *

from tests.utils import MongoDBTestCase


class TestSequenceField(MongoDBTestCase):
    def test_sequence_field(self):
        class Person(Document):
            id = SequenceField(primary_key=True)
            name = StringField()

        self.db["mongoengine.counters"].drop()
        Person.drop_collection()

        for x in range(10):
            Person(name="Person %s" % x).save()

        c = self.db["mongoengine.counters"].find_one({"_id": "person.id"})
        assert c["next"] == 10

        ids = [i.id for i in Person.objects]
        assert ids == list(range(1, 11))

        c = self.db["mongoengine.counters"].find_one({"_id": "person.id"})
        assert c["next"] == 10

        Person.id.set_next_value(1000)
        c = self.db["mongoengine.counters"].find_one({"_id": "person.id"})
        assert c["next"] == 1000

    def test_sequence_field_get_next_value(self):
        class Person(Document):
            id = SequenceField(primary_key=True)
            name = StringField()

        self.db["mongoengine.counters"].drop()
        Person.drop_collection()

        for x in range(10):
            Person(name="Person %s" % x).save()

        assert Person.id.get_next_value() == 11
        self.db["mongoengine.counters"].drop()

        assert Person.id.get_next_value() == 1

        class Person(Document):
            id = SequenceField(primary_key=True, value_decorator=str)
            name = StringField()

        self.db["mongoengine.counters"].drop()
        Person.drop_collection()

        for x in range(10):
            Person(name="Person %s" % x).save()

        assert Person.id.get_next_value() == "11"
        self.db["mongoengine.counters"].drop()

        assert Person.id.get_next_value() == "1"

    def test_sequence_field_sequence_name(self):
        class Person(Document):
            id = SequenceField(primary_key=True, sequence_name="jelly")
            name = StringField()

        self.db["mongoengine.counters"].drop()
        Person.drop_collection()

        for x in range(10):
            Person(name="Person %s" % x).save()

        c = self.db["mongoengine.counters"].find_one({"_id": "jelly.id"})
        assert c["next"] == 10

        ids = [i.id for i in Person.objects]
        assert ids == list(range(1, 11))

        c = self.db["mongoengine.counters"].find_one({"_id": "jelly.id"})
        assert c["next"] == 10

        Person.id.set_next_value(1000)
        c = self.db["mongoengine.counters"].find_one({"_id": "jelly.id"})
        assert c["next"] == 1000

    def test_multiple_sequence_fields(self):
        class Person(Document):
            id = SequenceField(primary_key=True)
            counter = SequenceField()
            name = StringField()

        self.db["mongoengine.counters"].drop()
        Person.drop_collection()

        for x in range(10):
            Person(name="Person %s" % x).save()

        c = self.db["mongoengine.counters"].find_one({"_id": "person.id"})
        assert c["next"] == 10

        ids = [i.id for i in Person.objects]
        assert ids == list(range(1, 11))

        counters = [i.counter for i in Person.objects]
        assert counters == list(range(1, 11))

        c = self.db["mongoengine.counters"].find_one({"_id": "person.id"})
        assert c["next"] == 10

        Person.id.set_next_value(1000)
        c = self.db["mongoengine.counters"].find_one({"_id": "person.id"})
        assert c["next"] == 1000

        Person.counter.set_next_value(999)
        c = self.db["mongoengine.counters"].find_one({"_id": "person.counter"})
        assert c["next"] == 999

    def test_sequence_fields_reload(self):
        class Animal(Document):
            counter = SequenceField()
            name = StringField()

        self.db["mongoengine.counters"].drop()
        Animal.drop_collection()

        a = Animal(name="Boi").save()

        assert a.counter == 1
        a.reload()
        assert a.counter == 1

        a.counter = None
        assert a.counter == 2
        a.save()

        assert a.counter == 2

        a = Animal.objects.first()
        assert a.counter == 2
        a.reload()
        assert a.counter == 2

    def test_multiple_sequence_fields_on_docs(self):
        class Animal(Document):
            id = SequenceField(primary_key=True)
            name = StringField()

        class Person(Document):
            id = SequenceField(primary_key=True)
            name = StringField()

        self.db["mongoengine.counters"].drop()
        Animal.drop_collection()
        Person.drop_collection()

        for x in range(10):
            Animal(name="Animal %s" % x).save()
            Person(name="Person %s" % x).save()

        c = self.db["mongoengine.counters"].find_one({"_id": "person.id"})
        assert c["next"] == 10

        c = self.db["mongoengine.counters"].find_one({"_id": "animal.id"})
        assert c["next"] == 10

        ids = [i.id for i in Person.objects]
        assert ids == list(range(1, 11))

        _id = [i.id for i in Animal.objects]
        assert _id == list(range(1, 11))

        c = self.db["mongoengine.counters"].find_one({"_id": "person.id"})
        assert c["next"] == 10

        c = self.db["mongoengine.counters"].find_one({"_id": "animal.id"})
        assert c["next"] == 10

    def test_sequence_field_value_decorator(self):
        class Person(Document):
            id = SequenceField(primary_key=True, value_decorator=str)
            name = StringField()

        self.db["mongoengine.counters"].drop()
        Person.drop_collection()

        for x in range(10):
            p = Person(name="Person %s" % x)
            p.save()

        c = self.db["mongoengine.counters"].find_one({"_id": "person.id"})
        assert c["next"] == 10

        ids = [i.id for i in Person.objects]
        assert ids == [str(i) for i in range(1, 11)]

        c = self.db["mongoengine.counters"].find_one({"_id": "person.id"})
        assert c["next"] == 10

    def test_embedded_sequence_field(self):
        class Comment(EmbeddedDocument):
            id = SequenceField()
            content = StringField(required=True)

        class Post(Document):
            title = StringField(required=True)
            comments = ListField(EmbeddedDocumentField(Comment))

        self.db["mongoengine.counters"].drop()
        Post.drop_collection()

        Post(
            title="MongoEngine",
            comments=[
                Comment(content="NoSQL Rocks"),
                Comment(content="MongoEngine Rocks"),
            ],
        ).save()
        c = self.db["mongoengine.counters"].find_one({"_id": "comment.id"})
        assert c["next"] == 2
        post = Post.objects.first()
        assert 1 == post.comments[0].id
        assert 2 == post.comments[1].id

    def test_inherited_sequencefield(self):
        class Base(Document):
            name = StringField()
            counter = SequenceField()
            meta = {"abstract": True}

        class Foo(Base):
            pass

        class Bar(Base):
            pass

        bar = Bar(name="Bar")
        bar.save()

        foo = Foo(name="Foo")
        foo.save()

        assert "base.counter" in self.db["mongoengine.counters"].find().distinct("_id")
        assert not (
            ("foo.counter" or "bar.counter")
            in self.db["mongoengine.counters"].find().distinct("_id")
        )
        assert foo.counter != bar.counter
        assert foo._fields["counter"].owner_document == Base
        assert bar._fields["counter"].owner_document == Base

    def test_no_inherited_sequencefield(self):
        class Base(Document):
            name = StringField()
            meta = {"abstract": True}

        class Foo(Base):
            counter = SequenceField()

        class Bar(Base):
            counter = SequenceField()

        bar = Bar(name="Bar")
        bar.save()

        foo = Foo(name="Foo")
        foo.save()

        assert "base.counter" not in self.db["mongoengine.counters"].find().distinct(
            "_id"
        )
        existing_counters = self.db["mongoengine.counters"].find().distinct("_id")
        assert "foo.counter" in existing_counters
        assert "bar.counter" in existing_counters
        assert foo.counter == bar.counter
        assert foo._fields["counter"].owner_document == Foo
        assert bar._fields["counter"].owner_document == Bar

    def test_sequence_setattr_not_incrementing_counter(self):
        class Person(DynamicDocument):
            id = SequenceField(primary_key=True)
            name = StringField()

        self.db["mongoengine.counters"].drop()
        Person.drop_collection()

        for x in range(10):
            Person(name="Person %s" % x).save()

        c = self.db["mongoengine.counters"].find_one({"_id": "person.id"})
        assert c["next"] == 10

        # Setting SequenceField field value should not increment counter:
        new_person = Person()
        new_person.id = 1100

        # Counter should still be at 10
        c = self.db["mongoengine.counters"].find_one({"_id": "person.id"})
        assert c["next"] == 10
