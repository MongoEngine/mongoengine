# -*- coding: utf-8 -*-

from mongoengine import *

from tests.utils import MongoDBTestCase


class TestSequenceField(MongoDBTestCase):
    def test_sequence_field(self):
        class Person(Document):
            id = SequenceField(primary_key=True)
            name = StringField()

        self.db['mongoengine.counters'].drop()
        Person.drop_collection()

        for x in range(10):
            Person(name="Person %s" % x).save()

        c = self.db['mongoengine.counters'].find_one({'_id': 'person.id'})
        self.assertEqual(c['next'], 10)

        ids = [i.id for i in Person.objects]
        self.assertEqual(ids, range(1, 11))

        c = self.db['mongoengine.counters'].find_one({'_id': 'person.id'})
        self.assertEqual(c['next'], 10)

        Person.id.set_next_value(1000)
        c = self.db['mongoengine.counters'].find_one({'_id': 'person.id'})
        self.assertEqual(c['next'], 1000)

    def test_sequence_field_get_next_value(self):
        class Person(Document):
            id = SequenceField(primary_key=True)
            name = StringField()

        self.db['mongoengine.counters'].drop()
        Person.drop_collection()

        for x in range(10):
            Person(name="Person %s" % x).save()

        self.assertEqual(Person.id.get_next_value(), 11)
        self.db['mongoengine.counters'].drop()

        self.assertEqual(Person.id.get_next_value(), 1)

        class Person(Document):
            id = SequenceField(primary_key=True, value_decorator=str)
            name = StringField()

        self.db['mongoengine.counters'].drop()
        Person.drop_collection()

        for x in range(10):
            Person(name="Person %s" % x).save()

        self.assertEqual(Person.id.get_next_value(), '11')
        self.db['mongoengine.counters'].drop()

        self.assertEqual(Person.id.get_next_value(), '1')

    def test_sequence_field_sequence_name(self):
        class Person(Document):
            id = SequenceField(primary_key=True, sequence_name='jelly')
            name = StringField()

        self.db['mongoengine.counters'].drop()
        Person.drop_collection()

        for x in range(10):
            Person(name="Person %s" % x).save()

        c = self.db['mongoengine.counters'].find_one({'_id': 'jelly.id'})
        self.assertEqual(c['next'], 10)

        ids = [i.id for i in Person.objects]
        self.assertEqual(ids, range(1, 11))

        c = self.db['mongoengine.counters'].find_one({'_id': 'jelly.id'})
        self.assertEqual(c['next'], 10)

        Person.id.set_next_value(1000)
        c = self.db['mongoengine.counters'].find_one({'_id': 'jelly.id'})
        self.assertEqual(c['next'], 1000)

    def test_multiple_sequence_fields(self):
        class Person(Document):
            id = SequenceField(primary_key=True)
            counter = SequenceField()
            name = StringField()

        self.db['mongoengine.counters'].drop()
        Person.drop_collection()

        for x in range(10):
            Person(name="Person %s" % x).save()

        c = self.db['mongoengine.counters'].find_one({'_id': 'person.id'})
        self.assertEqual(c['next'], 10)

        ids = [i.id for i in Person.objects]
        self.assertEqual(ids, range(1, 11))

        counters = [i.counter for i in Person.objects]
        self.assertEqual(counters, range(1, 11))

        c = self.db['mongoengine.counters'].find_one({'_id': 'person.id'})
        self.assertEqual(c['next'], 10)

        Person.id.set_next_value(1000)
        c = self.db['mongoengine.counters'].find_one({'_id': 'person.id'})
        self.assertEqual(c['next'], 1000)

        Person.counter.set_next_value(999)
        c = self.db['mongoengine.counters'].find_one({'_id': 'person.counter'})
        self.assertEqual(c['next'], 999)

    def test_sequence_fields_reload(self):
        class Animal(Document):
            counter = SequenceField()
            name = StringField()

        self.db['mongoengine.counters'].drop()
        Animal.drop_collection()

        a = Animal(name="Boi").save()

        self.assertEqual(a.counter, 1)
        a.reload()
        self.assertEqual(a.counter, 1)

        a.counter = None
        self.assertEqual(a.counter, 2)
        a.save()

        self.assertEqual(a.counter, 2)

        a = Animal.objects.first()
        self.assertEqual(a.counter, 2)
        a.reload()
        self.assertEqual(a.counter, 2)

    def test_multiple_sequence_fields_on_docs(self):
        class Animal(Document):
            id = SequenceField(primary_key=True)
            name = StringField()

        class Person(Document):
            id = SequenceField(primary_key=True)
            name = StringField()

        self.db['mongoengine.counters'].drop()
        Animal.drop_collection()
        Person.drop_collection()

        for x in range(10):
            Animal(name="Animal %s" % x).save()
            Person(name="Person %s" % x).save()

        c = self.db['mongoengine.counters'].find_one({'_id': 'person.id'})
        self.assertEqual(c['next'], 10)

        c = self.db['mongoengine.counters'].find_one({'_id': 'animal.id'})
        self.assertEqual(c['next'], 10)

        ids = [i.id for i in Person.objects]
        self.assertEqual(ids, range(1, 11))

        id = [i.id for i in Animal.objects]
        self.assertEqual(id, range(1, 11))

        c = self.db['mongoengine.counters'].find_one({'_id': 'person.id'})
        self.assertEqual(c['next'], 10)

        c = self.db['mongoengine.counters'].find_one({'_id': 'animal.id'})
        self.assertEqual(c['next'], 10)

    def test_sequence_field_value_decorator(self):
        class Person(Document):
            id = SequenceField(primary_key=True, value_decorator=str)
            name = StringField()

        self.db['mongoengine.counters'].drop()
        Person.drop_collection()

        for x in range(10):
            p = Person(name="Person %s" % x)
            p.save()

        c = self.db['mongoengine.counters'].find_one({'_id': 'person.id'})
        self.assertEqual(c['next'], 10)

        ids = [i.id for i in Person.objects]
        self.assertEqual(ids, map(str, range(1, 11)))

        c = self.db['mongoengine.counters'].find_one({'_id': 'person.id'})
        self.assertEqual(c['next'], 10)

    def test_embedded_sequence_field(self):
        class Comment(EmbeddedDocument):
            id = SequenceField()
            content = StringField(required=True)

        class Post(Document):
            title = StringField(required=True)
            comments = ListField(EmbeddedDocumentField(Comment))

        self.db['mongoengine.counters'].drop()
        Post.drop_collection()

        Post(title="MongoEngine",
             comments=[Comment(content="NoSQL Rocks"),
                       Comment(content="MongoEngine Rocks")]).save()
        c = self.db['mongoengine.counters'].find_one({'_id': 'comment.id'})
        self.assertEqual(c['next'], 2)
        post = Post.objects.first()
        self.assertEqual(1, post.comments[0].id)
        self.assertEqual(2, post.comments[1].id)

    def test_inherited_sequencefield(self):
        class Base(Document):
            name = StringField()
            counter = SequenceField()
            meta = {'abstract': True}

        class Foo(Base):
            pass

        class Bar(Base):
            pass

        bar = Bar(name='Bar')
        bar.save()

        foo = Foo(name='Foo')
        foo.save()

        self.assertTrue('base.counter' in
                        self.db['mongoengine.counters'].find().distinct('_id'))
        self.assertFalse(('foo.counter' or 'bar.counter') in
                         self.db['mongoengine.counters'].find().distinct('_id'))
        self.assertNotEqual(foo.counter, bar.counter)
        self.assertEqual(foo._fields['counter'].owner_document, Base)
        self.assertEqual(bar._fields['counter'].owner_document, Base)

    def test_no_inherited_sequencefield(self):
        class Base(Document):
            name = StringField()
            meta = {'abstract': True}

        class Foo(Base):
            counter = SequenceField()

        class Bar(Base):
            counter = SequenceField()

        bar = Bar(name='Bar')
        bar.save()

        foo = Foo(name='Foo')
        foo.save()

        self.assertFalse('base.counter' in
                         self.db['mongoengine.counters'].find().distinct('_id'))
        self.assertTrue(('foo.counter' and 'bar.counter') in
                         self.db['mongoengine.counters'].find().distinct('_id'))
        self.assertEqual(foo.counter, bar.counter)
        self.assertEqual(foo._fields['counter'].owner_document, Foo)
        self.assertEqual(bar._fields['counter'].owner_document, Bar)
