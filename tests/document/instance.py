# -*- coding: utf-8 -*-
import bson
import os
import pickle
import unittest
import uuid
import weakref

from datetime import datetime
from bson import DBRef, ObjectId
from tests import fixtures
from tests.fixtures import (PickleEmbedded, PickleTest, PickleSignalsTest,
                            PickleDynamicEmbedded, PickleDynamicTest)

from mongoengine import *
from mongoengine.base import get_document, _document_registry
from mongoengine.connection import get_db
from mongoengine.errors import (NotRegistered, InvalidDocumentError,
                                InvalidQueryError, NotUniqueError,
                                FieldDoesNotExist, SaveConditionError)
from mongoengine.queryset import NULLIFY, Q
from mongoengine.context_managers import switch_db, query_counter
from mongoengine import signals

from tests.utils import needs_mongodb_v26

TEST_IMAGE_PATH = os.path.join(os.path.dirname(__file__),
                               '../fields/mongoengine.png')

__all__ = ("InstanceTest",)


class InstanceTest(unittest.TestCase):

    def setUp(self):
        connect(db='mongoenginetest')
        self.db = get_db()

        class Job(EmbeddedDocument):
            name = StringField()
            years = IntField()

        class Person(Document):
            name = StringField()
            age = IntField()
            job = EmbeddedDocumentField(Job)

            non_field = True

            meta = {"allow_inheritance": True}

        self.Person = Person
        self.Job = Job

    def tearDown(self):
        for collection in self.db.collection_names():
            if 'system.' in collection:
                continue
            self.db.drop_collection(collection)

    def assertDbEqual(self, docs):
        self.assertEqual(
            list(self.Person._get_collection().find().sort("id")),
            sorted(docs, key=lambda doc: doc["_id"]))

    def assertHasInstance(self, field, instance):
        self.assertTrue(hasattr(field, "_instance"))
        self.assertTrue(field._instance is not None)
        if isinstance(field._instance, weakref.ProxyType):
            self.assertTrue(field._instance.__eq__(instance))
        else:
            self.assertEqual(field._instance, instance)

    def test_capped_collection(self):
        """Ensure that capped collections work properly."""
        class Log(Document):
            date = DateTimeField(default=datetime.now)
            meta = {
                'max_documents': 10,
                'max_size': 4096,
            }

        Log.drop_collection()

        # Ensure that the collection handles up to its maximum
        for _ in range(10):
            Log().save()

        self.assertEqual(Log.objects.count(), 10)

        # Check that extra documents don't increase the size
        Log().save()
        self.assertEqual(Log.objects.count(), 10)

        options = Log.objects._collection.options()
        self.assertEqual(options['capped'], True)
        self.assertEqual(options['max'], 10)
        self.assertEqual(options['size'], 4096)

        # Check that the document cannot be redefined with different options
        class Log(Document):
            date = DateTimeField(default=datetime.now)
            meta = {
                'max_documents': 11,
            }

        # Accessing Document.objects creates the collection
        with self.assertRaises(InvalidCollectionError):
            Log.objects

    def test_capped_collection_default(self):
        """Ensure that capped collections defaults work properly."""
        class Log(Document):
            date = DateTimeField(default=datetime.now)
            meta = {
                'max_documents': 10,
            }

        Log.drop_collection()

        # Create a doc to create the collection
        Log().save()

        options = Log.objects._collection.options()
        self.assertEqual(options['capped'], True)
        self.assertEqual(options['max'], 10)
        self.assertEqual(options['size'], 10 * 2**20)

        # Check that the document with default value can be recreated
        class Log(Document):
            date = DateTimeField(default=datetime.now)
            meta = {
                'max_documents': 10,
            }

        # Create the collection by accessing Document.objects
        Log.objects

    def test_capped_collection_no_max_size_problems(self):
        """Ensure that capped collections with odd max_size work properly.
        MongoDB rounds up max_size to next multiple of 256, recreating a doc
        with the same spec failed in mongoengine <0.10
        """
        class Log(Document):
            date = DateTimeField(default=datetime.now)
            meta = {
                'max_size': 10000,
            }

        Log.drop_collection()

        # Create a doc to create the collection
        Log().save()

        options = Log.objects._collection.options()
        self.assertEqual(options['capped'], True)
        self.assertTrue(options['size'] >= 10000)

        # Check that the document with odd max_size value can be recreated
        class Log(Document):
            date = DateTimeField(default=datetime.now)
            meta = {
                'max_size': 10000,
            }

        # Create the collection by accessing Document.objects
        Log.objects

    def test_repr(self):
        """Ensure that unicode representation works
        """
        class Article(Document):
            title = StringField()

            def __unicode__(self):
                return self.title

        doc = Article(title=u'привет мир')

        self.assertEqual('<Article: привет мир>', repr(doc))

    def test_repr_none(self):
        """Ensure None values are handled correctly."""
        class Article(Document):
            title = StringField()

            def __str__(self):
                return None

        doc = Article(title=u'привет мир')
        self.assertEqual('<Article: None>', repr(doc))

    def test_queryset_resurrects_dropped_collection(self):
        self.Person.drop_collection()
        self.assertEqual([], list(self.Person.objects()))

        # Ensure works correctly with inhertited classes
        class Actor(self.Person):
            pass

        Actor.objects()
        self.Person.drop_collection()
        self.assertEqual([], list(Actor.objects()))

    def test_polymorphic_references(self):
        """Ensure that the correct subclasses are returned from a query
        when using references / generic references
        """
        class Animal(Document):
            meta = {'allow_inheritance': True}

        class Fish(Animal):
            pass

        class Mammal(Animal):
            pass

        class Dog(Mammal):
            pass

        class Human(Mammal):
            pass

        class Zoo(Document):
            animals = ListField(ReferenceField(Animal))

        Zoo.drop_collection()
        Animal.drop_collection()

        Animal().save()
        Fish().save()
        Mammal().save()
        Dog().save()
        Human().save()

        # Save a reference to each animal
        zoo = Zoo(animals=Animal.objects)
        zoo.save()
        zoo.reload()

        classes = [a.__class__ for a in Zoo.objects.first().animals]
        self.assertEqual(classes, [Animal, Fish, Mammal, Dog, Human])

        Zoo.drop_collection()

        class Zoo(Document):
            animals = ListField(GenericReferenceField())

        # Save a reference to each animal
        zoo = Zoo(animals=Animal.objects)
        zoo.save()
        zoo.reload()

        classes = [a.__class__ for a in Zoo.objects.first().animals]
        self.assertEqual(classes, [Animal, Fish, Mammal, Dog, Human])

    def test_reference_inheritance(self):
        class Stats(Document):
            created = DateTimeField(default=datetime.now)

            meta = {'allow_inheritance': False}

        class CompareStats(Document):
            generated = DateTimeField(default=datetime.now)
            stats = ListField(ReferenceField(Stats))

        Stats.drop_collection()
        CompareStats.drop_collection()

        list_stats = []

        for i in range(10):
            s = Stats()
            s.save()
            list_stats.append(s)

        cmp_stats = CompareStats(stats=list_stats)
        cmp_stats.save()

        self.assertEqual(list_stats, CompareStats.objects.first().stats)

    def test_db_field_load(self):
        """Ensure we load data correctly from the right db field."""
        class Person(Document):
            name = StringField(required=True)
            _rank = StringField(required=False, db_field="rank")

            @property
            def rank(self):
                return self._rank or "Private"

        Person.drop_collection()

        Person(name="Jack", _rank="Corporal").save()

        Person(name="Fred").save()

        self.assertEqual(Person.objects.get(name="Jack").rank, "Corporal")
        self.assertEqual(Person.objects.get(name="Fred").rank, "Private")

    def test_db_embedded_doc_field_load(self):
        """Ensure we load embedded document data correctly."""
        class Rank(EmbeddedDocument):
            title = StringField(required=True)

        class Person(Document):
            name = StringField(required=True)
            rank_ = EmbeddedDocumentField(Rank,
                                          required=False,
                                          db_field='rank')

            @property
            def rank(self):
                if self.rank_ is None:
                    return "Private"
                return self.rank_.title

        Person.drop_collection()

        Person(name="Jack", rank_=Rank(title="Corporal")).save()
        Person(name="Fred").save()

        self.assertEqual(Person.objects.get(name="Jack").rank, "Corporal")
        self.assertEqual(Person.objects.get(name="Fred").rank, "Private")

    def test_custom_id_field(self):
        """Ensure that documents may be created with custom primary keys."""
        class User(Document):
            username = StringField(primary_key=True)
            name = StringField()

            meta = {'allow_inheritance': True}

        User.drop_collection()

        self.assertEqual(User._fields['username'].db_field, '_id')
        self.assertEqual(User._meta['id_field'], 'username')

        # test no primary key field
        self.assertRaises(ValidationError, User(name='test').save)

        # define a subclass with a different primary key field than the
        # parent
        with self.assertRaises(ValueError):
            class EmailUser(User):
                email = StringField(primary_key=True)

        class EmailUser(User):
            email = StringField()

        user = User(username='test', name='test user')
        user.save()

        user_obj = User.objects.first()
        self.assertEqual(user_obj.id, 'test')
        self.assertEqual(user_obj.pk, 'test')

        user_son = User.objects._collection.find_one()
        self.assertEqual(user_son['_id'], 'test')
        self.assertTrue('username' not in user_son['_id'])

        User.drop_collection()

        user = User(pk='mongo', name='mongo user')
        user.save()

        user_obj = User.objects.first()
        self.assertEqual(user_obj.id, 'mongo')
        self.assertEqual(user_obj.pk, 'mongo')

        user_son = User.objects._collection.find_one()
        self.assertEqual(user_son['_id'], 'mongo')
        self.assertTrue('username' not in user_son['_id'])

    def test_document_not_registered(self):
        class Place(Document):
            name = StringField()

            meta = {'allow_inheritance': True}

        class NicePlace(Place):
            pass

        Place.drop_collection()

        Place(name="London").save()
        NicePlace(name="Buckingham Palace").save()

        # Mimic Place and NicePlace definitions being in a different file
        # and the NicePlace model not being imported in at query time.
        del(_document_registry['Place.NicePlace'])

        with self.assertRaises(NotRegistered):
            list(Place.objects.all())

    def test_document_registry_regressions(self):
        class Location(Document):
            name = StringField()
            meta = {'allow_inheritance': True}

        class Area(Location):
            location = ReferenceField('Location', dbref=True)

        Location.drop_collection()

        self.assertEqual(Area, get_document("Area"))
        self.assertEqual(Area, get_document("Location.Area"))

    def test_creation(self):
        """Ensure that document may be created using keyword arguments."""
        person = self.Person(name="Test User", age=30)
        self.assertEqual(person.name, "Test User")
        self.assertEqual(person.age, 30)

    def test_to_dbref(self):
        """Ensure that you can get a dbref of a document."""
        person = self.Person(name="Test User", age=30)
        self.assertRaises(OperationError, person.to_dbref)
        person.save()
        person.to_dbref()

    def test_save_abstract_document(self):
        """Saving an abstract document should fail."""
        class Doc(Document):
            name = StringField()
            meta = {'abstract': True}

        with self.assertRaises(InvalidDocumentError):
            Doc(name='aaa').save()

    def test_reload(self):
        """Ensure that attributes may be reloaded."""
        person = self.Person(name="Test User", age=20)
        person.save()

        person_obj = self.Person.objects.first()
        person_obj.name = "Mr Test User"
        person_obj.age = 21
        person_obj.save()

        self.assertEqual(person.name, "Test User")
        self.assertEqual(person.age, 20)

        person.reload('age')
        self.assertEqual(person.name, "Test User")
        self.assertEqual(person.age, 21)

        person.reload()
        self.assertEqual(person.name, "Mr Test User")
        self.assertEqual(person.age, 21)

        person.reload()
        self.assertEqual(person.name, "Mr Test User")
        self.assertEqual(person.age, 21)

    def test_reload_sharded(self):
        class Animal(Document):
            superphylum = StringField()
            meta = {'shard_key': ('superphylum',)}

        Animal.drop_collection()
        doc = Animal(superphylum='Deuterostomia')
        doc.save()
        doc.reload()

    def test_reload_sharded_nested(self):
        class SuperPhylum(EmbeddedDocument):
            name = StringField()

        class Animal(Document):
            superphylum = EmbeddedDocumentField(SuperPhylum)
            meta = {'shard_key': ('superphylum.name',)}

        Animal.drop_collection()
        doc = Animal(superphylum=SuperPhylum(name='Deuterostomia'))
        doc.save()
        doc.reload()

    def test_reload_with_changed_fields(self):
        """Ensures reloading will not affect changed fields"""
        class User(Document):
            name = StringField()
            number = IntField()
        User.drop_collection()

        user = User(name="Bob", number=1).save()
        user.name = "John"
        user.number = 2

        self.assertEqual(user._get_changed_fields(), ['name', 'number'])
        user.reload('number')
        self.assertEqual(user._get_changed_fields(), ['name'])
        user.save()
        user.reload()
        self.assertEqual(user.name, "John")

    def test_reload_referencing(self):
        """Ensures reloading updates weakrefs correctly."""
        class Embedded(EmbeddedDocument):
            dict_field = DictField()
            list_field = ListField()

        class Doc(Document):
            dict_field = DictField()
            list_field = ListField()
            embedded_field = EmbeddedDocumentField(Embedded)

        Doc.drop_collection()
        doc = Doc()
        doc.dict_field = {'hello': 'world'}
        doc.list_field = ['1', 2, {'hello': 'world'}]

        embedded_1 = Embedded()
        embedded_1.dict_field = {'hello': 'world'}
        embedded_1.list_field = ['1', 2, {'hello': 'world'}]
        doc.embedded_field = embedded_1
        doc.save()

        doc = doc.reload(10)
        doc.list_field.append(1)
        doc.dict_field['woot'] = "woot"
        doc.embedded_field.list_field.append(1)
        doc.embedded_field.dict_field['woot'] = "woot"

        self.assertEqual(doc._get_changed_fields(), [
            'list_field', 'dict_field.woot', 'embedded_field.list_field',
            'embedded_field.dict_field.woot'])
        doc.save()

        self.assertEqual(len(doc.list_field), 4)
        doc = doc.reload(10)
        self.assertEqual(doc._get_changed_fields(), [])
        self.assertEqual(len(doc.list_field), 4)
        self.assertEqual(len(doc.dict_field), 2)
        self.assertEqual(len(doc.embedded_field.list_field), 4)
        self.assertEqual(len(doc.embedded_field.dict_field), 2)

        doc.list_field.append(1)
        doc.save()
        doc.dict_field['extra'] = 1
        doc = doc.reload(10, 'list_field')
        self.assertEqual(doc._get_changed_fields(), ['dict_field.extra'])
        self.assertEqual(len(doc.list_field), 5)
        self.assertEqual(len(doc.dict_field), 3)
        self.assertEqual(len(doc.embedded_field.list_field), 4)
        self.assertEqual(len(doc.embedded_field.dict_field), 2)

    def test_reload_doesnt_exist(self):
        class Foo(Document):
            pass

        f = Foo()
        try:
            f.reload()
        except Foo.DoesNotExist:
            pass
        except Exception:
            self.assertFalse("Threw wrong exception")

        f.save()
        f.delete()
        try:
            f.reload()
        except Foo.DoesNotExist:
            pass
        except Exception:
            self.assertFalse("Threw wrong exception")

    def test_reload_of_non_strict_with_special_field_name(self):
        """Ensures reloading works for documents with meta strict == False."""
        class Post(Document):
            meta = {
                'strict': False
            }
            title = StringField()
            items = ListField()

        Post.drop_collection()

        Post._get_collection().insert({
            "title": "Items eclipse",
            "items": ["more lorem", "even more ipsum"]
        })

        post = Post.objects.first()
        post.reload()
        self.assertEqual(post.title, "Items eclipse")
        self.assertEqual(post.items, ["more lorem", "even more ipsum"])

    def test_dictionary_access(self):
        """Ensure that dictionary-style field access works properly."""
        person = self.Person(name='Test User', age=30, job=self.Job())
        self.assertEqual(person['name'], 'Test User')

        self.assertRaises(KeyError, person.__getitem__, 'salary')
        self.assertRaises(KeyError, person.__setitem__, 'salary', 50)

        person['name'] = 'Another User'
        self.assertEqual(person['name'], 'Another User')

        # Length = length(assigned fields + id)
        self.assertEqual(len(person), 5)

        self.assertTrue('age' in person)
        person.age = None
        self.assertFalse('age' in person)
        self.assertFalse('nationality' in person)

    def test_embedded_document_to_mongo(self):
        class Person(EmbeddedDocument):
            name = StringField()
            age = IntField()

            meta = {"allow_inheritance": True}

        class Employee(Person):
            salary = IntField()

        self.assertEqual(Person(name="Bob", age=35).to_mongo().keys(),
                         ['_cls', 'name', 'age'])
        self.assertEqual(
            Employee(name="Bob", age=35, salary=0).to_mongo().keys(),
            ['_cls', 'name', 'age', 'salary'])

    def test_embedded_document_to_mongo_id(self):
        class SubDoc(EmbeddedDocument):
            id = StringField(required=True)

        sub_doc = SubDoc(id="abc")
        self.assertEqual(sub_doc.to_mongo().keys(), ['id'])

    def test_embedded_document(self):
        """Ensure that embedded documents are set up correctly."""
        class Comment(EmbeddedDocument):
            content = StringField()

        self.assertTrue('content' in Comment._fields)
        self.assertFalse('id' in Comment._fields)

    def test_embedded_document_instance(self):
        """Ensure that embedded documents can reference parent instance."""
        class Embedded(EmbeddedDocument):
            string = StringField()

        class Doc(Document):
            embedded_field = EmbeddedDocumentField(Embedded)

        Doc.drop_collection()

        doc = Doc(embedded_field=Embedded(string="Hi"))
        self.assertHasInstance(doc.embedded_field, doc)

        doc.save()
        doc = Doc.objects.get()
        self.assertHasInstance(doc.embedded_field, doc)

    def test_embedded_document_complex_instance(self):
        """Ensure that embedded documents in complex fields can reference
        parent instance.
        """
        class Embedded(EmbeddedDocument):
            string = StringField()

        class Doc(Document):
            embedded_field = ListField(EmbeddedDocumentField(Embedded))

        Doc.drop_collection()
        doc = Doc(embedded_field=[Embedded(string="Hi")])
        self.assertHasInstance(doc.embedded_field[0], doc)

        doc.save()
        doc = Doc.objects.get()
        self.assertHasInstance(doc.embedded_field[0], doc)

    def test_embedded_document_complex_instance_no_use_db_field(self):
        """Ensure that use_db_field is propagated to list of Emb Docs."""
        class Embedded(EmbeddedDocument):
            string = StringField(db_field='s')

        class Doc(Document):
            embedded_field = ListField(EmbeddedDocumentField(Embedded))

        d = Doc(embedded_field=[Embedded(string="Hi")]).to_mongo(
            use_db_field=False).to_dict()
        self.assertEqual(d['embedded_field'], [{'string': 'Hi'}])

    def test_instance_is_set_on_setattr(self):
        class Email(EmbeddedDocument):
            email = EmailField()

        class Account(Document):
            email = EmbeddedDocumentField(Email)

        Account.drop_collection()

        acc = Account()
        acc.email = Email(email='test@example.com')
        self.assertHasInstance(acc._data["email"], acc)
        acc.save()

        acc1 = Account.objects.first()
        self.assertHasInstance(acc1._data["email"], acc1)

    def test_instance_is_set_on_setattr_on_embedded_document_list(self):
        class Email(EmbeddedDocument):
            email = EmailField()

        class Account(Document):
            emails = EmbeddedDocumentListField(Email)

        Account.drop_collection()
        acc = Account()
        acc.emails = [Email(email='test@example.com')]
        self.assertHasInstance(acc._data["emails"][0], acc)
        acc.save()

        acc1 = Account.objects.first()
        self.assertHasInstance(acc1._data["emails"][0], acc1)

    def test_document_clean(self):
        class TestDocument(Document):
            status = StringField()
            pub_date = DateTimeField()

            def clean(self):
                if self.status == 'draft' and self.pub_date is not None:
                    msg = 'Draft entries may not have a publication date.'
                    raise ValidationError(msg)
                # Set the pub_date for published items if not set.
                if self.status == 'published' and self.pub_date is None:
                    self.pub_date = datetime.now()

        TestDocument.drop_collection()

        t = TestDocument(status="draft", pub_date=datetime.now())

        try:
            t.save()
        except ValidationError as e:
            expect_msg = "Draft entries may not have a publication date."
            self.assertTrue(expect_msg in e.message)
            self.assertEqual(e.to_dict(), {'__all__': expect_msg})

        t = TestDocument(status="published")
        t.save(clean=False)

        self.assertEqual(t.pub_date, None)

        t = TestDocument(status="published")
        t.save(clean=True)

        self.assertEqual(type(t.pub_date), datetime)

    def test_document_embedded_clean(self):
        class TestEmbeddedDocument(EmbeddedDocument):
            x = IntField(required=True)
            y = IntField(required=True)
            z = IntField(required=True)

            meta = {'allow_inheritance': False}

            def clean(self):
                if self.z:
                    if self.z != self.x + self.y:
                        raise ValidationError('Value of z != x + y')
                else:
                    self.z = self.x + self.y

        class TestDocument(Document):
            doc = EmbeddedDocumentField(TestEmbeddedDocument)
            status = StringField()

        TestDocument.drop_collection()

        t = TestDocument(doc=TestEmbeddedDocument(x=10, y=25, z=15))
        try:
            t.save()
        except ValidationError as e:
            expect_msg = "Value of z != x + y"
            self.assertTrue(expect_msg in e.message)
            self.assertEqual(e.to_dict(), {'doc': {'__all__': expect_msg}})

        t = TestDocument(doc=TestEmbeddedDocument(x=10, y=25)).save()
        self.assertEqual(t.doc.z, 35)

        # Asserts not raises
        t = TestDocument(doc=TestEmbeddedDocument(x=15, y=35, z=5))
        t.save(clean=False)

    def test_modify_empty(self):
        doc = self.Person(name="bob", age=10).save()

        with self.assertRaises(InvalidDocumentError):
            self.Person().modify(set__age=10)

        self.assertDbEqual([dict(doc.to_mongo())])

    def test_modify_invalid_query(self):
        doc1 = self.Person(name="bob", age=10).save()
        doc2 = self.Person(name="jim", age=20).save()
        docs = [dict(doc1.to_mongo()), dict(doc2.to_mongo())]

        with self.assertRaises(InvalidQueryError):
            doc1.modify({'id': doc2.id}, set__value=20)

        self.assertDbEqual(docs)

    def test_modify_match_another_document(self):
        doc1 = self.Person(name="bob", age=10).save()
        doc2 = self.Person(name="jim", age=20).save()
        docs = [dict(doc1.to_mongo()), dict(doc2.to_mongo())]

        assert not doc1.modify({'name': doc2.name}, set__age=100)

        self.assertDbEqual(docs)

    def test_modify_not_exists(self):
        doc1 = self.Person(name="bob", age=10).save()
        doc2 = self.Person(id=ObjectId(), name="jim", age=20)
        docs = [dict(doc1.to_mongo())]

        assert not doc2.modify({'name': doc2.name}, set__age=100)

        self.assertDbEqual(docs)

    def test_modify_update(self):
        other_doc = self.Person(name="bob", age=10).save()
        doc = self.Person(
            name="jim", age=20, job=self.Job(name="10gen", years=3)).save()

        doc_copy = doc._from_son(doc.to_mongo())

        # these changes must go away
        doc.name = "liza"
        doc.job.name = "Google"
        doc.job.years = 3

        assert doc.modify(
            set__age=21, set__job__name="MongoDB", unset__job__years=True)
        doc_copy.age = 21
        doc_copy.job.name = "MongoDB"
        del doc_copy.job.years

        assert doc.to_json() == doc_copy.to_json()
        assert doc._get_changed_fields() == []

        self.assertDbEqual([dict(other_doc.to_mongo()), dict(doc.to_mongo())])

    @needs_mongodb_v26
    def test_modify_with_positional_push(self):
        class BlogPost(Document):
            tags = ListField(StringField())

        post = BlogPost.objects.create(tags=['python'])
        self.assertEqual(post.tags, ['python'])
        post.modify(push__tags__0=['code', 'mongo'])
        self.assertEqual(post.tags, ['code', 'mongo', 'python'])

        # Assert same order of the list items is maintained in the db
        self.assertEqual(
            BlogPost._get_collection().find_one({'_id': post.pk})['tags'],
            ['code', 'mongo', 'python']
        )

    def test_save(self):
        """Ensure that a document may be saved in the database."""

        # Create person object and save it to the database
        person = self.Person(name='Test User', age=30)
        person.save()

        # Ensure that the object is in the database
        collection = self.db[self.Person._get_collection_name()]
        person_obj = collection.find_one({'name': 'Test User'})
        self.assertEqual(person_obj['name'], 'Test User')
        self.assertEqual(person_obj['age'], 30)
        self.assertEqual(person_obj['_id'], person.id)

        # Test skipping validation on save
        class Recipient(Document):
            email = EmailField(required=True)

        recipient = Recipient(email='not-an-email')
        self.assertRaises(ValidationError, recipient.save)
        recipient.save(validate=False)

    def test_save_to_a_value_that_equates_to_false(self):
        class Thing(EmbeddedDocument):
            count = IntField()

        class User(Document):
            thing = EmbeddedDocumentField(Thing)

        User.drop_collection()

        user = User(thing=Thing(count=1))
        user.save()
        user.reload()

        user.thing.count = 0
        user.save()

        user.reload()
        self.assertEqual(user.thing.count, 0)

    def test_save_max_recursion_not_hit(self):
        class Person(Document):
            name = StringField()
            parent = ReferenceField('self')
            friend = ReferenceField('self')

        Person.drop_collection()

        p1 = Person(name="Wilson Snr")
        p1.parent = None
        p1.save()

        p2 = Person(name="Wilson Jr")
        p2.parent = p1
        p2.save()

        p1.friend = p2
        p1.save()

        # Confirm can save and it resets the changed fields without hitting
        # max recursion error
        p0 = Person.objects.first()
        p0.name = 'wpjunior'
        p0.save()

    def test_save_max_recursion_not_hit_with_file_field(self):
        class Foo(Document):
            name = StringField()
            picture = FileField()
            bar = ReferenceField('self')

        Foo.drop_collection()

        a = Foo(name='hello').save()

        a.bar = a
        with open(TEST_IMAGE_PATH, 'rb') as test_image:
            a.picture = test_image
            a.save()

            # Confirm can save and it resets the changed fields without hitting
            # max recursion error
            b = Foo.objects.with_id(a.id)
            b.name = 'world'
            b.save()

            self.assertEqual(b.picture, b.bar.picture, b.bar.bar.picture)

    def test_save_cascades(self):
        class Person(Document):
            name = StringField()
            parent = ReferenceField('self')

        Person.drop_collection()

        p1 = Person(name="Wilson Snr")
        p1.parent = None
        p1.save()

        p2 = Person(name="Wilson Jr")
        p2.parent = p1
        p2.save()

        p = Person.objects(name="Wilson Jr").get()
        p.parent.name = "Daddy Wilson"
        p.save(cascade=True)

        p1.reload()
        self.assertEqual(p1.name, p.parent.name)

    def test_save_cascade_kwargs(self):
        class Person(Document):
            name = StringField()
            parent = ReferenceField('self')

        Person.drop_collection()

        p1 = Person(name="Wilson Snr")
        p1.parent = None
        p1.save()

        p2 = Person(name="Wilson Jr")
        p2.parent = p1
        p1.name = "Daddy Wilson"
        p2.save(force_insert=True, cascade_kwargs={"force_insert": False})

        p1.reload()
        p2.reload()
        self.assertEqual(p1.name, p2.parent.name)

    def test_save_cascade_meta_false(self):
        class Person(Document):
            name = StringField()
            parent = ReferenceField('self')

            meta = {'cascade': False}

        Person.drop_collection()

        p1 = Person(name="Wilson Snr")
        p1.parent = None
        p1.save()

        p2 = Person(name="Wilson Jr")
        p2.parent = p1
        p2.save()

        p = Person.objects(name="Wilson Jr").get()
        p.parent.name = "Daddy Wilson"
        p.save()

        p1.reload()
        self.assertNotEqual(p1.name, p.parent.name)

        p.save(cascade=True)
        p1.reload()
        self.assertEqual(p1.name, p.parent.name)

    def test_save_cascade_meta_true(self):
        class Person(Document):
            name = StringField()
            parent = ReferenceField('self')

            meta = {'cascade': False}

        Person.drop_collection()

        p1 = Person(name="Wilson Snr")
        p1.parent = None
        p1.save()

        p2 = Person(name="Wilson Jr")
        p2.parent = p1
        p2.save(cascade=True)

        p = Person.objects(name="Wilson Jr").get()
        p.parent.name = "Daddy Wilson"
        p.save()

        p1.reload()
        self.assertNotEqual(p1.name, p.parent.name)

    def test_save_cascades_generically(self):
        class Person(Document):
            name = StringField()
            parent = GenericReferenceField()

        Person.drop_collection()

        p1 = Person(name="Wilson Snr")
        p1.save()

        p2 = Person(name="Wilson Jr")
        p2.parent = p1
        p2.save()

        p = Person.objects(name="Wilson Jr").get()
        p.parent.name = "Daddy Wilson"
        p.save()

        p1.reload()
        self.assertNotEqual(p1.name, p.parent.name)

        p.save(cascade=True)
        p1.reload()
        self.assertEqual(p1.name, p.parent.name)

    def test_save_atomicity_condition(self):
        class Widget(Document):
            toggle = BooleanField(default=False)
            count = IntField(default=0)
            save_id = UUIDField()

        def flip(widget):
            widget.toggle = not widget.toggle
            widget.count += 1

        def UUID(i):
            return uuid.UUID(int=i)

        Widget.drop_collection()

        w1 = Widget(toggle=False, save_id=UUID(1))

        # ignore save_condition on new record creation
        w1.save(save_condition={'save_id': UUID(42)})
        w1.reload()
        self.assertFalse(w1.toggle)
        self.assertEqual(w1.save_id, UUID(1))
        self.assertEqual(w1.count, 0)

        # mismatch in save_condition prevents save and raise exception
        flip(w1)
        self.assertTrue(w1.toggle)
        self.assertEqual(w1.count, 1)
        self.assertRaises(SaveConditionError,
            w1.save, save_condition={'save_id': UUID(42)})
        w1.reload()
        self.assertFalse(w1.toggle)
        self.assertEqual(w1.count, 0)

        # matched save_condition allows save
        flip(w1)
        self.assertTrue(w1.toggle)
        self.assertEqual(w1.count, 1)
        w1.save(save_condition={'save_id': UUID(1)})
        w1.reload()
        self.assertTrue(w1.toggle)
        self.assertEqual(w1.count, 1)

        # save_condition can be used to ensure atomic read & updates
        # i.e., prevent interleaved reads and writes from separate contexts
        w2 = Widget.objects.get()
        self.assertEqual(w1, w2)
        old_id = w1.save_id

        flip(w1)
        w1.save_id = UUID(2)
        w1.save(save_condition={'save_id': old_id})
        w1.reload()
        self.assertFalse(w1.toggle)
        self.assertEqual(w1.count, 2)
        flip(w2)
        flip(w2)
        self.assertRaises(SaveConditionError,
            w2.save, save_condition={'save_id': old_id})
        w2.reload()
        self.assertFalse(w2.toggle)
        self.assertEqual(w2.count, 2)

        # save_condition uses mongoengine-style operator syntax
        flip(w1)
        w1.save(save_condition={'count__lt': w1.count})
        w1.reload()
        self.assertTrue(w1.toggle)
        self.assertEqual(w1.count, 3)
        flip(w1)
        self.assertRaises(SaveConditionError,
            w1.save, save_condition={'count__gte': w1.count})
        w1.reload()
        self.assertTrue(w1.toggle)
        self.assertEqual(w1.count, 3)

    def test_update(self):
        """Ensure that an existing document is updated instead of be
        overwritten.
        """
        # Create person object and save it to the database
        person = self.Person(name='Test User', age=30)
        person.save()

        # Create same person object, with same id, without age
        same_person = self.Person(name='Test')
        same_person.id = person.id
        same_person.save()

        # Confirm only one object
        self.assertEqual(self.Person.objects.count(), 1)

        # reload
        person.reload()
        same_person.reload()

        # Confirm the same
        self.assertEqual(person, same_person)
        self.assertEqual(person.name, same_person.name)
        self.assertEqual(person.age, same_person.age)

        # Confirm the saved values
        self.assertEqual(person.name, 'Test')
        self.assertEqual(person.age, 30)

        # Test only / exclude only updates included fields
        person = self.Person.objects.only('name').get()
        person.name = 'User'
        person.save()

        person.reload()
        self.assertEqual(person.name, 'User')
        self.assertEqual(person.age, 30)

        # test exclude only updates set fields
        person = self.Person.objects.exclude('name').get()
        person.age = 21
        person.save()

        person.reload()
        self.assertEqual(person.name, 'User')
        self.assertEqual(person.age, 21)

        # Test only / exclude can set non excluded / included fields
        person = self.Person.objects.only('name').get()
        person.name = 'Test'
        person.age = 30
        person.save()

        person.reload()
        self.assertEqual(person.name, 'Test')
        self.assertEqual(person.age, 30)

        # test exclude only updates set fields
        person = self.Person.objects.exclude('name').get()
        person.name = 'User'
        person.age = 21
        person.save()

        person.reload()
        self.assertEqual(person.name, 'User')
        self.assertEqual(person.age, 21)

        # Confirm does remove unrequired fields
        person = self.Person.objects.exclude('name').get()
        person.age = None
        person.save()

        person.reload()
        self.assertEqual(person.name, 'User')
        self.assertEqual(person.age, None)

        person = self.Person.objects.get()
        person.name = None
        person.age = None
        person.save()

        person.reload()
        self.assertEqual(person.name, None)
        self.assertEqual(person.age, None)

    def test_update_rename_operator(self):
        """Test the $rename operator."""
        coll = self.Person._get_collection()
        doc = self.Person(name='John').save()
        raw_doc = coll.find_one({'_id': doc.pk})
        self.assertEqual(set(raw_doc.keys()), set(['_id', '_cls', 'name']))

        doc.update(rename__name='first_name')
        raw_doc = coll.find_one({'_id': doc.pk})
        self.assertEqual(set(raw_doc.keys()),
                         set(['_id', '_cls', 'first_name']))
        self.assertEqual(raw_doc['first_name'], 'John')

    def test_inserts_if_you_set_the_pk(self):
        p1 = self.Person(name='p1', id=bson.ObjectId()).save()
        p2 = self.Person(name='p2')
        p2.id = bson.ObjectId()
        p2.save()

        self.assertEqual(2, self.Person.objects.count())

    def test_can_save_if_not_included(self):
        class EmbeddedDoc(EmbeddedDocument):
            pass

        class Simple(Document):
            pass

        class Doc(Document):
            string_field = StringField(default='1')
            int_field = IntField(default=1)
            float_field = FloatField(default=1.1)
            boolean_field = BooleanField(default=True)
            datetime_field = DateTimeField(default=datetime.now)
            embedded_document_field = EmbeddedDocumentField(
                EmbeddedDoc, default=lambda: EmbeddedDoc())
            list_field = ListField(default=lambda: [1, 2, 3])
            dict_field = DictField(default=lambda: {"hello": "world"})
            objectid_field = ObjectIdField(default=bson.ObjectId)
            reference_field = ReferenceField(Simple, default=lambda:
                                             Simple().save())
            map_field = MapField(IntField(), default=lambda: {"simple": 1})
            decimal_field = DecimalField(default=1.0)
            complex_datetime_field = ComplexDateTimeField(default=datetime.now)
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
        my_doc = Doc.objects.only("string_field").first()
        my_doc.string_field = "string"
        my_doc.save()

        my_doc = Doc.objects.get(string_field="string")
        self.assertEqual(my_doc.string_field, "string")
        self.assertEqual(my_doc.int_field, 1)

    def test_document_update(self):

        # try updating a non-saved document
        with self.assertRaises(OperationError):
            person = self.Person(name='dcrosta')
            person.update(set__name='Dan Crosta')

        author = self.Person(name='dcrosta')
        author.save()

        author.update(set__name='Dan Crosta')
        author.reload()

        p1 = self.Person.objects.first()
        self.assertEqual(p1.name, author.name)

        # try sending an empty update
        with self.assertRaises(OperationError):
            person = self.Person.objects.first()
            person.update()

        # update that doesn't explicitly specify an operator should default
        # to 'set__'
        person = self.Person.objects.first()
        person.update(name="Dan")
        person.reload()
        self.assertEqual("Dan", person.name)

    def test_update_unique_field(self):
        class Doc(Document):
            name = StringField(unique=True)

        doc1 = Doc(name="first").save()
        doc2 = Doc(name="second").save()

        with self.assertRaises(NotUniqueError):
            doc2.update(set__name=doc1.name)

    def test_embedded_update(self):
        """Test update on `EmbeddedDocumentField` fields."""
        class Page(EmbeddedDocument):
            log_message = StringField(verbose_name="Log message",
                                      required=True)

        class Site(Document):
            page = EmbeddedDocumentField(Page)

        Site.drop_collection()
        site = Site(page=Page(log_message="Warning: Dummy message"))
        site.save()

        # Update
        site = Site.objects.first()
        site.page.log_message = "Error: Dummy message"
        site.save()

        site = Site.objects.first()
        self.assertEqual(site.page.log_message, "Error: Dummy message")

    def test_update_list_field(self):
        """Test update on `ListField` with $pull + $in.
        """
        class Doc(Document):
            foo = ListField(StringField())

        Doc.drop_collection()
        doc = Doc(foo=['a', 'b', 'c'])
        doc.save()

        # Update
        doc = Doc.objects.first()
        doc.update(pull__foo__in=['a', 'c'])

        doc = Doc.objects.first()
        self.assertEqual(doc.foo, ['b'])

    def test_embedded_update_db_field(self):
        """Test update on `EmbeddedDocumentField` fields when db_field
        is other than default.
        """
        class Page(EmbeddedDocument):
            log_message = StringField(verbose_name="Log message",
                                      db_field="page_log_message",
                                      required=True)

        class Site(Document):
            page = EmbeddedDocumentField(Page)

        Site.drop_collection()

        site = Site(page=Page(log_message="Warning: Dummy message"))
        site.save()

        # Update
        site = Site.objects.first()
        site.page.log_message = "Error: Dummy message"
        site.save()

        site = Site.objects.first()
        self.assertEqual(site.page.log_message, "Error: Dummy message")

    def test_save_only_changed_fields(self):
        """Ensure save only sets / unsets changed fields."""
        class User(self.Person):
            active = BooleanField(default=True)

        User.drop_collection()

        # Create person object and save it to the database
        user = User(name='Test User', age=30, active=True)
        user.save()
        user.reload()

        # Simulated Race condition
        same_person = self.Person.objects.get()
        same_person.active = False

        user.age = 21
        user.save()

        same_person.name = 'User'
        same_person.save()

        person = self.Person.objects.get()
        self.assertEqual(person.name, 'User')
        self.assertEqual(person.age, 21)
        self.assertEqual(person.active, False)

    def test_query_count_when_saving(self):
        """Ensure references don't cause extra fetches when saving"""
        class Organization(Document):
            name = StringField()

        class User(Document):
            name = StringField()
            orgs = ListField(ReferenceField('Organization'))

        class Feed(Document):
            name = StringField()

        class UserSubscription(Document):
            name = StringField()
            user = ReferenceField(User)
            feed = ReferenceField(Feed)

        Organization.drop_collection()
        User.drop_collection()
        Feed.drop_collection()
        UserSubscription.drop_collection()

        o1 = Organization(name="o1").save()
        o2 = Organization(name="o2").save()

        u1 = User(name="Ross", orgs=[o1, o2]).save()
        f1 = Feed(name="MongoEngine").save()

        sub = UserSubscription(user=u1, feed=f1).save()

        user = User.objects.first()
        # Even if stored as ObjectId's internally mongoengine uses DBRefs
        # As ObjectId's aren't automatically derefenced
        self.assertTrue(isinstance(user._data['orgs'][0], DBRef))
        self.assertTrue(isinstance(user.orgs[0], Organization))
        self.assertTrue(isinstance(user._data['orgs'][0], Organization))

        # Changing a value
        with query_counter() as q:
            self.assertEqual(q, 0)
            sub = UserSubscription.objects.first()
            self.assertEqual(q, 1)
            sub.name = "Test Sub"
            sub.save()
            self.assertEqual(q, 2)

        # Changing a value that will cascade
        with query_counter() as q:
            self.assertEqual(q, 0)
            sub = UserSubscription.objects.first()
            self.assertEqual(q, 1)
            sub.user.name = "Test"
            self.assertEqual(q, 2)
            sub.save(cascade=True)
            self.assertEqual(q, 3)

        # Changing a value and one that will cascade
        with query_counter() as q:
            self.assertEqual(q, 0)
            sub = UserSubscription.objects.first()
            sub.name = "Test Sub 2"
            self.assertEqual(q, 1)
            sub.user.name = "Test 2"
            self.assertEqual(q, 2)
            sub.save(cascade=True)
            self.assertEqual(q, 4)  # One for the UserSub and one for the User

        # Saving with just the refs
        with query_counter() as q:
            self.assertEqual(q, 0)
            sub = UserSubscription(user=u1.pk, feed=f1.pk)
            self.assertEqual(q, 0)
            sub.save()
            self.assertEqual(q, 1)

        # Saving with just the refs on a ListField
        with query_counter() as q:
            self.assertEqual(q, 0)
            User(name="Bob", orgs=[o1.pk, o2.pk]).save()
            self.assertEqual(q, 1)

        # Saving new objects
        with query_counter() as q:
            self.assertEqual(q, 0)
            user = User.objects.first()
            self.assertEqual(q, 1)
            feed = Feed.objects.first()
            self.assertEqual(q, 2)
            sub = UserSubscription(user=user, feed=feed)
            self.assertEqual(q, 2)  # Check no change
            sub.save()
            self.assertEqual(q, 3)

    def test_set_unset_one_operation(self):
        """Ensure that $set and $unset actions are performed in the
        same operation.
        """
        class FooBar(Document):
            foo = StringField(default=None)
            bar = StringField(default=None)

        FooBar.drop_collection()

        # write an entity with a single prop
        foo = FooBar(foo='foo').save()

        self.assertEqual(foo.foo, 'foo')
        del foo.foo
        foo.bar = 'bar'

        with query_counter() as q:
            self.assertEqual(0, q)
            foo.save()
            self.assertEqual(1, q)

    def test_save_only_changed_fields_recursive(self):
        """Ensure save only sets / unsets changed fields."""
        class Comment(EmbeddedDocument):
            published = BooleanField(default=True)

        class User(self.Person):
            comments_dict = DictField()
            comments = ListField(EmbeddedDocumentField(Comment))
            active = BooleanField(default=True)

        User.drop_collection()

        # Create person object and save it to the database
        person = User(name='Test User', age=30, active=True)
        person.comments.append(Comment())
        person.save()
        person.reload()

        person = self.Person.objects.get()
        self.assertTrue(person.comments[0].published)

        person.comments[0].published = False
        person.save()

        person = self.Person.objects.get()
        self.assertFalse(person.comments[0].published)

        # Simple dict w
        person.comments_dict['first_post'] = Comment()
        person.save()

        person = self.Person.objects.get()
        self.assertTrue(person.comments_dict['first_post'].published)

        person.comments_dict['first_post'].published = False
        person.save()

        person = self.Person.objects.get()
        self.assertFalse(person.comments_dict['first_post'].published)

    def test_delete(self):
        """Ensure that document may be deleted using the delete method."""
        person = self.Person(name="Test User", age=30)
        person.save()
        self.assertEqual(self.Person.objects.count(), 1)
        person.delete()
        self.assertEqual(self.Person.objects.count(), 0)

    def test_save_custom_id(self):
        """Ensure that a document may be saved with a custom _id."""

        # Create person object and save it to the database
        person = self.Person(name='Test User', age=30,
                             id='497ce96f395f2f052a494fd4')
        person.save()

        # Ensure that the object is in the database with the correct _id
        collection = self.db[self.Person._get_collection_name()]
        person_obj = collection.find_one({'name': 'Test User'})
        self.assertEqual(str(person_obj['_id']), '497ce96f395f2f052a494fd4')

    def test_save_custom_pk(self):
        """Ensure that a document may be saved with a custom _id using
        pk alias.
        """
        # Create person object and save it to the database
        person = self.Person(name='Test User', age=30,
                             pk='497ce96f395f2f052a494fd4')
        person.save()

        # Ensure that the object is in the database with the correct _id
        collection = self.db[self.Person._get_collection_name()]
        person_obj = collection.find_one({'name': 'Test User'})
        self.assertEqual(str(person_obj['_id']), '497ce96f395f2f052a494fd4')

    def test_save_list(self):
        """Ensure that a list field may be properly saved."""
        class Comment(EmbeddedDocument):
            content = StringField()

        class BlogPost(Document):
            content = StringField()
            comments = ListField(EmbeddedDocumentField(Comment))
            tags = ListField(StringField())

        BlogPost.drop_collection()

        post = BlogPost(content='Went for a walk today...')
        post.tags = tags = ['fun', 'leisure']
        comments = [Comment(content='Good for you'), Comment(content='Yay.')]
        post.comments = comments
        post.save()

        collection = self.db[BlogPost._get_collection_name()]
        post_obj = collection.find_one()
        self.assertEqual(post_obj['tags'], tags)
        for comment_obj, comment in zip(post_obj['comments'], comments):
            self.assertEqual(comment_obj['content'], comment['content'])

    def test_list_search_by_embedded(self):
        class User(Document):
            username = StringField(required=True)

            meta = {'allow_inheritance': False}

        class Comment(EmbeddedDocument):
            comment = StringField()
            user = ReferenceField(User,
                                  required=True)

            meta = {'allow_inheritance': False}

        class Page(Document):
            comments = ListField(EmbeddedDocumentField(Comment))
            meta = {'allow_inheritance': False,
                    'indexes': [
                        {'fields': ['comments.user']}
                    ]}

        User.drop_collection()
        Page.drop_collection()

        u1 = User(username="wilson")
        u1.save()

        u2 = User(username="rozza")
        u2.save()

        u3 = User(username="hmarr")
        u3.save()

        p1 = Page(comments=[Comment(user=u1, comment="Its very good"),
                            Comment(user=u2, comment="Hello world"),
                            Comment(user=u3, comment="Ping Pong"),
                            Comment(user=u1, comment="I like a beer")])
        p1.save()

        p2 = Page(comments=[Comment(user=u1, comment="Its very good"),
                            Comment(user=u2, comment="Hello world")])
        p2.save()

        p3 = Page(comments=[Comment(user=u3, comment="Its very good")])
        p3.save()

        p4 = Page(comments=[Comment(user=u2, comment="Heavy Metal song")])
        p4.save()

        self.assertEqual(
            [p1, p2],
            list(Page.objects.filter(comments__user=u1)))
        self.assertEqual(
            [p1, p2, p4],
            list(Page.objects.filter(comments__user=u2)))
        self.assertEqual(
            [p1, p3],
            list(Page.objects.filter(comments__user=u3)))

    def test_save_embedded_document(self):
        """Ensure that a document with an embedded document field may
        be saved in the database.
        """
        class EmployeeDetails(EmbeddedDocument):
            position = StringField()

        class Employee(self.Person):
            salary = IntField()
            details = EmbeddedDocumentField(EmployeeDetails)

        # Create employee object and save it to the database
        employee = Employee(name='Test Employee', age=50, salary=20000)
        employee.details = EmployeeDetails(position='Developer')
        employee.save()

        # Ensure that the object is in the database
        collection = self.db[self.Person._get_collection_name()]
        employee_obj = collection.find_one({'name': 'Test Employee'})
        self.assertEqual(employee_obj['name'], 'Test Employee')
        self.assertEqual(employee_obj['age'], 50)

        # Ensure that the 'details' embedded object saved correctly
        self.assertEqual(employee_obj['details']['position'], 'Developer')

    def test_embedded_update_after_save(self):
        """Test update of `EmbeddedDocumentField` attached to a newly
        saved document.
        """
        class Page(EmbeddedDocument):
            log_message = StringField(verbose_name="Log message",
                                      required=True)

        class Site(Document):
            page = EmbeddedDocumentField(Page)

        Site.drop_collection()
        site = Site(page=Page(log_message="Warning: Dummy message"))
        site.save()

        # Update
        site.page.log_message = "Error: Dummy message"
        site.save()

        site = Site.objects.first()
        self.assertEqual(site.page.log_message, "Error: Dummy message")

    def test_updating_an_embedded_document(self):
        """Ensure that a document with an embedded document field may
        be saved in the database.
        """
        class EmployeeDetails(EmbeddedDocument):
            position = StringField()

        class Employee(self.Person):
            salary = IntField()
            details = EmbeddedDocumentField(EmployeeDetails)

        # Create employee object and save it to the database
        employee = Employee(name='Test Employee', age=50, salary=20000)
        employee.details = EmployeeDetails(position='Developer')
        employee.save()

        # Test updating an embedded document
        promoted_employee = Employee.objects.get(name='Test Employee')
        promoted_employee.details.position = 'Senior Developer'
        promoted_employee.save()

        promoted_employee.reload()
        self.assertEqual(promoted_employee.name, 'Test Employee')
        self.assertEqual(promoted_employee.age, 50)

        # Ensure that the 'details' embedded object saved correctly
        self.assertEqual(
            promoted_employee.details.position, 'Senior Developer')

        # Test removal
        promoted_employee.details = None
        promoted_employee.save()

        promoted_employee.reload()
        self.assertEqual(promoted_employee.details, None)

    def test_object_mixins(self):
        class NameMixin(object):
            name = StringField()

        class Foo(EmbeddedDocument, NameMixin):
            quantity = IntField()

        self.assertEqual(['name', 'quantity'], sorted(Foo._fields.keys()))

        class Bar(Document, NameMixin):
            widgets = StringField()

        self.assertEqual(['id', 'name', 'widgets'], sorted(Bar._fields.keys()))

    def test_mixin_inheritance(self):
        class BaseMixIn(object):
            count = IntField()
            data = StringField()

        class DoubleMixIn(BaseMixIn):
            comment = StringField()

        class TestDoc(Document, DoubleMixIn):
            age = IntField()

        TestDoc.drop_collection()
        t = TestDoc(count=12, data="test",
                    comment="great!", age=19)

        t.save()

        t = TestDoc.objects.first()

        self.assertEqual(t.age, 19)
        self.assertEqual(t.comment, "great!")
        self.assertEqual(t.data, "test")
        self.assertEqual(t.count, 12)

    def test_save_reference(self):
        """Ensure that a document reference field may be saved in the
        database.
        """
        class BlogPost(Document):
            meta = {'collection': 'blogpost_1'}
            content = StringField()
            author = ReferenceField(self.Person)

        BlogPost.drop_collection()

        author = self.Person(name='Test User')
        author.save()

        post = BlogPost(content='Watched some TV today... how exciting.')
        # Should only reference author when saving
        post.author = author
        post.save()

        post_obj = BlogPost.objects.first()

        # Test laziness
        self.assertTrue(isinstance(post_obj._data['author'],
                                   bson.DBRef))
        self.assertTrue(isinstance(post_obj.author, self.Person))
        self.assertEqual(post_obj.author.name, 'Test User')

        # Ensure that the dereferenced object may be changed and saved
        post_obj.author.age = 25
        post_obj.author.save()

        author = list(self.Person.objects(name='Test User'))[-1]
        self.assertEqual(author.age, 25)

    def test_duplicate_db_fields_raise_invalid_document_error(self):
        """Ensure a InvalidDocumentError is thrown if duplicate fields
        declare the same db_field.
        """
        with self.assertRaises(InvalidDocumentError):
            class Foo(Document):
                name = StringField()
                name2 = StringField(db_field='name')

    def test_invalid_son(self):
        """Raise an error if loading invalid data."""
        class Occurrence(EmbeddedDocument):
            number = IntField()

        class Word(Document):
            stem = StringField()
            count = IntField(default=1)
            forms = ListField(StringField(), default=list)
            occurs = ListField(EmbeddedDocumentField(Occurrence), default=list)

        with self.assertRaises(InvalidDocumentError):
            Word._from_son({
                'stem': [1, 2, 3],
                'forms': 1,
                'count': 'one',
                'occurs': {"hello": None}
            })

        # Tests for issue #1438: https://github.com/MongoEngine/mongoengine/issues/1438
        with self.assertRaises(ValueError):
            Word._from_son('this is not a valid SON dict')

    def test_reverse_delete_rule_cascade_and_nullify(self):
        """Ensure that a referenced document is also deleted upon
        deletion.
        """
        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(self.Person, reverse_delete_rule=CASCADE)
            reviewer = ReferenceField(self.Person, reverse_delete_rule=NULLIFY)

        self.Person.drop_collection()
        BlogPost.drop_collection()

        author = self.Person(name='Test User')
        author.save()

        reviewer = self.Person(name='Re Viewer')
        reviewer.save()

        post = BlogPost(content='Watched some TV')
        post.author = author
        post.reviewer = reviewer
        post.save()

        reviewer.delete()
        # No effect on the BlogPost
        self.assertEqual(BlogPost.objects.count(), 1)
        self.assertEqual(BlogPost.objects.get().reviewer, None)

        # Delete the Person, which should lead to deletion of the BlogPost, too
        author.delete()
        self.assertEqual(BlogPost.objects.count(), 0)

    def test_reverse_delete_rule_pull(self):
        """Ensure that a referenced document is also deleted with
        pull.
        """
        class Record(Document):
            name = StringField()
            children = ListField(ReferenceField('self', reverse_delete_rule=PULL))

        Record.drop_collection()

        parent_record = Record(name='parent').save()
        child_record = Record(name='child').save()
        parent_record.children.append(child_record)
        parent_record.save()

        child_record.delete()
        self.assertEqual(Record.objects(name='parent').get().children, [])


    def test_reverse_delete_rule_with_custom_id_field(self):
        """Ensure that a referenced document with custom primary key
        is also deleted upon deletion.
        """
        class User(Document):
            name = StringField(primary_key=True)

        class Book(Document):
            author = ReferenceField(User, reverse_delete_rule=CASCADE)
            reviewer = ReferenceField(User, reverse_delete_rule=NULLIFY)

        User.drop_collection()
        Book.drop_collection()

        user = User(name='Mike').save()
        reviewer = User(name='John').save()
        book = Book(author=user, reviewer=reviewer).save()

        reviewer.delete()
        self.assertEqual(Book.objects.count(), 1)
        self.assertEqual(Book.objects.get().reviewer, None)

        user.delete()
        self.assertEqual(Book.objects.count(), 0)

    def test_reverse_delete_rule_with_shared_id_among_collections(self):
        """Ensure that cascade delete rule doesn't mix id among
        collections.
        """
        class User(Document):
            id = IntField(primary_key=True)

        class Book(Document):
            id = IntField(primary_key=True)
            author = ReferenceField(User, reverse_delete_rule=CASCADE)

        User.drop_collection()
        Book.drop_collection()

        user_1 = User(id=1).save()
        user_2 = User(id=2).save()
        book_1 = Book(id=1, author=user_2).save()
        book_2 = Book(id=2, author=user_1).save()

        user_2.delete()
        # Deleting user_2 should also delete book_1 but not book_2
        self.assertEqual(Book.objects.count(), 1)
        self.assertEqual(Book.objects.get(), book_2)

        user_3 = User(id=3).save()
        book_3 = Book(id=3, author=user_3).save()

        user_3.delete()
        # Deleting user_3 should also delete book_3
        self.assertEqual(Book.objects.count(), 1)
        self.assertEqual(Book.objects.get(), book_2)

    def test_reverse_delete_rule_with_document_inheritance(self):
        """Ensure that a referenced document is also deleted upon
        deletion of a child document.
        """
        class Writer(self.Person):
            pass

        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(self.Person, reverse_delete_rule=CASCADE)
            reviewer = ReferenceField(self.Person, reverse_delete_rule=NULLIFY)

        self.Person.drop_collection()
        BlogPost.drop_collection()

        author = Writer(name='Test User')
        author.save()

        reviewer = Writer(name='Re Viewer')
        reviewer.save()

        post = BlogPost(content='Watched some TV')
        post.author = author
        post.reviewer = reviewer
        post.save()

        reviewer.delete()
        self.assertEqual(BlogPost.objects.count(), 1)
        self.assertEqual(BlogPost.objects.get().reviewer, None)

        # Delete the Writer should lead to deletion of the BlogPost
        author.delete()
        self.assertEqual(BlogPost.objects.count(), 0)

    def test_reverse_delete_rule_cascade_and_nullify_complex_field(self):
        """Ensure that a referenced document is also deleted upon
        deletion for complex fields.
        """
        class BlogPost(Document):
            content = StringField()
            authors = ListField(ReferenceField(
                self.Person, reverse_delete_rule=CASCADE))
            reviewers = ListField(ReferenceField(
                self.Person, reverse_delete_rule=NULLIFY))

        self.Person.drop_collection()
        BlogPost.drop_collection()

        author = self.Person(name='Test User')
        author.save()

        reviewer = self.Person(name='Re Viewer')
        reviewer.save()

        post = BlogPost(content='Watched some TV')
        post.authors = [author]
        post.reviewers = [reviewer]
        post.save()

        # Deleting the reviewer should have no effect on the BlogPost
        reviewer.delete()
        self.assertEqual(BlogPost.objects.count(), 1)
        self.assertEqual(BlogPost.objects.get().reviewers, [])

        # Delete the Person, which should lead to deletion of the BlogPost, too
        author.delete()
        self.assertEqual(BlogPost.objects.count(), 0)

    def test_reverse_delete_rule_cascade_triggers_pre_delete_signal(self):
        """Ensure the pre_delete signal is triggered upon a cascading
        deletion setup a blog post with content, an author and editor
        delete the author which triggers deletion of blogpost via
        cascade blog post's pre_delete signal alters an editor attribute.
        """
        class Editor(self.Person):
            review_queue = IntField(default=0)

        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(self.Person, reverse_delete_rule=CASCADE)
            editor = ReferenceField(Editor)

            @classmethod
            def pre_delete(cls, sender, document, **kwargs):
                # decrement the docs-to-review count
                document.editor.update(dec__review_queue=1)

        signals.pre_delete.connect(BlogPost.pre_delete, sender=BlogPost)

        self.Person.drop_collection()
        BlogPost.drop_collection()
        Editor.drop_collection()

        author = self.Person(name='Will S.').save()
        editor = Editor(name='Max P.', review_queue=1).save()
        BlogPost(content='wrote some books', author=author,
                 editor=editor).save()

        # delete the author, the post is also deleted due to the CASCADE rule
        author.delete()

        # the pre-delete signal should have decremented the editor's queue
        editor = Editor.objects(name='Max P.').get()
        self.assertEqual(editor.review_queue, 0)

    def test_two_way_reverse_delete_rule(self):
        """Ensure that Bi-Directional relationships work with
        reverse_delete_rule
        """
        class Bar(Document):
            content = StringField()
            foo = ReferenceField('Foo')

        class Foo(Document):
            content = StringField()
            bar = ReferenceField(Bar)

        Bar.register_delete_rule(Foo, 'bar', NULLIFY)
        Foo.register_delete_rule(Bar, 'foo', NULLIFY)

        Bar.drop_collection()
        Foo.drop_collection()

        b = Bar(content="Hello")
        b.save()

        f = Foo(content="world", bar=b)
        f.save()

        b.foo = f
        b.save()

        f.delete()

        self.assertEqual(Bar.objects.count(), 1)  # No effect on the BlogPost
        self.assertEqual(Bar.objects.get().foo, None)

    def test_invalid_reverse_delete_rule_raise_errors(self):
        with self.assertRaises(InvalidDocumentError):
            class Blog(Document):
                content = StringField()
                authors = MapField(ReferenceField(
                    self.Person, reverse_delete_rule=CASCADE))
                reviewers = DictField(
                    field=ReferenceField(
                        self.Person,
                        reverse_delete_rule=NULLIFY))

        with self.assertRaises(InvalidDocumentError):
            class Parents(EmbeddedDocument):
                father = ReferenceField('Person', reverse_delete_rule=DENY)
                mother = ReferenceField('Person', reverse_delete_rule=DENY)

    def test_reverse_delete_rule_cascade_recurs(self):
        """Ensure that a chain of documents is also deleted upon
        cascaded deletion.
        """
        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(self.Person, reverse_delete_rule=CASCADE)

        class Comment(Document):
            text = StringField()
            post = ReferenceField(BlogPost, reverse_delete_rule=CASCADE)

        self.Person.drop_collection()
        BlogPost.drop_collection()
        Comment.drop_collection()

        author = self.Person(name='Test User')
        author.save()

        post = BlogPost(content='Watched some TV')
        post.author = author
        post.save()

        comment = Comment(text='Kudos.')
        comment.post = post
        comment.save()

        # Delete the Person, which should lead to deletion of the BlogPost,
        # and, recursively to the Comment, too
        author.delete()
        self.assertEqual(Comment.objects.count(), 0)

    def test_reverse_delete_rule_deny(self):
        """Ensure that a document cannot be referenced if there are
        still documents referring to it.
        """
        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(self.Person, reverse_delete_rule=DENY)

        self.Person.drop_collection()
        BlogPost.drop_collection()

        author = self.Person(name='Test User')
        author.save()

        post = BlogPost(content='Watched some TV')
        post.author = author
        post.save()

        # Delete the Person should be denied
        self.assertRaises(OperationError, author.delete)  # Should raise denied error
        self.assertEqual(BlogPost.objects.count(), 1)  # No objects may have been deleted
        self.assertEqual(self.Person.objects.count(), 1)

        # Other users, that don't have BlogPosts must be removable, like normal
        author = self.Person(name='Another User')
        author.save()

        self.assertEqual(self.Person.objects.count(), 2)
        author.delete()
        self.assertEqual(self.Person.objects.count(), 1)

    def subclasses_and_unique_keys_works(self):
        class A(Document):
            pass

        class B(A):
            foo = BooleanField(unique=True)

        A.drop_collection()
        B.drop_collection()

        A().save()
        A().save()
        B(foo=True).save()

        self.assertEqual(A.objects.count(), 2)
        self.assertEqual(B.objects.count(), 1)

    def test_document_hash(self):
        """Test document in list, dict, set."""
        class User(Document):
            pass

        class BlogPost(Document):
            pass

        # Clear old data
        User.drop_collection()
        BlogPost.drop_collection()

        u1 = User.objects.create()
        u2 = User.objects.create()
        u3 = User.objects.create()
        u4 = User()  # New object

        b1 = BlogPost.objects.create()
        b2 = BlogPost.objects.create()

        # Make sure docs are properly identified in a list (__eq__ is used
        # for the comparison).
        all_user_list = list(User.objects.all())
        self.assertTrue(u1 in all_user_list)
        self.assertTrue(u2 in all_user_list)
        self.assertTrue(u3 in all_user_list)
        self.assertTrue(u4 not in all_user_list)  # New object
        self.assertTrue(b1 not in all_user_list)  # Other object
        self.assertTrue(b2 not in all_user_list)  # Other object

        # Make sure docs can be used as keys in a dict (__hash__ is used
        # for hashing the docs).
        all_user_dic = {}
        for u in User.objects.all():
            all_user_dic[u] = "OK"

        self.assertEqual(all_user_dic.get(u1, False), "OK")
        self.assertEqual(all_user_dic.get(u2, False), "OK")
        self.assertEqual(all_user_dic.get(u3, False), "OK")
        self.assertEqual(all_user_dic.get(u4, False), False)  # New object
        self.assertEqual(all_user_dic.get(b1, False), False)  # Other object
        self.assertEqual(all_user_dic.get(b2, False), False)  # Other object

        # Make sure docs are properly identified in a set (__hash__ is used
        # for hashing the docs).
        all_user_set = set(User.objects.all())
        self.assertTrue(u1 in all_user_set)
        self.assertTrue(u4 not in all_user_set)
        self.assertTrue(b1 not in all_user_list)
        self.assertTrue(b2 not in all_user_list)

        # Make sure duplicate docs aren't accepted in the set
        self.assertEqual(len(all_user_set), 3)
        all_user_set.add(u1)
        all_user_set.add(u2)
        all_user_set.add(u3)
        self.assertEqual(len(all_user_set), 3)

    def test_picklable(self):
        pickle_doc = PickleTest(number=1, string="One", lists=['1', '2'])
        pickle_doc.embedded = PickleEmbedded()
        pickled_doc = pickle.dumps(pickle_doc)  # make sure pickling works even before the doc is saved
        pickle_doc.save()

        pickled_doc = pickle.dumps(pickle_doc)
        resurrected = pickle.loads(pickled_doc)

        self.assertEqual(resurrected, pickle_doc)

        # Test pickling changed data
        pickle_doc.lists.append("3")
        pickled_doc = pickle.dumps(pickle_doc)
        resurrected = pickle.loads(pickled_doc)

        self.assertEqual(resurrected, pickle_doc)
        resurrected.string = "Two"
        resurrected.save()

        pickle_doc = PickleTest.objects.first()
        self.assertEqual(resurrected, pickle_doc)
        self.assertEqual(pickle_doc.string, "Two")
        self.assertEqual(pickle_doc.lists, ["1", "2", "3"])

    def test_regular_document_pickle(self):
        pickle_doc = PickleTest(number=1, string="One", lists=['1', '2'])
        pickled_doc = pickle.dumps(pickle_doc)  # make sure pickling works even before the doc is saved
        pickle_doc.save()

        pickled_doc = pickle.dumps(pickle_doc)

        # Test that when a document's definition changes the new
        # definition is used
        fixtures.PickleTest = fixtures.NewDocumentPickleTest

        resurrected = pickle.loads(pickled_doc)
        self.assertEqual(resurrected.__class__,
                         fixtures.NewDocumentPickleTest)
        self.assertEqual(resurrected._fields_ordered,
                         fixtures.NewDocumentPickleTest._fields_ordered)
        self.assertNotEqual(resurrected._fields_ordered,
                            pickle_doc._fields_ordered)

        # The local PickleTest is still a ref to the original
        fixtures.PickleTest = PickleTest

    def test_dynamic_document_pickle(self):
        pickle_doc = PickleDynamicTest(
            name="test", number=1, string="One", lists=['1', '2'])
        pickle_doc.embedded = PickleDynamicEmbedded(foo="Bar")
        pickled_doc = pickle.dumps(pickle_doc)  # make sure pickling works even before the doc is saved

        pickle_doc.save()

        pickled_doc = pickle.dumps(pickle_doc)
        resurrected = pickle.loads(pickled_doc)

        self.assertEqual(resurrected, pickle_doc)
        self.assertEqual(resurrected._fields_ordered,
                         pickle_doc._fields_ordered)
        self.assertEqual(resurrected._dynamic_fields.keys(),
                         pickle_doc._dynamic_fields.keys())

        self.assertEqual(resurrected.embedded, pickle_doc.embedded)
        self.assertEqual(resurrected.embedded._fields_ordered,
                         pickle_doc.embedded._fields_ordered)
        self.assertEqual(resurrected.embedded._dynamic_fields.keys(),
                         pickle_doc.embedded._dynamic_fields.keys())

    def test_picklable_on_signals(self):
        pickle_doc = PickleSignalsTest(
            number=1, string="One", lists=['1', '2'])
        pickle_doc.embedded = PickleEmbedded()
        pickle_doc.save()
        pickle_doc.delete()

    def test_override_method_with_field(self):
        """Test creating a field with a field name that would override
        the "validate" method.
        """
        with self.assertRaises(InvalidDocumentError):
            class Blog(Document):
                validate = DictField()

    def test_mutating_documents(self):
        class B(EmbeddedDocument):
            field1 = StringField(default='field1')

        class A(Document):
            b = EmbeddedDocumentField(B, default=lambda: B())

        A.drop_collection()

        a = A()
        a.save()
        a.reload()
        self.assertEqual(a.b.field1, 'field1')

        class C(EmbeddedDocument):
            c_field = StringField(default='cfield')

        class B(EmbeddedDocument):
            field1 = StringField(default='field1')
            field2 = EmbeddedDocumentField(C, default=lambda: C())

        class A(Document):
            b = EmbeddedDocumentField(B, default=lambda: B())

        a = A.objects()[0]
        a.b.field2.c_field = 'new value'
        a.save()

        a.reload()
        self.assertEqual(a.b.field2.c_field, 'new value')

    def test_can_save_false_values(self):
        """Ensures you can save False values on save."""
        class Doc(Document):
            foo = StringField()
            archived = BooleanField(default=False, required=True)

        Doc.drop_collection()

        d = Doc()
        d.save()
        d.archived = False
        d.save()

        self.assertEqual(Doc.objects(archived=False).count(), 1)

    def test_can_save_false_values_dynamic(self):
        """Ensures you can save False values on dynamic docs."""
        class Doc(DynamicDocument):
            foo = StringField()

        Doc.drop_collection()

        d = Doc()
        d.save()
        d.archived = False
        d.save()

        self.assertEqual(Doc.objects(archived=False).count(), 1)

    def test_do_not_save_unchanged_references(self):
        """Ensures cascading saves dont auto update"""
        class Job(Document):
            name = StringField()

        class Person(Document):
            name = StringField()
            age = IntField()
            job = ReferenceField(Job)

        Job.drop_collection()
        Person.drop_collection()

        job = Job(name="Job 1")
        # job should not have any changed fields after the save
        job.save()

        person = Person(name="name", age=10, job=job)

        from pymongo.collection import Collection
        orig_update = Collection.update
        try:
            def fake_update(*args, **kwargs):
                self.fail("Unexpected update for %s" % args[0].name)
                return orig_update(*args, **kwargs)

            Collection.update = fake_update
            person.save()
        finally:
            Collection.update = orig_update

    def test_db_alias_tests(self):
        """DB Alias tests."""
        # mongoenginetest - Is default connection alias from setUp()
        # Register Aliases
        register_connection('testdb-1', 'mongoenginetest2')
        register_connection('testdb-2', 'mongoenginetest3')
        register_connection('testdb-3', 'mongoenginetest4')

        class User(Document):
            name = StringField()
            meta = {"db_alias": "testdb-1"}

        class Book(Document):
            name = StringField()
            meta = {"db_alias": "testdb-2"}

        # Drops
        User.drop_collection()
        Book.drop_collection()

        # Create
        bob = User.objects.create(name="Bob")
        hp = Book.objects.create(name="Harry Potter")

        # Selects
        self.assertEqual(User.objects.first(), bob)
        self.assertEqual(Book.objects.first(), hp)

        # DeReference
        class AuthorBooks(Document):
            author = ReferenceField(User)
            book = ReferenceField(Book)
            meta = {"db_alias": "testdb-3"}

        # Drops
        AuthorBooks.drop_collection()

        ab = AuthorBooks.objects.create(author=bob, book=hp)

        # select
        self.assertEqual(AuthorBooks.objects.first(), ab)
        self.assertEqual(AuthorBooks.objects.first().book, hp)
        self.assertEqual(AuthorBooks.objects.first().author, bob)
        self.assertEqual(AuthorBooks.objects.filter(author=bob).first(), ab)
        self.assertEqual(AuthorBooks.objects.filter(book=hp).first(), ab)

        # DB Alias
        self.assertEqual(User._get_db(), get_db("testdb-1"))
        self.assertEqual(Book._get_db(), get_db("testdb-2"))
        self.assertEqual(AuthorBooks._get_db(), get_db("testdb-3"))

        # Collections
        self.assertEqual(
            User._get_collection(),
            get_db("testdb-1")[User._get_collection_name()])
        self.assertEqual(
            Book._get_collection(),
            get_db("testdb-2")[Book._get_collection_name()])
        self.assertEqual(
            AuthorBooks._get_collection(),
            get_db("testdb-3")[AuthorBooks._get_collection_name()])

    def test_db_alias_overrides(self):
        """Test db_alias can be overriden."""
        # Register a connection with db_alias testdb-2
        register_connection('testdb-2', 'mongoenginetest2')

        class A(Document):
            """Uses default db_alias
            """
            name = StringField()
            meta = {"allow_inheritance": True}

        class B(A):
            """Uses testdb-2 db_alias
            """
            meta = {"db_alias": "testdb-2"}

        A.objects.all()

        self.assertEqual('testdb-2', B._meta.get('db_alias'))
        self.assertEqual('mongoenginetest',
                         A._get_collection().database.name)
        self.assertEqual('mongoenginetest2',
                         B._get_collection().database.name)

    def test_db_alias_propagates(self):
        """db_alias propagates?"""
        register_connection('testdb-1', 'mongoenginetest2')

        class A(Document):
            name = StringField()
            meta = {"db_alias": "testdb-1", "allow_inheritance": True}

        class B(A):
            pass

        self.assertEqual('testdb-1', B._meta.get('db_alias'))

    def test_db_ref_usage(self):
        """DB Ref usage in dict_fields."""
        class User(Document):
            name = StringField()

        class Book(Document):
            name = StringField()
            author = ReferenceField(User)
            extra = DictField()
            meta = {
                'ordering': ['+name']
            }

            def __unicode__(self):
                return self.name

            def __str__(self):
                return self.name

        # Drops
        User.drop_collection()
        Book.drop_collection()

        # Authors
        bob = User.objects.create(name="Bob")
        jon = User.objects.create(name="Jon")

        # Redactors
        karl = User.objects.create(name="Karl")
        susan = User.objects.create(name="Susan")
        peter = User.objects.create(name="Peter")

        # Bob
        Book.objects.create(name="1", author=bob, extra={
            "a": bob.to_dbref(), "b": [karl.to_dbref(), susan.to_dbref()]})
        Book.objects.create(name="2", author=bob, extra={
            "a": bob.to_dbref(), "b": karl.to_dbref()})
        Book.objects.create(name="3", author=bob, extra={
            "a": bob.to_dbref(), "c": [jon.to_dbref(), peter.to_dbref()]})
        Book.objects.create(name="4", author=bob)

        # Jon
        Book.objects.create(name="5", author=jon)
        Book.objects.create(name="6", author=peter)
        Book.objects.create(name="7", author=jon)
        Book.objects.create(name="8", author=jon)
        Book.objects.create(name="9", author=jon,
                            extra={"a": peter.to_dbref()})

        # Checks
        self.assertEqual(",".join([str(b) for b in Book.objects.all()]),
                         "1,2,3,4,5,6,7,8,9")
        # bob related books
        self.assertEqual(",".join([str(b) for b in Book.objects.filter(
                                  Q(extra__a=bob) |
                                  Q(author=bob) |
                                  Q(extra__b=bob))]),
                         "1,2,3,4")

        # Susan & Karl related books
        self.assertEqual(",".join([str(b) for b in Book.objects.filter(
                                   Q(extra__a__all=[karl, susan]) |
                                   Q(author__all=[karl, susan]) |
                                   Q(extra__b__all=[
                                     karl.to_dbref(), susan.to_dbref()]))
                                   ]), "1")

        # $Where
        self.assertEqual(u",".join([str(b) for b in Book.objects.filter(
                                    __raw__={
                                        "$where": """
                                            function(){
                                                return this.name == '1' ||
                                                       this.name == '2';}"""
                                    })]),
                         "1,2")

    def test_switch_db_instance(self):
        register_connection('testdb-1', 'mongoenginetest2')

        class Group(Document):
            name = StringField()

        Group.drop_collection()
        with switch_db(Group, 'testdb-1') as Group:
            Group.drop_collection()

        Group(name="hello - default").save()
        self.assertEqual(1, Group.objects.count())

        group = Group.objects.first()
        group.switch_db('testdb-1')
        group.name = "hello - testdb!"
        group.save()

        with switch_db(Group, 'testdb-1') as Group:
            group = Group.objects.first()
            self.assertEqual("hello - testdb!", group.name)

        group = Group.objects.first()
        self.assertEqual("hello - default", group.name)

        # Slightly contrived now - perform an update
        # Only works as they have the same object_id
        group.switch_db('testdb-1')
        group.update(set__name="hello - update")

        with switch_db(Group, 'testdb-1') as Group:
            group = Group.objects.first()
            self.assertEqual("hello - update", group.name)
            Group.drop_collection()
            self.assertEqual(0, Group.objects.count())

        group = Group.objects.first()
        self.assertEqual("hello - default", group.name)

        # Totally contrived now - perform a delete
        # Only works as they have the same object_id
        group.switch_db('testdb-1')
        group.delete()

        with switch_db(Group, 'testdb-1') as Group:
            self.assertEqual(0, Group.objects.count())

        group = Group.objects.first()
        self.assertEqual("hello - default", group.name)

    def test_load_undefined_fields(self):
        class User(Document):
            name = StringField()

        User.drop_collection()

        User._get_collection().save({
            'name': 'John',
            'foo': 'Bar',
            'data': [1, 2, 3]
        })

        self.assertRaises(FieldDoesNotExist, User.objects.first)

    def test_load_undefined_fields_with_strict_false(self):
        class User(Document):
            name = StringField()

            meta = {'strict': False}

        User.drop_collection()

        User._get_collection().save({
            'name': 'John',
            'foo': 'Bar',
            'data': [1, 2, 3]
        })

        user = User.objects.first()
        self.assertEqual(user.name, 'John')
        self.assertFalse(hasattr(user, 'foo'))
        self.assertEqual(user._data['foo'], 'Bar')
        self.assertFalse(hasattr(user, 'data'))
        self.assertEqual(user._data['data'], [1, 2, 3])

    def test_load_undefined_fields_on_embedded_document(self):
        class Thing(EmbeddedDocument):
            name = StringField()

        class User(Document):
            name = StringField()
            thing = EmbeddedDocumentField(Thing)

        User.drop_collection()

        User._get_collection().save({
            'name': 'John',
            'thing': {
                'name': 'My thing',
                'foo': 'Bar',
                'data': [1, 2, 3]
            }
        })

        self.assertRaises(FieldDoesNotExist, User.objects.first)

    def test_load_undefined_fields_on_embedded_document_with_strict_false_on_doc(self):
        class Thing(EmbeddedDocument):
            name = StringField()

        class User(Document):
            name = StringField()
            thing = EmbeddedDocumentField(Thing)

            meta = {'strict': False}

        User.drop_collection()

        User._get_collection().save({
            'name': 'John',
            'thing': {
                'name': 'My thing',
                'foo': 'Bar',
                'data': [1, 2, 3]
            }
        })

        self.assertRaises(FieldDoesNotExist, User.objects.first)

    def test_load_undefined_fields_on_embedded_document_with_strict_false(self):
        class Thing(EmbeddedDocument):
            name = StringField()

            meta = {'strict': False}

        class User(Document):
            name = StringField()
            thing = EmbeddedDocumentField(Thing)

        User.drop_collection()

        User._get_collection().save({
            'name': 'John',
            'thing': {
                'name': 'My thing',
                'foo': 'Bar',
                'data': [1, 2, 3]
            }
        })

        user = User.objects.first()
        self.assertEqual(user.name, 'John')
        self.assertEqual(user.thing.name, 'My thing')
        self.assertFalse(hasattr(user.thing, 'foo'))
        self.assertEqual(user.thing._data['foo'], 'Bar')
        self.assertFalse(hasattr(user.thing, 'data'))
        self.assertEqual(user.thing._data['data'], [1, 2, 3])

    def test_spaces_in_keys(self):
        class Embedded(DynamicEmbeddedDocument):
            pass

        class Doc(DynamicDocument):
            pass

        Doc.drop_collection()
        doc = Doc()
        setattr(doc, 'hello world', 1)
        doc.save()

        one = Doc.objects.filter(**{'hello world': 1}).count()
        self.assertEqual(1, one)

    def test_shard_key(self):
        class LogEntry(Document):
            machine = StringField()
            log = StringField()

            meta = {
                'shard_key': ('machine',)
            }

        LogEntry.drop_collection()

        log = LogEntry()
        log.machine = "Localhost"
        log.save()

        self.assertTrue(log.id is not None)

        log.log = "Saving"
        log.save()

        # try to change the shard key
        with self.assertRaises(OperationError):
            log.machine = "127.0.0.1"

    def test_shard_key_in_embedded_document(self):
        class Foo(EmbeddedDocument):
            foo = StringField()

        class Bar(Document):
            meta = {
                'shard_key': ('foo.foo',)
            }
            foo = EmbeddedDocumentField(Foo)
            bar = StringField()

        foo_doc = Foo(foo='hello')
        bar_doc = Bar(foo=foo_doc, bar='world')
        bar_doc.save()

        self.assertTrue(bar_doc.id is not None)

        bar_doc.bar = 'baz'
        bar_doc.save()

        # try to change the shard key
        with self.assertRaises(OperationError):
            bar_doc.foo.foo = 'something'
            bar_doc.save()

    def test_shard_key_primary(self):
        class LogEntry(Document):
            machine = StringField(primary_key=True)
            log = StringField()

            meta = {
                'shard_key': ('machine',)
            }

        LogEntry.drop_collection()

        log = LogEntry()
        log.machine = "Localhost"
        log.save()

        self.assertTrue(log.id is not None)

        log.log = "Saving"
        log.save()

        # try to change the shard key
        with self.assertRaises(OperationError):
            log.machine = "127.0.0.1"

    def test_kwargs_simple(self):
        class Embedded(EmbeddedDocument):
            name = StringField()

        class Doc(Document):
            doc_name = StringField()
            doc = EmbeddedDocumentField(Embedded)

            def __eq__(self, other):
                return (self.doc_name == other.doc_name and
                        self.doc == other.doc)

        classic_doc = Doc(doc_name="my doc", doc=Embedded(name="embedded doc"))
        dict_doc = Doc(**{"doc_name": "my doc",
                          "doc": {"name": "embedded doc"}})

        self.assertEqual(classic_doc, dict_doc)
        self.assertEqual(classic_doc._data, dict_doc._data)

    def test_kwargs_complex(self):
        class Embedded(EmbeddedDocument):
            name = StringField()

        class Doc(Document):
            doc_name = StringField()
            docs = ListField(EmbeddedDocumentField(Embedded))

            def __eq__(self, other):
                return (self.doc_name == other.doc_name and
                        self.docs == other.docs)

        classic_doc = Doc(doc_name="my doc", docs=[
                          Embedded(name="embedded doc1"),
                          Embedded(name="embedded doc2")])
        dict_doc = Doc(**{"doc_name": "my doc",
                          "docs": [{"name": "embedded doc1"},
                                   {"name": "embedded doc2"}]})

        self.assertEqual(classic_doc, dict_doc)
        self.assertEqual(classic_doc._data, dict_doc._data)

    def test_positional_creation(self):
        """Ensure that document may be created using positional arguments."""
        person = self.Person("Test User", 42)
        self.assertEqual(person.name, "Test User")
        self.assertEqual(person.age, 42)

    def test_mixed_creation(self):
        """Ensure that document may be created using mixed arguments."""
        person = self.Person("Test User", age=42)
        self.assertEqual(person.name, "Test User")
        self.assertEqual(person.age, 42)

    def test_positional_creation_embedded(self):
        """Ensure that embedded document may be created using positional
        arguments.
        """
        job = self.Job("Test Job", 4)
        self.assertEqual(job.name, "Test Job")
        self.assertEqual(job.years, 4)

    def test_mixed_creation_embedded(self):
        """Ensure that embedded document may be created using mixed
        arguments.
        """
        job = self.Job("Test Job", years=4)
        self.assertEqual(job.name, "Test Job")
        self.assertEqual(job.years, 4)

    def test_mixed_creation_dynamic(self):
        """Ensure that document may be created using mixed arguments."""
        class Person(DynamicDocument):
            name = StringField()

        person = Person("Test User", age=42)
        self.assertEqual(person.name, "Test User")
        self.assertEqual(person.age, 42)

    def test_bad_mixed_creation(self):
        """Ensure that document gives correct error when duplicating
        arguments.
        """
        with self.assertRaises(TypeError):
            return self.Person("Test User", 42, name="Bad User")

    def test_data_contains_id_field(self):
        """Ensure that asking for _data returns 'id'."""
        class Person(Document):
            name = StringField()

        Person.drop_collection()
        Person(name="Harry Potter").save()

        person = Person.objects.first()
        self.assertTrue('id' in person._data.keys())
        self.assertEqual(person._data.get('id'), person.id)

    def test_complex_nesting_document_and_embedded_document(self):
        class Macro(EmbeddedDocument):
            value = DynamicField(default="UNDEFINED")

        class Parameter(EmbeddedDocument):
            macros = MapField(EmbeddedDocumentField(Macro))

            def expand(self):
                self.macros["test"] = Macro()

        class Node(Document):
            parameters = MapField(EmbeddedDocumentField(Parameter))

            def expand(self):
                self.flattened_parameter = {}
                for parameter_name, parameter in self.parameters.iteritems():
                    parameter.expand()

        class NodesSystem(Document):
            name = StringField(required=True)
            nodes = MapField(ReferenceField(Node, dbref=False))

            def save(self, *args, **kwargs):
                for node_name, node in self.nodes.iteritems():
                    node.expand()
                    node.save(*args, **kwargs)
                super(NodesSystem, self).save(*args, **kwargs)

        NodesSystem.drop_collection()
        Node.drop_collection()

        system = NodesSystem(name="system")
        system.nodes["node"] = Node()
        system.save()
        system.nodes["node"].parameters["param"] = Parameter()
        system.save()

        system = NodesSystem.objects.first()
        self.assertEqual(
            "UNDEFINED",
            system.nodes["node"].parameters["param"].macros["test"].value)

    def test_embedded_document_equality(self):
        class Test(Document):
            field = StringField(required=True)

        class Embedded(EmbeddedDocument):
            ref = ReferenceField(Test)

        Test.drop_collection()
        test = Test(field='123').save()      # has id

        e = Embedded(ref=test)
        f1 = Embedded._from_son(e.to_mongo())
        f2 = Embedded._from_son(e.to_mongo())

        self.assertEqual(f1, f2)
        f1.ref  # Dereferences lazily
        self.assertEqual(f1, f2)

    def test_dbref_equality(self):
        class Test2(Document):
            name = StringField()

        class Test3(Document):
            name = StringField()

        class Test(Document):
            name = StringField()
            test2 = ReferenceField('Test2')
            test3 = ReferenceField('Test3')

        Test.drop_collection()
        Test2.drop_collection()
        Test3.drop_collection()

        t2 = Test2(name='a')
        t2.save()

        t3 = Test3(name='x')
        t3.id = t2.id
        t3.save()

        t = Test(name='b', test2=t2, test3=t3)

        f = Test._from_son(t.to_mongo())

        dbref2 = f._data['test2']
        obj2 = f.test2
        self.assertTrue(isinstance(dbref2, DBRef))
        self.assertTrue(isinstance(obj2, Test2))
        self.assertTrue(obj2.id == dbref2.id)
        self.assertTrue(obj2 == dbref2)
        self.assertTrue(dbref2 == obj2)

        dbref3 = f._data['test3']
        obj3 = f.test3
        self.assertTrue(isinstance(dbref3, DBRef))
        self.assertTrue(isinstance(obj3, Test3))
        self.assertTrue(obj3.id == dbref3.id)
        self.assertTrue(obj3 == dbref3)
        self.assertTrue(dbref3 == obj3)

        self.assertTrue(obj2.id == obj3.id)
        self.assertTrue(dbref2.id == dbref3.id)
        self.assertFalse(dbref2 == dbref3)
        self.assertFalse(dbref3 == dbref2)
        self.assertTrue(dbref2 != dbref3)
        self.assertTrue(dbref3 != dbref2)

        self.assertFalse(obj2 == dbref3)
        self.assertFalse(dbref3 == obj2)
        self.assertTrue(obj2 != dbref3)
        self.assertTrue(dbref3 != obj2)

        self.assertFalse(obj3 == dbref2)
        self.assertFalse(dbref2 == obj3)
        self.assertTrue(obj3 != dbref2)
        self.assertTrue(dbref2 != obj3)

    def test_default_values(self):
        class Person(Document):
            created_on = DateTimeField(default=lambda: datetime.utcnow())
            name = StringField()

        p = Person(name='alon')
        p.save()
        orig_created_on = Person.objects().only('created_on')[0].created_on

        p2 = Person.objects().only('name')[0]
        p2.name = 'alon2'
        p2.save()
        p3 = Person.objects().only('created_on')[0]
        self.assertEquals(orig_created_on, p3.created_on)

        class Person(Document):
            created_on = DateTimeField(default=lambda: datetime.utcnow())
            name = StringField()
            height = IntField(default=189)

        p4 = Person.objects()[0]
        p4.save()
        self.assertEquals(p4.height, 189)

        # However the default will not be fixed in DB
        self.assertEquals(Person.objects(height=189).count(), 0)

        # alter DB for the new default
        coll = Person._get_collection()
        for person in Person.objects.as_pymongo():
            if 'height' not in person:
                person['height'] = 189
                coll.save(person)

        self.assertEquals(Person.objects(height=189).count(), 1)

    def test_from_son(self):
        # 771
        class MyPerson(self.Person):
            meta = dict(shard_key=["id"])
        p = MyPerson.from_json('{"name": "name", "age": 27}', created=True)
        self.assertEquals(p.id, None)
        p.id = "12345"  # in case it is not working: "OperationError: Shard Keys are immutable..." will be raised here
        p = MyPerson._from_son({"name": "name", "age": 27}, created=True)
        self.assertEquals(p.id, None)
        p.id = "12345"  # in case it is not working: "OperationError: Shard Keys are immutable..." will be raised here

    def test_null_field(self):
        # 734
        class User(Document):
            name = StringField()
            height = IntField(default=184, null=True)
            str_fld = StringField(null=True)
            int_fld = IntField(null=True)
            flt_fld = FloatField(null=True)
            dt_fld = DateTimeField(null=True)
            cdt_fld = ComplexDateTimeField(null=True)

        User.objects.delete()
        u = User(name='user')
        u.save()
        u_from_db = User.objects.get(name='user')
        u_from_db.height = None
        u_from_db.save()
        self.assertEquals(u_from_db.height, None)
        # 864
        self.assertEqual(u_from_db.str_fld, None)
        self.assertEqual(u_from_db.int_fld, None)
        self.assertEqual(u_from_db.flt_fld, None)
        self.assertEqual(u_from_db.dt_fld, None)
        self.assertEqual(u_from_db.cdt_fld, None)

        # 735
        User.objects.delete()
        u = User(name='user')
        u.save()
        User.objects(name='user').update_one(set__height=None, upsert=True)
        u_from_db = User.objects.get(name='user')
        self.assertEquals(u_from_db.height, None)

    def test_not_saved_eq(self):
        """Ensure we can compare documents not saved.
        """
        class Person(Document):
            pass

        p = Person()
        p1 = Person()
        self.assertNotEqual(p, p1)
        self.assertEqual(p, p)

    def test_list_iter(self):
        # 914
        class B(EmbeddedDocument):
            v = StringField()

        class A(Document):
            l = ListField(EmbeddedDocumentField(B))

        A.objects.delete()
        A(l=[B(v='1'), B(v='2'), B(v='3')]).save()
        a = A.objects.get()
        self.assertEqual(a.l._instance, a)
        for idx, b in enumerate(a.l):
            self.assertEqual(b._instance, a)
        self.assertEqual(idx, 2)

    def test_falsey_pk(self):
        """Ensure that we can create and update a document with Falsey PK."""
        class Person(Document):
            age = IntField(primary_key=True)
            height = FloatField()

        person = Person()
        person.age = 0
        person.height = 1.89
        person.save()

        person.update(set__height=2.0)

    @needs_mongodb_v26
    def test_push_with_position(self):
        """Ensure that push with position works properly for an instance."""
        class BlogPost(Document):
            slug = StringField()
            tags = ListField(StringField())

        blog = BlogPost()
        blog.slug = "ABC"
        blog.tags = ["python"]
        blog.save()

        blog.update(push__tags__0=["mongodb", "code"])
        blog.reload()
        self.assertEqual(blog.tags, ['mongodb', 'code', 'python'])

    def test_push_nested_list(self):
        """Ensure that push update works in nested list"""
        class BlogPost(Document):
            slug = StringField()
            tags = ListField()

        blog = BlogPost(slug="test").save()
        blog.update(push__tags=["value1", 123])
        blog.reload()
        self.assertEqual(blog.tags, [["value1", 123]])


if __name__ == '__main__':
    unittest.main()
