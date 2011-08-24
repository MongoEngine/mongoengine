import pickle
import pymongo
import unittest
import warnings

from datetime import datetime

import pymongo
import pickle
import weakref

from fixtures import Base, Mixin, PickleEmbedded, PickleTest

from mongoengine import *
from mongoengine.base import _document_registry, NotRegistered
from mongoengine.connection import _get_db


class DocumentTest(unittest.TestCase):

    def setUp(self):
        connect(db='mongoenginetest')
        self.db = _get_db()

        class Person(Document):
            name = StringField()
            age = IntField()
        self.Person = Person

    def tearDown(self):
        self.Person.drop_collection()

    def test_drop_collection(self):
        """Ensure that the collection may be dropped from the database.
        """
        self.Person(name='Test').save()

        collection = self.Person._get_collection_name()
        self.assertTrue(collection in self.db.collection_names())

        self.Person.drop_collection()
        self.assertFalse(collection in self.db.collection_names())

    def test_definition(self):
        """Ensure that document may be defined using fields.
        """
        name_field = StringField()
        age_field = IntField()

        class Person(Document):
            name = name_field
            age = age_field
            non_field = True

        self.assertEqual(Person._fields['name'], name_field)
        self.assertEqual(Person._fields['age'], age_field)
        self.assertFalse('non_field' in Person._fields)
        self.assertTrue('id' in Person._fields)
        # Test iteration over fields
        fields = list(Person())
        self.assertTrue('name' in fields and 'age' in fields)
        # Ensure Document isn't treated like an actual document
        self.assertFalse(hasattr(Document, '_fields'))

    def test_collection_name(self):
        """Ensure that a collection with a specified name may be used.
        """

        class DefaultNamingTest(Document):
            pass
        self.assertEquals('default_naming_test', DefaultNamingTest._get_collection_name())

        class CustomNamingTest(Document):
            meta = {'collection': 'pimp_my_collection'}

        self.assertEquals('pimp_my_collection', CustomNamingTest._get_collection_name())

        class DynamicNamingTest(Document):
            meta = {'collection': lambda c: "DYNAMO"}
        self.assertEquals('DYNAMO', DynamicNamingTest._get_collection_name())

        # Use Abstract class to handle backwards compatibility
        class BaseDocument(Document):
            meta = {
                'abstract': True,
                'collection': lambda c: c.__name__.lower()
            }

        class OldNamingConvention(BaseDocument):
            pass
        self.assertEquals('oldnamingconvention', OldNamingConvention._get_collection_name())

        class InheritedAbstractNamingTest(BaseDocument):
            meta = {'collection': 'wibble'}
        self.assertEquals('wibble', InheritedAbstractNamingTest._get_collection_name())

        with warnings.catch_warnings(record=True) as w:
            # Cause all warnings to always be triggered.
            warnings.simplefilter("always")

            class NonAbstractBase(Document):
                pass

            class InheritedDocumentFailTest(NonAbstractBase):
                meta = {'collection': 'fail'}

            self.assertTrue(issubclass(w[0].category, SyntaxWarning))
            self.assertEquals('non_abstract_base', InheritedDocumentFailTest._get_collection_name())

        # Mixin tests
        class BaseMixin(object):
            meta = {
                'collection': lambda c: c.__name__.lower()
            }

        class OldMixinNamingConvention(Document, BaseMixin):
            pass
        self.assertEquals('oldmixinnamingconvention', OldMixinNamingConvention._get_collection_name())

        class BaseMixin(object):
            meta = {
                'collection': lambda c: c.__name__.lower()
            }

        class BaseDocument(Document, BaseMixin):
            pass

        class MyDocument(BaseDocument):
            pass
        self.assertEquals('mydocument', OldMixinNamingConvention._get_collection_name())

    def test_get_superclasses(self):
        """Ensure that the correct list of superclasses is assembled.
        """
        class Animal(Document): pass
        class Fish(Animal): pass
        class Mammal(Animal): pass
        class Human(Mammal): pass
        class Dog(Mammal): pass

        mammal_superclasses = {'Animal': Animal}
        self.assertEqual(Mammal._superclasses, mammal_superclasses)

        dog_superclasses = {
            'Animal': Animal,
            'Animal.Mammal': Mammal,
        }
        self.assertEqual(Dog._superclasses, dog_superclasses)

    def test_get_subclasses(self):
        """Ensure that the correct list of subclasses is retrieved by the
        _get_subclasses method.
        """
        class Animal(Document): pass
        class Fish(Animal): pass
        class Mammal(Animal): pass
        class Human(Mammal): pass
        class Dog(Mammal): pass

        mammal_subclasses = {
            'Animal.Mammal.Dog': Dog,
            'Animal.Mammal.Human': Human
        }
        self.assertEqual(Mammal._get_subclasses(), mammal_subclasses)

        animal_subclasses = {
            'Animal.Fish': Fish,
            'Animal.Mammal': Mammal,
            'Animal.Mammal.Dog': Dog,
            'Animal.Mammal.Human': Human
        }
        self.assertEqual(Animal._get_subclasses(), animal_subclasses)

    def test_external_super_and_sub_classes(self):
        """Ensure that the correct list of sub and super classes is assembled.
        when importing part of the model
        """
        class Animal(Base): pass
        class Fish(Animal): pass
        class Mammal(Animal): pass
        class Human(Mammal): pass
        class Dog(Mammal): pass

        mammal_superclasses = {'Base': Base, 'Base.Animal': Animal}
        self.assertEqual(Mammal._superclasses, mammal_superclasses)

        dog_superclasses = {
            'Base': Base,
            'Base.Animal': Animal,
            'Base.Animal.Mammal': Mammal,
        }
        self.assertEqual(Dog._superclasses, dog_superclasses)

        animal_subclasses = {
            'Base.Animal.Fish': Fish,
            'Base.Animal.Mammal': Mammal,
            'Base.Animal.Mammal.Dog': Dog,
            'Base.Animal.Mammal.Human': Human
        }
        self.assertEqual(Animal._get_subclasses(), animal_subclasses)

        mammal_subclasses = {
            'Base.Animal.Mammal.Dog': Dog,
            'Base.Animal.Mammal.Human': Human
        }
        self.assertEqual(Mammal._get_subclasses(), mammal_subclasses)

        Base.drop_collection()

        h = Human()
        h.save()

        self.assertEquals(Human.objects.count(), 1)
        self.assertEquals(Mammal.objects.count(), 1)
        self.assertEquals(Animal.objects.count(), 1)
        self.assertEquals(Base.objects.count(), 1)
        Base.drop_collection()

    def test_polymorphic_queries(self):
        """Ensure that the correct subclasses are returned from a query"""
        class Animal(Document): pass
        class Fish(Animal): pass
        class Mammal(Animal): pass
        class Human(Mammal): pass
        class Dog(Mammal): pass

        Animal.drop_collection()

        Animal().save()
        Fish().save()
        Mammal().save()
        Human().save()
        Dog().save()

        classes = [obj.__class__ for obj in Animal.objects]
        self.assertEqual(classes, [Animal, Fish, Mammal, Human, Dog])

        classes = [obj.__class__ for obj in Mammal.objects]
        self.assertEqual(classes, [Mammal, Human, Dog])

        classes = [obj.__class__ for obj in Human.objects]
        self.assertEqual(classes, [Human])

        Animal.drop_collection()

    def test_polymorphic_references(self):
        """Ensure that the correct subclasses are returned from a query when
        using references / generic references
        """
        class Animal(Document): pass
        class Fish(Animal): pass
        class Mammal(Animal): pass
        class Human(Mammal): pass
        class Dog(Mammal): pass

        class Zoo(Document):
            animals = ListField(ReferenceField(Animal))

        Zoo.drop_collection()
        Animal.drop_collection()

        Animal().save()
        Fish().save()
        Mammal().save()
        Human().save()
        Dog().save()

        # Save a reference to each animal
        zoo = Zoo(animals=Animal.objects)
        zoo.save()
        zoo.reload()

        classes = [a.__class__ for a in Zoo.objects.first().animals]
        self.assertEqual(classes, [Animal, Fish, Mammal, Human, Dog])

        Zoo.drop_collection()

        class Zoo(Document):
            animals = ListField(GenericReferenceField(Animal))

        # Save a reference to each animal
        zoo = Zoo(animals=Animal.objects)
        zoo.save()
        zoo.reload()

        classes = [a.__class__ for a in Zoo.objects.first().animals]
        self.assertEqual(classes, [Animal, Fish, Mammal, Human, Dog])

        Zoo.drop_collection()
        Animal.drop_collection()

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

        for i in xrange(10):
            s = Stats()
            s.save()
            list_stats.append(s)

        cmp_stats = CompareStats(stats=list_stats)
        cmp_stats.save()

        self.assertEqual(list_stats, CompareStats.objects.first().stats)

    def test_inheritance(self):
        """Ensure that document may inherit fields from a superclass document.
        """
        class Employee(self.Person):
            salary = IntField()

        self.assertTrue('name' in Employee._fields)
        self.assertTrue('salary' in Employee._fields)
        self.assertEqual(Employee._get_collection_name(),
                         self.Person._get_collection_name())

        # Ensure that MRO error is not raised
        class A(Document): pass
        class B(A): pass
        class C(B): pass

    def test_allow_inheritance(self):
        """Ensure that inheritance may be disabled on simple classes and that
        _cls and _types will not be used.
        """

        class Animal(Document):
            name = StringField()
            meta = {'allow_inheritance': False}

        Animal.drop_collection()
        def create_dog_class():
            class Dog(Animal):
                pass
        self.assertRaises(ValueError, create_dog_class)

        # Check that _cls etc aren't present on simple documents
        dog = Animal(name='dog')
        dog.save()
        collection = self.db[Animal._get_collection_name()]
        obj = collection.find_one()
        self.assertFalse('_cls' in obj)
        self.assertFalse('_types' in obj)

        Animal.drop_collection()

        def create_employee_class():
            class Employee(self.Person):
                meta = {'allow_inheritance': False}
        self.assertRaises(ValueError, create_employee_class)

        # Test the same for embedded documents
        class Comment(EmbeddedDocument):
            content = StringField()
            meta = {'allow_inheritance': False}

        def create_special_comment():
            class SpecialComment(Comment):
                pass
        self.assertRaises(ValueError, create_special_comment)

        comment = Comment(content='test')
        self.assertFalse('_cls' in comment.to_mongo())
        self.assertFalse('_types' in comment.to_mongo())

    def test_allow_inheritance_abstract_document(self):
        """Ensure that abstract documents can set inheritance rules and that
        _cls and _types will not be used.
        """
        class FinalDocument(Document):
            meta = {'abstract': True,
                    'allow_inheritance': False}

        class Animal(FinalDocument):
            name = StringField()

        Animal.drop_collection()
        def create_dog_class():
            class Dog(Animal):
                pass
        self.assertRaises(ValueError, create_dog_class)

        # Check that _cls etc aren't present on simple documents
        dog = Animal(name='dog')
        dog.save()
        collection = self.db[Animal._get_collection_name()]
        obj = collection.find_one()
        self.assertFalse('_cls' in obj)
        self.assertFalse('_types' in obj)

        Animal.drop_collection()

    def test_how_to_turn_off_inheritance(self):
        """Demonstrates migrating from allow_inheritance = True to False.
        """
        class Animal(Document):
            name = StringField()
            meta = {
                'indexes': ['name']
            }

        Animal.drop_collection()

        dog = Animal(name='dog')
        dog.save()

        collection = self.db[Animal._get_collection_name()]
        obj = collection.find_one()
        self.assertTrue('_cls' in obj)
        self.assertTrue('_types' in obj)

        info = collection.index_information()
        info = [value['key'] for key, value in info.iteritems()]
        self.assertEquals([[(u'_id', 1)], [(u'_types', 1), (u'name', 1)]], info)

        # Turn off inheritance
        class Animal(Document):
            name = StringField()
            meta = {
                'allow_inheritance': False,
                'indexes': ['name']
            }
        collection.update({}, {"$unset": {"_types": 1, "_cls": 1}},  multi=True)

        # Confirm extra data is removed
        obj = collection.find_one()
        self.assertFalse('_cls' in obj)
        self.assertFalse('_types' in obj)

        info = collection.index_information()
        info = [value['key'] for key, value in info.iteritems()]
        self.assertEquals([[(u'_id', 1)], [(u'_types', 1), (u'name', 1)]], info)

        info = collection.index_information()
        indexes_to_drop = [key for key, value in info.iteritems() if '_types' in dict(value['key'])]
        for index in indexes_to_drop:
            collection.drop_index(index)

        info = collection.index_information()
        info = [value['key'] for key, value in info.iteritems()]
        self.assertEquals([[(u'_id', 1)]], info)

        # Recreate indexes
        dog = Animal.objects.first()
        dog.save()
        info = collection.index_information()
        info = [value['key'] for key, value in info.iteritems()]
        self.assertEquals([[(u'_id', 1)], [(u'name', 1),]], info)

        Animal.drop_collection()

    def test_abstract_documents(self):
        """Ensure that a document superclass can be marked as abstract
        thereby not using it as the name for the collection."""

        class Animal(Document):
            name = StringField()
            meta = {'abstract': True}

        class Fish(Animal): pass
        class Guppy(Fish): pass

        class Mammal(Animal):
            meta = {'abstract': True}
        class Human(Mammal): pass

        self.assertFalse('collection' in Animal._meta)
        self.assertFalse('collection' in Mammal._meta)

        self.assertEqual(Animal._get_collection_name(), None)
        self.assertEqual(Mammal._get_collection_name(), None)

        self.assertEqual(Fish._get_collection_name(), 'fish')
        self.assertEqual(Guppy._get_collection_name(), 'fish')
        self.assertEqual(Human._get_collection_name(), 'human')

        def create_bad_abstract():
            class EvilHuman(Human):
                evil = BooleanField(default=True)
                meta = {'abstract': True}
        self.assertRaises(ValueError, create_bad_abstract)

    def test_collection_name(self):
        """Ensure that a collection with a specified name may be used.
        """
        collection = 'personCollTest'
        if collection in self.db.collection_names():
            self.db.drop_collection(collection)

        class Person(Document):
            name = StringField()
            meta = {'collection': collection}

        user = Person(name="Test User")
        user.save()
        self.assertTrue(collection in self.db.collection_names())

        user_obj = self.db[collection].find_one()
        self.assertEqual(user_obj['name'], "Test User")

        user_obj = Person.objects[0]
        self.assertEqual(user_obj.name, "Test User")

        Person.drop_collection()
        self.assertFalse(collection in self.db.collection_names())

    def test_collection_name_and_primary(self):
        """Ensure that a collection with a specified name may be used.
        """

        class Person(Document):
            name = StringField(primary_key=True)
            meta = {'collection': 'app'}

        user = Person(name="Test User")
        user.save()

        user_obj = Person.objects[0]
        self.assertEqual(user_obj.name, "Test User")

        Person.drop_collection()

    def test_inherited_collections(self):
        """Ensure that subclassed documents don't override parents' collections.
        """
        with warnings.catch_warnings(record=True) as w:
            # Cause all warnings to always be triggered.
            warnings.simplefilter("always")

            class Drink(Document):
                name = StringField()

            class AlcoholicDrink(Drink):
                meta = {'collection': 'booze'}

            class Drinker(Document):
                drink = GenericReferenceField()

            # Confirm we triggered a SyntaxWarning
            assert issubclass(w[0].category, SyntaxWarning)

        Drink.drop_collection()
        AlcoholicDrink.drop_collection()
        Drinker.drop_collection()

        red_bull = Drink(name='Red Bull')
        red_bull.save()

        programmer = Drinker(drink=red_bull)
        programmer.save()

        beer = AlcoholicDrink(name='Beer')
        beer.save()
        real_person = Drinker(drink=beer)
        real_person.save()

        self.assertEqual(Drinker.objects[0].drink.name, red_bull.name)
        self.assertEqual(Drinker.objects[1].drink.name, beer.name)

    def test_capped_collection(self):
        """Ensure that capped collections work properly.
        """
        class Log(Document):
            date = DateTimeField(default=datetime.now)
            meta = {
                'max_documents': 10,
                'max_size': 90000,
            }

        Log.drop_collection()

        # Ensure that the collection handles up to its maximum
        for i in range(10):
            Log().save()

        self.assertEqual(len(Log.objects), 10)

        # Check that extra documents don't increase the size
        Log().save()
        self.assertEqual(len(Log.objects), 10)

        options = Log.objects._collection.options()
        self.assertEqual(options['capped'], True)
        self.assertEqual(options['max'], 10)
        self.assertEqual(options['size'], 90000)

        # Check that the document cannot be redefined with different options
        def recreate_log_document():
            class Log(Document):
                date = DateTimeField(default=datetime.now)
                meta = {
                    'max_documents': 11,
                }
            # Create the collection by accessing Document.objects
            Log.objects
        self.assertRaises(InvalidCollectionError, recreate_log_document)

        Log.drop_collection()

    def test_indexes(self):
        """Ensure that indexes are used when meta[indexes] is specified.
        """
        class BlogPost(Document):
            date = DateTimeField(db_field='addDate', default=datetime.now)
            category = StringField()
            tags = ListField(StringField())
            meta = {
                'indexes': [
                    '-date',
                    'tags',
                    ('category', '-date')
                ],
            }

        BlogPost.drop_collection()

        info = BlogPost.objects._collection.index_information()
        # _id, '-date', 'tags', ('cat', 'date')
        # NB: there is no index on _types by itself, since
        # the indices on -date and tags will both contain
        # _types as first element in the key
        self.assertEqual(len(info), 4)

        # Indexes are lazy so use list() to perform query
        list(BlogPost.objects)
        info = BlogPost.objects._collection.index_information()
        info = [value['key'] for key, value in info.iteritems()]
        self.assertTrue([('_types', 1), ('category', 1), ('addDate', -1)]
                        in info)
        self.assertTrue([('_types', 1), ('addDate', -1)] in info)
        # tags is a list field so it shouldn't have _types in the index
        self.assertTrue([('tags', 1)] in info)

        class ExtendedBlogPost(BlogPost):
            title = StringField()
            meta = {'indexes': ['title']}

        BlogPost.drop_collection()

        list(ExtendedBlogPost.objects)
        info = ExtendedBlogPost.objects._collection.index_information()
        info = [value['key'] for key, value in info.iteritems()]
        self.assertTrue([('_types', 1), ('category', 1), ('addDate', -1)]
                        in info)
        self.assertTrue([('_types', 1), ('addDate', -1)] in info)
        self.assertTrue([('_types', 1), ('title', 1)] in info)

        BlogPost.drop_collection()

    def test_dictionary_indexes(self):
        """Ensure that indexes are used when meta[indexes] contains dictionaries
        instead of lists.
        """
        class BlogPost(Document):
            date = DateTimeField(db_field='addDate', default=datetime.now)
            category = StringField()
            tags = ListField(StringField())
            meta = {
                'indexes': [
                    { 'fields': ['-date'], 'unique': True,
                      'sparse': True, 'types': False },
                ],
            }

        BlogPost.drop_collection()

        info = BlogPost.objects._collection.index_information()
        # _id, '-date'
        self.assertEqual(len(info), 3)

        # Indexes are lazy so use list() to perform query
        list(BlogPost.objects)
        info = BlogPost.objects._collection.index_information()
        info = [(value['key'],
                 value.get('unique', False),
                 value.get('sparse', False))
                for key, value in info.iteritems()]
        self.assertTrue(([('addDate', -1)], True, True) in info)

        BlogPost.drop_collection()

    def test_embedded_document_index(self):
        """Tests settings an index on an embedded document
        """
        class Date(EmbeddedDocument):
            year = IntField(db_field='yr')

        class BlogPost(Document):
            title = StringField()
            date = EmbeddedDocumentField(Date)

            meta = {
                'indexes': [
                    '-date.year'
                ],
            }

        BlogPost.drop_collection()

        info = BlogPost.objects._collection.index_information()
        self.assertEqual(info.keys(), ['_types_1_date.yr_-1', '_id_'])
        BlogPost.drop_collection()

    def test_list_embedded_document_index(self):
        """Ensure list embedded documents can be indexed
        """
        class Tag(EmbeddedDocument):
            name = StringField(db_field='tag')

        class BlogPost(Document):
            title = StringField()
            tags = ListField(EmbeddedDocumentField(Tag))

            meta = {
                'indexes': [
                    'tags.name'
                ],
            }

        BlogPost.drop_collection()

        info = BlogPost.objects._collection.index_information()
        # we don't use _types in with list fields by default
        self.assertEqual(info.keys(), ['_id_', '_types_1', 'tags.tag_1'])

        post1 = BlogPost(title="Embedded Indexes tests in place",
                        tags=[Tag(name="about"), Tag(name="time")]
                )
        post1.save()
        BlogPost.drop_collection()

    def test_geo_indexes_recursion(self):

        class User(Document):
            channel = ReferenceField('Channel')
            location = GeoPointField()

        class Channel(Document):
            user = ReferenceField('User')
            location = GeoPointField()

        self.assertEquals(len(User._geo_indices()), 2)

    def test_hint(self):

        class BlogPost(Document):
            tags = ListField(StringField())
            meta = {
                'indexes': [
                    'tags',
                ],
            }

        BlogPost.drop_collection()

        for i in xrange(0, 10):
            tags = [("tag %i" % n) for n in xrange(0, i % 2)]
            BlogPost(tags=tags).save()

        self.assertEquals(BlogPost.objects.count(), 10)
        self.assertEquals(BlogPost.objects.hint().count(), 10)
        self.assertEquals(BlogPost.objects.hint([('tags', 1)]).count(), 10)

        self.assertEquals(BlogPost.objects.hint([('ZZ', 1)]).count(), 10)

        def invalid_index():
            BlogPost.objects.hint('tags')
        self.assertRaises(TypeError, invalid_index)

        def invalid_index_2():
            return BlogPost.objects.hint(('tags', 1))
        self.assertRaises(TypeError, invalid_index_2)

    def test_unique(self):
        """Ensure that uniqueness constraints are applied to fields.
        """
        class BlogPost(Document):
            title = StringField()
            slug = StringField(unique=True)

        BlogPost.drop_collection()

        post1 = BlogPost(title='test1', slug='test')
        post1.save()

        # Two posts with the same slug is not allowed
        post2 = BlogPost(title='test2', slug='test')
        self.assertRaises(OperationError, post2.save)

    def test_unique_with(self):
        """Ensure that unique_with constraints are applied to fields.
        """
        class Date(EmbeddedDocument):
            year = IntField(db_field='yr')

        class BlogPost(Document):
            title = StringField()
            date = EmbeddedDocumentField(Date)
            slug = StringField(unique_with='date.year')

        BlogPost.drop_collection()

        post1 = BlogPost(title='test1', date=Date(year=2009), slug='test')
        post1.save()

        # day is different so won't raise exception
        post2 = BlogPost(title='test2', date=Date(year=2010), slug='test')
        post2.save()

        # Now there will be two docs with the same slug and the same day: fail
        post3 = BlogPost(title='test3', date=Date(year=2010), slug='test')
        self.assertRaises(OperationError, post3.save)

        BlogPost.drop_collection()

    def test_unique_embedded_document(self):
        """Ensure that uniqueness constraints are applied to fields on embedded documents.
        """
        class SubDocument(EmbeddedDocument):
            year = IntField(db_field='yr')
            slug = StringField(unique=True)

        class BlogPost(Document):
            title = StringField()
            sub = EmbeddedDocumentField(SubDocument)

        BlogPost.drop_collection()

        post1 = BlogPost(title='test1', sub=SubDocument(year=2009, slug="test"))
        post1.save()

        # sub.slug is different so won't raise exception
        post2 = BlogPost(title='test2', sub=SubDocument(year=2010, slug='another-slug'))
        post2.save()

        # Now there will be two docs with the same sub.slug
        post3 = BlogPost(title='test3', sub=SubDocument(year=2010, slug='test'))
        self.assertRaises(OperationError, post3.save)

        BlogPost.drop_collection()

    def test_unique_with_embedded_document_and_embedded_unique(self):
        """Ensure that uniqueness constraints are applied to fields on
        embedded documents.  And work with unique_with as well.
        """
        class SubDocument(EmbeddedDocument):
            year = IntField(db_field='yr')
            slug = StringField(unique=True)

        class BlogPost(Document):
            title = StringField(unique_with='sub.year')
            sub = EmbeddedDocumentField(SubDocument)

        BlogPost.drop_collection()

        post1 = BlogPost(title='test1', sub=SubDocument(year=2009, slug="test"))
        post1.save()

        # sub.slug is different so won't raise exception
        post2 = BlogPost(title='test2', sub=SubDocument(year=2010, slug='another-slug'))
        post2.save()

        # Now there will be two docs with the same sub.slug
        post3 = BlogPost(title='test3', sub=SubDocument(year=2010, slug='test'))
        self.assertRaises(OperationError, post3.save)

        # Now there will be two docs with the same title and year
        post3 = BlogPost(title='test1', sub=SubDocument(year=2009, slug='test-1'))
        self.assertRaises(OperationError, post3.save)

        BlogPost.drop_collection()

    def test_unique_and_indexes(self):
        """Ensure that 'unique' constraints aren't overridden by
        meta.indexes.
        """
        class Customer(Document):
            cust_id = IntField(unique=True, required=True)
            meta = {
                'indexes': ['cust_id'],
                'allow_inheritance': False,
            }

        Customer.drop_collection()
        cust = Customer(cust_id=1)
        cust.save()

        cust_dupe = Customer(cust_id=1)
        try:
            cust_dupe.save()
            raise AssertionError, "We saved a dupe!"
        except OperationError:
            pass
        Customer.drop_collection()

    def test_unique_and_primary(self):
        """If you set a field as primary, then unexpected behaviour can occur.
        You won't create a duplicate but you will update an existing document.
        """

        class User(Document):
            name = StringField(primary_key=True, unique=True)
            password = StringField()

        User.drop_collection()

        user = User(name='huangz', password='secret')
        user.save()

        user = User(name='huangz', password='secret2')
        user.save()

        self.assertEqual(User.objects.count(), 1)
        self.assertEqual(User.objects.get().password, 'secret2')

        User.drop_collection()

    def test_custom_id_field(self):
        """Ensure that documents may be created with custom primary keys.
        """
        class User(Document):
            username = StringField(primary_key=True)
            name = StringField()

        User.drop_collection()

        self.assertEqual(User._fields['username'].db_field, '_id')
        self.assertEqual(User._meta['id_field'], 'username')

        def create_invalid_user():
            User(name='test').save() # no primary key field
        self.assertRaises(ValidationError, create_invalid_user)

        def define_invalid_user():
            class EmailUser(User):
                email = StringField(primary_key=True)
        self.assertRaises(ValueError, define_invalid_user)

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

        User.drop_collection()


    def test_document_not_registered(self):

        class Place(Document):
            name = StringField()

        class NicePlace(Place):
            pass

        Place.drop_collection()

        Place(name="London").save()
        NicePlace(name="Buckingham Palace").save()

        # Mimic Place and NicePlace definitions being in a different file
        # and the NicePlace model not being imported in at query time.
        @classmethod
        def _get_subclasses(cls):
            return {}
        Place._get_subclasses = _get_subclasses

        def query_without_importing_nice_place():
            print Place.objects.all()
        self.assertRaises(NotRegistered, query_without_importing_nice_place)


    def test_creation(self):
        """Ensure that document may be created using keyword arguments.
        """
        person = self.Person(name="Test User", age=30)
        self.assertEqual(person.name, "Test User")
        self.assertEqual(person.age, 30)

    def test_to_dbref(self):
        """Ensure that you can get a dbref of a document"""
        person = self.Person(name="Test User", age=30)
        self.assertRaises(OperationError, person.to_dbref)
        person.save()

        person.to_dbref()

    def test_reload(self):
        """Ensure that attributes may be reloaded.
        """
        person = self.Person(name="Test User", age=20)
        person.save()

        person_obj = self.Person.objects.first()
        person_obj.name = "Mr Test User"
        person_obj.age = 21
        person_obj.save()

        self.assertEqual(person.name, "Test User")
        self.assertEqual(person.age, 20)

        person.reload()
        self.assertEqual(person.name, "Mr Test User")
        self.assertEqual(person.age, 21)

    def test_reload_referencing(self):
        """Ensures reloading updates weakrefs correctly
        """
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

        doc.reload()
        doc.list_field.append(1)
        doc.dict_field['woot'] = "woot"
        doc.embedded_field.list_field.append(1)
        doc.embedded_field.dict_field['woot'] = "woot"

        self.assertEquals(doc._get_changed_fields(), [
            'list_field', 'dict_field', 'embedded_field.list_field',
            'embedded_field.dict_field'])
        doc.save()

        doc.reload()
        self.assertEquals(doc._get_changed_fields(), [])
        self.assertEquals(len(doc.list_field), 4)
        self.assertEquals(len(doc.dict_field), 2)
        self.assertEquals(len(doc.embedded_field.list_field), 4)
        self.assertEquals(len(doc.embedded_field.dict_field), 2)

    def test_dictionary_access(self):
        """Ensure that dictionary-style field access works properly.
        """
        person = self.Person(name='Test User', age=30)
        self.assertEquals(person['name'], 'Test User')

        self.assertRaises(KeyError, person.__getitem__, 'salary')
        self.assertRaises(KeyError, person.__setitem__, 'salary', 50)

        person['name'] = 'Another User'
        self.assertEquals(person['name'], 'Another User')

        # Length = length(assigned fields + id)
        self.assertEquals(len(person), 3)

        self.assertTrue('age' in person)
        person.age = None
        self.assertFalse('age' in person)
        self.assertFalse('nationality' in person)

    def test_embedded_document(self):
        """Ensure that embedded documents are set up correctly.
        """
        class Comment(EmbeddedDocument):
            content = StringField()

        self.assertTrue('content' in Comment._fields)
        self.assertFalse('id' in Comment._fields)
        self.assertFalse('collection' in Comment._meta)

    def test_embedded_document_validation(self):
        """Ensure that embedded documents may be validated.
        """
        class Comment(EmbeddedDocument):
            date = DateTimeField()
            content = StringField(required=True)

        comment = Comment()
        self.assertRaises(ValidationError, comment.validate)

        comment.content = 'test'
        comment.validate()

        comment.date = 4
        self.assertRaises(ValidationError, comment.validate)

        comment.date = datetime.now()
        comment.validate()

    def test_save(self):
        """Ensure that a document may be saved in the database.
        """
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

        recipient = Recipient(email='root@localhost')
        self.assertRaises(ValidationError, recipient.save)
        try:
            recipient.save(validate=False)
        except ValidationError:
            self.fail()

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
        self.assertEquals(user.thing.count, 0)

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
        p.save()

        p1.reload()
        self.assertEquals(p1.name, p.parent.name)

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
        self.assertEquals(p1.name, p.parent.name)

    def test_update(self):
        """Ensure that an existing document is updated instead of be overwritten.
        """
        # Create person object and save it to the database
        person = self.Person(name='Test User', age=30)
        person.save()

        # Create same person object, with same id, without age
        same_person = self.Person(name='Test')
        same_person.id = person.id
        same_person.save()

        # Confirm only one object
        self.assertEquals(self.Person.objects.count(), 1)

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

    def test_document_update(self):

        def update_not_saved_raises():
            person = self.Person(name='dcrosta')
            person.update(set__name='Dan Crosta')

        self.assertRaises(OperationError, update_not_saved_raises)

        author = self.Person(name='dcrosta')
        author.save()

        author.update(set__name='Dan Crosta')
        author.reload()

        p1 = self.Person.objects.first()
        self.assertEquals(p1.name, author.name)

        def update_no_value_raises():
            person = self.Person.objects.first()
            person.update()

        self.assertRaises(OperationError, update_no_value_raises)

    def test_embedded_update(self):
        """
        Test update on `EmbeddedDocumentField` fields
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
        site = Site.objects.first()
        site.page.log_message = "Error: Dummy message"
        site.save()

        site = Site.objects.first()
        self.assertEqual(site.page.log_message, "Error: Dummy message")

    def test_embedded_update_db_field(self):
        """
        Test update on `EmbeddedDocumentField` fields when db_field is other
        than default.
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

    def test_delta(self):

        class Doc(Document):
            string_field = StringField()
            int_field = IntField()
            dict_field = DictField()
            list_field = ListField()

        Doc.drop_collection()
        doc = Doc()
        doc.save()

        doc = Doc.objects.first()
        self.assertEquals(doc._get_changed_fields(), [])
        self.assertEquals(doc._delta(), ({}, {}))

        doc.string_field = 'hello'
        self.assertEquals(doc._get_changed_fields(), ['string_field'])
        self.assertEquals(doc._delta(), ({'string_field': 'hello'}, {}))

        doc._changed_fields = []
        doc.int_field = 1
        self.assertEquals(doc._get_changed_fields(), ['int_field'])
        self.assertEquals(doc._delta(), ({'int_field': 1}, {}))

        doc._changed_fields = []
        dict_value = {'hello': 'world', 'ping': 'pong'}
        doc.dict_field = dict_value
        self.assertEquals(doc._get_changed_fields(), ['dict_field'])
        self.assertEquals(doc._delta(), ({'dict_field': dict_value}, {}))

        doc._changed_fields = []
        list_value = ['1', 2, {'hello': 'world'}]
        doc.list_field = list_value
        self.assertEquals(doc._get_changed_fields(), ['list_field'])
        self.assertEquals(doc._delta(), ({'list_field': list_value}, {}))

        # Test unsetting
        doc._changed_fields = []
        doc.dict_field = {}
        self.assertEquals(doc._get_changed_fields(), ['dict_field'])
        self.assertEquals(doc._delta(), ({}, {'dict_field': 1}))

        doc._changed_fields = []
        doc.list_field = []
        self.assertEquals(doc._get_changed_fields(), ['list_field'])
        self.assertEquals(doc._delta(), ({}, {'list_field': 1}))

    def test_delta_recursive(self):

        class Embedded(EmbeddedDocument):
            string_field = StringField()
            int_field = IntField()
            dict_field = DictField()
            list_field = ListField()

        class Doc(Document):
            string_field = StringField()
            int_field = IntField()
            dict_field = DictField()
            list_field = ListField()
            embedded_field = EmbeddedDocumentField(Embedded)

        Doc.drop_collection()
        doc = Doc()
        doc.save()

        doc = Doc.objects.first()
        self.assertEquals(doc._get_changed_fields(), [])
        self.assertEquals(doc._delta(), ({}, {}))

        embedded_1 = Embedded()
        embedded_1.string_field = 'hello'
        embedded_1.int_field = 1
        embedded_1.dict_field = {'hello': 'world'}
        embedded_1.list_field = ['1', 2, {'hello': 'world'}]
        doc.embedded_field = embedded_1

        self.assertEquals(doc._get_changed_fields(), ['embedded_field'])

        embedded_delta = {
            '_types': ['Embedded'],
            '_cls': 'Embedded',
            'string_field': 'hello',
            'int_field': 1,
            'dict_field': {'hello': 'world'},
            'list_field': ['1', 2, {'hello': 'world'}]
        }
        self.assertEquals(doc.embedded_field._delta(), (embedded_delta, {}))
        self.assertEquals(doc._delta(), ({'embedded_field': embedded_delta}, {}))

        doc.save()
        doc.reload()

        doc.embedded_field.dict_field = {}
        self.assertEquals(doc._get_changed_fields(), ['embedded_field.dict_field'])
        self.assertEquals(doc.embedded_field._delta(), ({}, {'dict_field': 1}))
        self.assertEquals(doc._delta(), ({}, {'embedded_field.dict_field': 1}))
        doc.save()
        doc.reload()
        self.assertEquals(doc.embedded_field.dict_field, {})

        doc.embedded_field.list_field = []
        self.assertEquals(doc._get_changed_fields(), ['embedded_field.list_field'])
        self.assertEquals(doc.embedded_field._delta(), ({}, {'list_field': 1}))
        self.assertEquals(doc._delta(), ({}, {'embedded_field.list_field': 1}))
        doc.save()
        doc.reload()
        self.assertEquals(doc.embedded_field.list_field, [])

        embedded_2 = Embedded()
        embedded_2.string_field = 'hello'
        embedded_2.int_field = 1
        embedded_2.dict_field = {'hello': 'world'}
        embedded_2.list_field = ['1', 2, {'hello': 'world'}]

        doc.embedded_field.list_field = ['1', 2, embedded_2]
        self.assertEquals(doc._get_changed_fields(), ['embedded_field.list_field'])
        self.assertEquals(doc.embedded_field._delta(), ({
            'list_field': ['1', 2, {
                '_cls': 'Embedded',
                '_types': ['Embedded'],
                'string_field': 'hello',
                'dict_field': {'hello': 'world'},
                'int_field': 1,
                'list_field': ['1', 2, {'hello': 'world'}],
            }]
        }, {}))

        self.assertEquals(doc._delta(), ({
            'embedded_field.list_field': ['1', 2, {
                '_cls': 'Embedded',
                 '_types': ['Embedded'],
                 'string_field': 'hello',
                 'dict_field': {'hello': 'world'},
                 'int_field': 1,
                 'list_field': ['1', 2, {'hello': 'world'}],
            }]
        }, {}))
        doc.save()
        doc.reload()

        self.assertEquals(doc.embedded_field.list_field[0], '1')
        self.assertEquals(doc.embedded_field.list_field[1], 2)
        for k in doc.embedded_field.list_field[2]._fields:
            self.assertEquals(doc.embedded_field.list_field[2][k], embedded_2[k])

        doc.embedded_field.list_field[2].string_field = 'world'
        self.assertEquals(doc._get_changed_fields(), ['embedded_field.list_field.2.string_field'])
        self.assertEquals(doc.embedded_field._delta(), ({'list_field.2.string_field': 'world'}, {}))
        self.assertEquals(doc._delta(), ({'embedded_field.list_field.2.string_field': 'world'}, {}))
        doc.save()
        doc.reload()
        self.assertEquals(doc.embedded_field.list_field[2].string_field, 'world')

        # Test multiple assignments
        doc.embedded_field.list_field[2].string_field = 'hello world'
        doc.embedded_field.list_field[2] = doc.embedded_field.list_field[2]
        self.assertEquals(doc._get_changed_fields(), ['embedded_field.list_field'])
        self.assertEquals(doc.embedded_field._delta(), ({
            'list_field': ['1', 2, {
            '_types': ['Embedded'],
            '_cls': 'Embedded',
            'string_field': 'hello world',
            'int_field': 1,
            'list_field': ['1', 2, {'hello': 'world'}],
            'dict_field': {'hello': 'world'}}]}, {}))
        self.assertEquals(doc._delta(), ({
            'embedded_field.list_field': ['1', 2, {
                '_types': ['Embedded'],
                '_cls': 'Embedded',
                'string_field': 'hello world',
                'int_field': 1,
                'list_field': ['1', 2, {'hello': 'world'}],
                'dict_field': {'hello': 'world'}}
            ]}, {}))
        doc.save()
        doc.reload()
        self.assertEquals(doc.embedded_field.list_field[2].string_field, 'hello world')

        # Test list native methods
        doc.embedded_field.list_field[2].list_field.pop(0)
        self.assertEquals(doc._delta(), ({'embedded_field.list_field.2.list_field': [2, {'hello': 'world'}]}, {}))
        doc.save()
        doc.reload()

        doc.embedded_field.list_field[2].list_field.append(1)
        self.assertEquals(doc._delta(), ({'embedded_field.list_field.2.list_field': [2, {'hello': 'world'}, 1]}, {}))
        doc.save()
        doc.reload()
        self.assertEquals(doc.embedded_field.list_field[2].list_field, [2, {'hello': 'world'}, 1])

        doc.embedded_field.list_field[2].list_field.sort()
        doc.save()
        doc.reload()
        self.assertEquals(doc.embedded_field.list_field[2].list_field, [1, 2, {'hello': 'world'}])

        del(doc.embedded_field.list_field[2].list_field[2]['hello'])
        self.assertEquals(doc._delta(), ({'embedded_field.list_field.2.list_field': [1, 2, {}]}, {}))
        doc.save()
        doc.reload()

        del(doc.embedded_field.list_field[2].list_field)
        self.assertEquals(doc._delta(), ({}, {'embedded_field.list_field.2.list_field': 1}))

        doc.save()
        doc.reload()

        doc.dict_field['Embedded'] = embedded_1
        doc.save()
        doc.reload()

        doc.dict_field['Embedded'].string_field = 'Hello World'
        self.assertEquals(doc._get_changed_fields(), ['dict_field.Embedded.string_field'])
        self.assertEquals(doc._delta(), ({'dict_field.Embedded.string_field': 'Hello World'}, {}))


    def test_delta_db_field(self):

        class Doc(Document):
            string_field = StringField(db_field='db_string_field')
            int_field = IntField(db_field='db_int_field')
            dict_field = DictField(db_field='db_dict_field')
            list_field = ListField(db_field='db_list_field')

        Doc.drop_collection()
        doc = Doc()
        doc.save()

        doc = Doc.objects.first()
        self.assertEquals(doc._get_changed_fields(), [])
        self.assertEquals(doc._delta(), ({}, {}))

        doc.string_field = 'hello'
        self.assertEquals(doc._get_changed_fields(), ['db_string_field'])
        self.assertEquals(doc._delta(), ({'db_string_field': 'hello'}, {}))

        doc._changed_fields = []
        doc.int_field = 1
        self.assertEquals(doc._get_changed_fields(), ['db_int_field'])
        self.assertEquals(doc._delta(), ({'db_int_field': 1}, {}))

        doc._changed_fields = []
        dict_value = {'hello': 'world', 'ping': 'pong'}
        doc.dict_field = dict_value
        self.assertEquals(doc._get_changed_fields(), ['db_dict_field'])
        self.assertEquals(doc._delta(), ({'db_dict_field': dict_value}, {}))

        doc._changed_fields = []
        list_value = ['1', 2, {'hello': 'world'}]
        doc.list_field = list_value
        self.assertEquals(doc._get_changed_fields(), ['db_list_field'])
        self.assertEquals(doc._delta(), ({'db_list_field': list_value}, {}))

        # Test unsetting
        doc._changed_fields = []
        doc.dict_field = {}
        self.assertEquals(doc._get_changed_fields(), ['db_dict_field'])
        self.assertEquals(doc._delta(), ({}, {'db_dict_field': 1}))

        doc._changed_fields = []
        doc.list_field = []
        self.assertEquals(doc._get_changed_fields(), ['db_list_field'])
        self.assertEquals(doc._delta(), ({}, {'db_list_field': 1}))

        # Test it saves that data
        doc = Doc()
        doc.save()

        doc.string_field = 'hello'
        doc.int_field = 1
        doc.dict_field = {'hello': 'world'}
        doc.list_field = ['1', 2, {'hello': 'world'}]
        doc.save()
        doc.reload()

        self.assertEquals(doc.string_field, 'hello')
        self.assertEquals(doc.int_field, 1)
        self.assertEquals(doc.dict_field, {'hello': 'world'})
        self.assertEquals(doc.list_field, ['1', 2, {'hello': 'world'}])

    def test_delta_recursive_db_field(self):

        class Embedded(EmbeddedDocument):
            string_field = StringField(db_field='db_string_field')
            int_field = IntField(db_field='db_int_field')
            dict_field = DictField(db_field='db_dict_field')
            list_field = ListField(db_field='db_list_field')

        class Doc(Document):
            string_field = StringField(db_field='db_string_field')
            int_field = IntField(db_field='db_int_field')
            dict_field = DictField(db_field='db_dict_field')
            list_field = ListField(db_field='db_list_field')
            embedded_field = EmbeddedDocumentField(Embedded, db_field='db_embedded_field')

        Doc.drop_collection()
        doc = Doc()
        doc.save()

        doc = Doc.objects.first()
        self.assertEquals(doc._get_changed_fields(), [])
        self.assertEquals(doc._delta(), ({}, {}))

        embedded_1 = Embedded()
        embedded_1.string_field = 'hello'
        embedded_1.int_field = 1
        embedded_1.dict_field = {'hello': 'world'}
        embedded_1.list_field = ['1', 2, {'hello': 'world'}]
        doc.embedded_field = embedded_1

        self.assertEquals(doc._get_changed_fields(), ['db_embedded_field'])

        embedded_delta = {
            '_types': ['Embedded'],
            '_cls': 'Embedded',
            'db_string_field': 'hello',
            'db_int_field': 1,
            'db_dict_field': {'hello': 'world'},
            'db_list_field': ['1', 2, {'hello': 'world'}]
        }
        self.assertEquals(doc.embedded_field._delta(), (embedded_delta, {}))
        self.assertEquals(doc._delta(), ({'db_embedded_field': embedded_delta}, {}))

        doc.save()
        doc.reload()

        doc.embedded_field.dict_field = {}
        self.assertEquals(doc._get_changed_fields(), ['db_embedded_field.db_dict_field'])
        self.assertEquals(doc.embedded_field._delta(), ({}, {'db_dict_field': 1}))
        self.assertEquals(doc._delta(), ({}, {'db_embedded_field.db_dict_field': 1}))
        doc.save()
        doc.reload()
        self.assertEquals(doc.embedded_field.dict_field, {})

        doc.embedded_field.list_field = []
        self.assertEquals(doc._get_changed_fields(), ['db_embedded_field.db_list_field'])
        self.assertEquals(doc.embedded_field._delta(), ({}, {'db_list_field': 1}))
        self.assertEquals(doc._delta(), ({}, {'db_embedded_field.db_list_field': 1}))
        doc.save()
        doc.reload()
        self.assertEquals(doc.embedded_field.list_field, [])

        embedded_2 = Embedded()
        embedded_2.string_field = 'hello'
        embedded_2.int_field = 1
        embedded_2.dict_field = {'hello': 'world'}
        embedded_2.list_field = ['1', 2, {'hello': 'world'}]

        doc.embedded_field.list_field = ['1', 2, embedded_2]
        self.assertEquals(doc._get_changed_fields(), ['db_embedded_field.db_list_field'])
        self.assertEquals(doc.embedded_field._delta(), ({
            'db_list_field': ['1', 2, {
                '_cls': 'Embedded',
                '_types': ['Embedded'],
                'db_string_field': 'hello',
                'db_dict_field': {'hello': 'world'},
                'db_int_field': 1,
                'db_list_field': ['1', 2, {'hello': 'world'}],
            }]
        }, {}))

        self.assertEquals(doc._delta(), ({
            'db_embedded_field.db_list_field': ['1', 2, {
                '_cls': 'Embedded',
                 '_types': ['Embedded'],
                 'db_string_field': 'hello',
                 'db_dict_field': {'hello': 'world'},
                 'db_int_field': 1,
                 'db_list_field': ['1', 2, {'hello': 'world'}],
            }]
        }, {}))
        doc.save()
        doc.reload()

        self.assertEquals(doc.embedded_field.list_field[0], '1')
        self.assertEquals(doc.embedded_field.list_field[1], 2)
        for k in doc.embedded_field.list_field[2]._fields:
            self.assertEquals(doc.embedded_field.list_field[2][k], embedded_2[k])

        doc.embedded_field.list_field[2].string_field = 'world'
        self.assertEquals(doc._get_changed_fields(), ['db_embedded_field.db_list_field.2.db_string_field'])
        self.assertEquals(doc.embedded_field._delta(), ({'db_list_field.2.db_string_field': 'world'}, {}))
        self.assertEquals(doc._delta(), ({'db_embedded_field.db_list_field.2.db_string_field': 'world'}, {}))
        doc.save()
        doc.reload()
        self.assertEquals(doc.embedded_field.list_field[2].string_field, 'world')

        # Test multiple assignments
        doc.embedded_field.list_field[2].string_field = 'hello world'
        doc.embedded_field.list_field[2] = doc.embedded_field.list_field[2]
        self.assertEquals(doc._get_changed_fields(), ['db_embedded_field.db_list_field'])
        self.assertEquals(doc.embedded_field._delta(), ({
            'db_list_field': ['1', 2, {
            '_types': ['Embedded'],
            '_cls': 'Embedded',
            'db_string_field': 'hello world',
            'db_int_field': 1,
            'db_list_field': ['1', 2, {'hello': 'world'}],
            'db_dict_field': {'hello': 'world'}}]}, {}))
        self.assertEquals(doc._delta(), ({
            'db_embedded_field.db_list_field': ['1', 2, {
                '_types': ['Embedded'],
                '_cls': 'Embedded',
                'db_string_field': 'hello world',
                'db_int_field': 1,
                'db_list_field': ['1', 2, {'hello': 'world'}],
                'db_dict_field': {'hello': 'world'}}
            ]}, {}))
        doc.save()
        doc.reload()
        self.assertEquals(doc.embedded_field.list_field[2].string_field, 'hello world')

        # Test list native methods
        doc.embedded_field.list_field[2].list_field.pop(0)
        self.assertEquals(doc._delta(), ({'db_embedded_field.db_list_field.2.db_list_field': [2, {'hello': 'world'}]}, {}))
        doc.save()
        doc.reload()

        doc.embedded_field.list_field[2].list_field.append(1)
        self.assertEquals(doc._delta(), ({'db_embedded_field.db_list_field.2.db_list_field': [2, {'hello': 'world'}, 1]}, {}))
        doc.save()
        doc.reload()
        self.assertEquals(doc.embedded_field.list_field[2].list_field, [2, {'hello': 'world'}, 1])

        doc.embedded_field.list_field[2].list_field.sort()
        doc.save()
        doc.reload()
        self.assertEquals(doc.embedded_field.list_field[2].list_field, [1, 2, {'hello': 'world'}])

        del(doc.embedded_field.list_field[2].list_field[2]['hello'])
        self.assertEquals(doc._delta(), ({'db_embedded_field.db_list_field.2.db_list_field': [1, 2, {}]}, {}))
        doc.save()
        doc.reload()

        del(doc.embedded_field.list_field[2].list_field)
        self.assertEquals(doc._delta(), ({}, {'db_embedded_field.db_list_field.2.db_list_field': 1}))

    def test_save_only_changed_fields(self):
        """Ensure save only sets / unsets changed fields
        """

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
        self.assertEquals(person.name, 'User')
        self.assertEquals(person.age, 21)
        self.assertEquals(person.active, False)

    def test_save_only_changed_fields_recursive(self):
        """Ensure save only sets / unsets changed fields
        """

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
        """Ensure that document may be deleted using the delete method.
        """
        person = self.Person(name="Test User", age=30)
        person.save()
        self.assertEqual(len(self.Person.objects), 1)
        person.delete()
        self.assertEqual(len(self.Person.objects), 0)

    def test_save_custom_id(self):
        """Ensure that a document may be saved with a custom _id.
        """
        # Create person object and save it to the database
        person = self.Person(name='Test User', age=30,
                             id='497ce96f395f2f052a494fd4')
        person.save()
        # Ensure that the object is in the database with the correct _id
        collection = self.db[self.Person._get_collection_name()]
        person_obj = collection.find_one({'name': 'Test User'})
        self.assertEqual(str(person_obj['_id']), '497ce96f395f2f052a494fd4')

    def test_save_custom_pk(self):
        """Ensure that a document may be saved with a custom _id using pk alias.
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
        """Ensure that a list field may be properly saved.
        """
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

        BlogPost.drop_collection()

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

        p1 = Page(comments = [Comment(user=u1, comment="Its very good"),
                              Comment(user=u2, comment="Hello world"),
                              Comment(user=u3, comment="Ping Pong"),
                              Comment(user=u1, comment="I like a beer")])
        p1.save()

        p2 = Page(comments = [Comment(user=u1, comment="Its very good"),
                              Comment(user=u2, comment="Hello world")])
        p2.save()

        p3 = Page(comments = [Comment(user=u3, comment="Its very good")])
        p3.save()

        p4 = Page(comments = [Comment(user=u2, comment="Heavy Metal song")])
        p4.save()

        self.assertEqual([p1, p2], list(Page.objects.filter(comments__user=u1)))
        self.assertEqual([p1, p2, p4], list(Page.objects.filter(comments__user=u2)))
        self.assertEqual([p1, p3], list(Page.objects.filter(comments__user=u3)))

    def test_save_embedded_document(self):
        """Ensure that a document with an embedded document field may be
        saved in the database.
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

    def test_updating_an_embedded_document(self):
        """Ensure that a document with an embedded document field may be
        saved in the database.
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
        self.assertEqual(promoted_employee.details.position, 'Senior Developer')

        # Test removal
        promoted_employee.details = None
        promoted_employee.save()

        promoted_employee.reload()
        self.assertEqual(promoted_employee.details, None)

    def test_mixins_dont_add_to_types(self):

        class Bob(Document): name = StringField()

        Bob.drop_collection()

        p = Bob(name="Rozza")
        p.save()
        Bob.drop_collection()

        class Person(Document, Mixin):
            pass

        Person.drop_collection()

        p = Person(name="Rozza")
        p.save()
        self.assertEquals(p._fields.keys(), ['name', 'id'])

        collection = self.db[Person._get_collection_name()]
        obj = collection.find_one()
        self.assertEquals(obj['_cls'], 'Person')
        self.assertEquals(obj['_types'], ['Person'])



        self.assertEquals(Person.objects.count(), 1)
        rozza = Person.objects.get(name="Rozza")

        Person.drop_collection()

    def test_save_reference(self):
        """Ensure that a document reference field may be saved in the database.
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
                                   pymongo.dbref.DBRef))
        self.assertTrue(isinstance(post_obj.author, self.Person))
        self.assertEqual(post_obj.author.name, 'Test User')

        # Ensure that the dereferenced object may be changed and saved
        post_obj.author.age = 25
        post_obj.author.save()

        author = list(self.Person.objects(name='Test User'))[-1]
        self.assertEqual(author.age, 25)

        BlogPost.drop_collection()


    def test_reverse_delete_rule_cascade_and_nullify(self):
        """Ensure that a referenced document is also deleted upon deletion.
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

        post = BlogPost(content = 'Watched some TV')
        post.author = author
        post.reviewer = reviewer
        post.save()

        reviewer.delete()
        self.assertEqual(len(BlogPost.objects), 1)  # No effect on the BlogPost
        self.assertEqual(BlogPost.objects.get().reviewer, None)

        # Delete the Person, which should lead to deletion of the BlogPost, too
        author.delete()
        self.assertEqual(len(BlogPost.objects), 0)

    def test_reverse_delete_rule_cascade_recurs(self):
        """Ensure that a chain of documents is also deleted upon cascaded
        deletion.
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

        post = BlogPost(content = 'Watched some TV')
        post.author = author
        post.save()

        comment = Comment(text = 'Kudos.')
        comment.post = post
        comment.save()

        # Delete the Person, which should lead to deletion of the BlogPost, and,
        # recursively to the Comment, too
        author.delete()
        self.assertEqual(len(Comment.objects), 0)

        self.Person.drop_collection()
        BlogPost.drop_collection()
        Comment.drop_collection()

    def test_reverse_delete_rule_deny(self):
        """Ensure that a document cannot be referenced if there are still
        documents referring to it.
        """

        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(self.Person, reverse_delete_rule=DENY)

        self.Person.drop_collection()
        BlogPost.drop_collection()

        author = self.Person(name='Test User')
        author.save()

        post = BlogPost(content = 'Watched some TV')
        post.author = author
        post.save()

        # Delete the Person should be denied
        self.assertRaises(OperationError, author.delete)  # Should raise denied error
        self.assertEqual(len(BlogPost.objects), 1)  # No objects may have been deleted
        self.assertEqual(len(self.Person.objects), 1)

        # Other users, that don't have BlogPosts must be removable, like normal
        author = self.Person(name='Another User')
        author.save()

        self.assertEqual(len(self.Person.objects), 2)
        author.delete()
        self.assertEqual(len(self.Person.objects), 1)

        self.Person.drop_collection()
        BlogPost.drop_collection()

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

        self.assertEquals(A.objects.count(), 2)
        self.assertEquals(B.objects.count(), 1)
        A.drop_collection()
        B.drop_collection()

    def test_document_hash(self):
        """Test document in list, dict, set
        """
        class User(Document):
            pass

        class BlogPost(Document):
            pass

        # Clear old datas
        User.drop_collection()
        BlogPost.drop_collection()

        u1 = User.objects.create()
        u2 = User.objects.create()
        u3 = User.objects.create()
        u4 = User() # New object

        b1 = BlogPost.objects.create()
        b2 = BlogPost.objects.create()

        # in List
        all_user_list = list(User.objects.all())

        self.assertTrue(u1 in all_user_list)
        self.assertTrue(u2 in all_user_list)
        self.assertTrue(u3 in all_user_list)
        self.assertFalse(u4 in all_user_list) # New object
        self.assertFalse(b1 in all_user_list) # Other object
        self.assertFalse(b2 in all_user_list) # Other object

        # in Dict
        all_user_dic = {}
        for u in User.objects.all():
            all_user_dic[u] = "OK"

        self.assertEqual(all_user_dic.get(u1, False), "OK" )
        self.assertEqual(all_user_dic.get(u2, False), "OK" )
        self.assertEqual(all_user_dic.get(u3, False), "OK" )
        self.assertEqual(all_user_dic.get(u4, False), False ) # New object
        self.assertEqual(all_user_dic.get(b1, False), False ) # Other object
        self.assertEqual(all_user_dic.get(b2, False), False ) # Other object

        # in Set
        all_user_set = set(User.objects.all())

        self.assertTrue(u1 in all_user_set )

    def test_picklable(self):

        pickle_doc = PickleTest(number=1, string="One", lists=['1', '2'])
        pickle_doc.embedded = PickleEmbedded()
        pickle_doc.save()

        pickled_doc = pickle.dumps(pickle_doc)
        resurrected = pickle.loads(pickled_doc)

        self.assertEquals(resurrected, pickle_doc)

        resurrected.string = "Two"
        resurrected.save()

        pickle_doc.reload()
        self.assertEquals(resurrected, pickle_doc)


if __name__ == '__main__':
    unittest.main()
