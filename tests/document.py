import pymongo
import bson
import unittest
import mock
import cPickle

from datetime import datetime

from mongoengine import *
from mongoengine.base import _document_registry
from mongoengine.connection import _get_db, connect
import mongoengine.connection
from mock import MagicMock, Mock, call

mongoengine.connection.set_default_db("test")


# has to be top level for pickling
class Citizen(Document):
    age = mongoengine.fields.IntField()


class DocumentTest(unittest.TestCase):

    def setUp(self):
        connect()
        self.db = _get_db()

        class Person(Document):
            name = StringField()
            age = IntField()
            uid = ObjectIdField()
        self.Person = Person

    def tearDown(self):
        self.Person.drop_collection()
        _document_registry.clear()

        Citizen.drop_collection()

    def test_bool(self):
        class EmptyDoc(EmbeddedDocument):
            pass

        empty_doc = EmptyDoc()
        self.assertTrue(bool(empty_doc))

        nonempty_doc = self.Person(name='Adam')
        self.assertTrue(bool(nonempty_doc))

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

        _document_registry.clear()
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
        class Base(Document): pass
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

        self.assertEquals(Human.count({}), 1)
        self.assertEquals(Mammal.count({}), 1)
        self.assertEquals(Animal.count({}), 1)
        self.assertEquals(Base.count({}), 1)
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

        self.assertEqual(list_stats, CompareStats.find_one({}).stats)

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


    def test_collection_name(self):
        """Ensure that a collection with a specified name may be used.
        """
        collection = 'personCollTest'
        if collection in self.db.collection_names():
            self.db.drop_collection(collection)

        _document_registry.clear()
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

        _document_registry.clear()
        class Person(Document):
            name = StringField(primary_key=True)
            meta = {'collection': 'app'}

        user = Person(name="Test User")
        user.save()

        user_obj = Person.objects[0]
        self.assertEqual(user_obj.name, "Test User")

        Person.drop_collection()

    def test_creation(self):
        """Ensure that document may be created using keyword arguments.
        """
        person = self.Person(name="Test User", age=30)
        self.assertEqual(person.name, "Test User")
        self.assertEqual(person.age, 30)

    def test_reload(self):
        """Ensure that attributes may be reloaded.
        """
        person = self.Person(name="Test User", age=20)
        person.save()

        person_obj = self.Person.find_one({})
        person_obj.name = "Mr Test User"
        person_obj.age = 21
        person_obj.save()

        self.assertEqual(person.name, "Test User")
        self.assertEqual(person.age, 20)

        person.reload()
        self.assertEqual(person.name, "Mr Test User")
        self.assertEqual(person.age, 21)

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
        self.assertEquals(len(person), 4)

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

        _document_registry.clear()
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
        p0 = Person.find_one({})
        p0.name = 'wpjunior'
        p0.save()

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
        self.assertEquals(self.Person.count({}), 1)

        # reload
        person.reload()
        same_person.reload()

        # Confirm the same
        self.assertEqual(person, same_person)
        self.assertEqual(person.name, same_person.name)
        self.assertEqual(person.age, same_person.age)

        # Confirm the saved values
        self.assertEqual(person.name, 'Test')
        self.assertIsNone(person.age)

    def test_document_update(self):

        person = self.Person(name='dcrosta',
                id=bson.ObjectId(), uid=bson.ObjectId())
        resp = person.set(name='Dan Crosta')

        self.assertEquals(resp['n'], 0)

        author = self.Person(name='dcrosta')
        author.save()

        author.set(name='Dan Crosta')
        author.reload()

        p1 = self.Person.find_one({})
        self.assertEquals(p1.name, author.name)

        p1.set(uid=None)
        p1.reload()
        self.assertEquals(p1.uid, None)

        def unset_primary_key():
            person = self.Person.find_one({})
            person.set(id=None)

        def update_no_value_raises():
            person = self.Person.find_one({})
            person.set()

        self.assertRaises(pymongo.errors.OperationFailure, unset_primary_key)
        self.assertRaises(pymongo.errors.OperationFailure, update_no_value_raises)

    def test_embedded_update(self):
        """
        Test update on `EmbeddedDocumentField` fields
        """

        class Page(EmbeddedDocument):
            log_message = StringField(required=True)

        class Site(Document):
            page = EmbeddedDocumentField(Page)


        Site.drop_collection()
        site = Site(page=Page(log_message="Warning: Dummy message"))
        site.save()

        # Update
        site = Site.find_one({})
        site.page.log_message = "Error: Dummy message"
        site.save()

        site = Site.find_one({})
        self.assertEqual(site.page.log_message, "Error: Dummy message")

    def test_embedded_update_db_field(self):
        """
        Test update on `EmbeddedDocumentField` fields when db_field is other
        than default.
        """

        class Page(EmbeddedDocument):
            log_message = StringField(db_field="page_log_message",
                                      required=True)

        class Site(Document):
            page = EmbeddedDocumentField(Page)


        Site.drop_collection()

        site = Site(page=Page(log_message="Warning: Dummy message"))
        site.save()

        # Update
        site = Site.find_one({})
        site.page.log_message = "Error: Dummy message"
        site.save()

        site = Site.find_one({})
        self.assertEqual(site.page.log_message, "Error: Dummy message")

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
            meta = {'allow_inheritance': False}

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

        post_obj = BlogPost.find_one({})

        # Test laziness
        self.assertTrue(isinstance(post_obj._lazy_data['author'],
                                   bson.dbref.DBRef))
        self.assertTrue(isinstance(post_obj.author, self.Person))
        self.assertEqual(post_obj.author.name, 'Test User')

        # Ensure that the dereferenced object may be changed and saved
        post_obj.author.age = 25
        post_obj.author.save()

        author = list(self.Person.objects(name='Test User'))[-1]
        self.assertEqual(author.age, 25)

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

        self.assertEquals(A.count({}), 2)
        self.assertEquals(B.count({}), 1)
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
        all_user_set = set(User.find({}))

        self.assertTrue(u1 in all_user_set)

    def throw_invalid_document_error(self):

        # test handles people trying to upsert
        def throw_invalid_document_error():
            class Blog(Document):
                validate = DictField()

        self.assertRaises(InvalidDocumentError, throw_invalid_document_error)

    def test_write_concern(self):
        class ImportantThing(Document):
            meta = {'write_concern': 2}
            name = StringField()

        class MajorityThing(Document):
            meta = {'write_concern': 'majority',
                    'force_insert': True}
            name = StringField()

        class NormalThing(Document):
            name = StringField()

        # test save() of ImportantThing gets w=2
        with mock.patch.object(ImportantThing._pymongo(), "save") as save_mock:
            it = ImportantThing(id=bson.ObjectId())
            save_mock.return_value = it.id
            it.save()

            save_mock.assert_called_with(it.to_mongo(), w=2)

        # test insert() of MajorityThing gets w=majority
        # note: uses insert() because force_insert is set
        with mock.patch.object(MajorityThing._pymongo(), "insert") as insert_mock:
            mt = MajorityThing(id=bson.ObjectId())
            insert_mock.return_value = mt.id
            mt.save()

            insert_mock.assert_called_with(mt.to_mongo(), w='majority')

        # test NormalThing gets default w=1
        with mock.patch.object(NormalThing._pymongo(), "save") as save_mock:
            nt = NormalThing(id=bson.ObjectId())
            save_mock.return_value = nt.id
            nt.save()

            save_mock.assert_called_with(nt.to_mongo(), w=1)

        # test ImportantThing update gets w=2
        with mock.patch.object(ImportantThing._pymongo(), "update") as update_mock:
            it.set(name="Adam")

            self.assertEquals(update_mock.call_count, 1)
            self.assertEquals(update_mock.call_args[1]['w'], 2)

        # test MajorityThing update gets w=majority
        with mock.patch.object(MajorityThing._pymongo(), "update") as update_mock:
            mt.set(name="Adam")

            self.assertEquals(update_mock.call_count, 1)
            self.assertEquals(update_mock.call_args[1]['w'], "majority")

        # test NormalThing update gets w=1
        with mock.patch.object(NormalThing._pymongo(), "update") as update_mock:
            nt.set(name="Adam")

            self.assertEquals(update_mock.call_count, 1)
            self.assertEquals(update_mock.call_args[1]['w'], 1)

    def test_by_id_key(self):
        class UnshardedCollection(Document):
            pass

        class IdShardedCollection(Document):
            meta = {'hash_field': 'id'}

        class NonIdShardedCollection(Document):
            meta = {'hash_field': 'name'}

            name = mongoengine.fields.StringField()

        doc_id = bson.ObjectId()

        # unsharded and non-ID sharded collections don't have anything injected
        self.assertEquals(UnshardedCollection._by_id_key(doc_id),
                          {'_id': doc_id})
        self.assertEquals(NonIdShardedCollection._by_id_key(doc_id),
                          {'_id': doc_id})

        # ID-sharded collections get the hash injected
        self.assertEquals(IdShardedCollection._by_id_key(doc_id),
                          {'_id': doc_id,
                           'shard_hash': IdShardedCollection._hash(doc_id)})

    def test_by_ids_key(self):
        class UnshardedCollection(Document):
            pass

        class IdShardedCollection(Document):
            meta = {'hash_field': 'id'}

        class NonIdShardedCollection(Document):
            meta = {'hash_field': 'name'}

            name = mongoengine.fields.StringField()

        doc_ids = [bson.ObjectId() for i in xrange(5)]

        # unsharded and non-ID sharded collections don't have anything injected
        self.assertEquals(UnshardedCollection._by_ids_key(doc_ids),
                          {'_id': {'$in': doc_ids}})
        self.assertEquals(NonIdShardedCollection._by_ids_key(doc_ids),
                          {'_id': {'$in': doc_ids}})

        # ID-sharded collections get the hash injected
        doc_hashes = [IdShardedCollection._hash(doc_id) for doc_id in doc_ids]
        self.assertEquals(IdShardedCollection._by_ids_key(doc_ids),
                          {'_id': {'$in': doc_ids},
                           'shard_hash': {'$in': doc_hashes}})

        # unsharded and non-ID sharded collections don't have anything injected
        self.assertEquals(UnshardedCollection._by_ids_key([]),
                          {'_id': {'$in': []}})
        self.assertEquals(NonIdShardedCollection._by_ids_key([]),
                          {'_id': {'$in': []}})

        # ID-sharded collections get the hash injected
        self.assertEquals(IdShardedCollection._by_ids_key([]),
                          {'_id': {'$in': []},
                           'shard_hash': {'$in': []}})

    def test_can_pickle(self):
        person = Citizen(age=20)
        person.save()

        pickled = cPickle.dumps(person)
        restored = cPickle.loads(pickled)

        self.assertEqual(person, restored)
        self.assertEqual(person.age, restored.age)

    @unittest.skip("disabled the feature for now")
    def test_find_raw_max_time_ms(self):
        cur = Citizen.find_raw({}, max_time_ms=None, limit=1)
        self.assertEquals(cur._Cursor__max_time_ms, Citizen.MAX_TIME_MS)
        cur = Citizen.find_raw({}, max_time_ms=0, limit=1)
        self.assertIsNone(cur._Cursor__max_time_ms)
        cur = Citizen.find_raw({}, max_time_ms=-1, limit=1)
        self.assertIsNone(cur._Cursor__max_time_ms)
        cur = Citizen.find_raw({}, max_time_ms=1000, limit=1)
        self.assertEquals(cur._Cursor__max_time_ms, 1000)

    def test_max_time_ms_find(self):
        col_mock = Mock()
        col_mock.name = 'asdf'
        doc_mock = MagicMock()
        doc_mock.__iter__.return_value = ['a','b']
        cur_mock = Mock()
        cur_mock.collection = col_mock
        cur_mock.next = MagicMock(side_effect=[doc_mock])
        find_raw = MagicMock(return_value=cur_mock)
        Citizen.find_raw = find_raw

        Citizen.find({}, max_time_ms=None)
        Citizen.find({}, max_time_ms=0)
        Citizen.find({}, max_time_ms=-1)
        Citizen.find({}, max_time_ms=1000)

        a,b,c,d = find_raw.call_args_list
        self.assertEquals(a[1]['max_time_ms'],None)
        self.assertEquals(b[1]['max_time_ms'],0)
        self.assertEquals(c[1]['max_time_ms'],-1)
        self.assertEquals(d[1]['max_time_ms'],1000)

    @unittest.skip("disabled the feature for now")
    def test_max_time_ms_find_iter(self):
        cur_mock = MagicMock()
        cur_mock._iterate_cursor = MagicMock(side_effect=['a'])
        find_raw = MagicMock(return_value=cur_mock)
        Citizen.find_raw = find_raw
        Citizen._from_augmented_son = MagicMock(return_value=None)

        Citizen.find_iter({}, max_time_ms=None).next()
        Citizen.find_iter({}, max_time_ms=0).next()
        Citizen.find_iter({}, max_time_ms=-1).next()
        Citizen.find_iter({}, max_time_ms=1000).next()

        a,b,c,d = find_raw.call_args_list

        self.assertEquals(a[1]['max_time_ms'],None)
        self.assertEquals(b[1]['max_time_ms'],0)
        self.assertEquals(c[1]['max_time_ms'],-1)
        self.assertEquals(d[1]['max_time_ms'],1000)

    def test_max_time_ms_find_one(self):
        find_raw = MagicMock(return_value=None)
        Citizen.find_raw = find_raw

        Citizen.find_one({}, max_time_ms=None)
        Citizen.find_one({}, max_time_ms=0)
        Citizen.find_one({}, max_time_ms=-1)
        Citizen.find_one({}, max_time_ms=1000)

        a,b,c,d = find_raw.call_args_list
        self.assertEquals(a[1]['max_time_ms'],None)
        self.assertEquals(b[1]['max_time_ms'],0)
        self.assertEquals(c[1]['max_time_ms'],-1)
        self.assertEquals(d[1]['max_time_ms'],1000)

    def test_max_time_ms_count(self):
        cur_mock = Mock()
        cur_mock.count = MagicMock(return_value=1)
        find_raw = Mock(return_value=cur_mock)
        Citizen.find_raw = find_raw

        Citizen.count({}, max_time_ms=None)
        Citizen.count({}, max_time_ms=0)
        Citizen.count({}, max_time_ms=-1)
        Citizen.count({}, max_time_ms=1000)

        a,b,c,d = find_raw.call_args_list
        self.assertEquals(a[1]['max_time_ms'],None)
        self.assertEquals(b[1]['max_time_ms'],0)
        self.assertEquals(c[1]['max_time_ms'],-1)
        self.assertEquals(d[1]['max_time_ms'],1000)

    def test_max_time_ms_distinct(self):
        cur_mock = Mock()
        cur_mock.count = MagicMock(return_value=1)
        find_raw = Mock(return_value=cur_mock)
        Citizen.find_raw = find_raw

        Citizen.distinct({}, '_id', max_time_ms=None)
        Citizen.distinct({}, '_id', max_time_ms=0)
        Citizen.distinct({}, '_id', max_time_ms=-1)
        Citizen.distinct({}, '_id', max_time_ms=1000)

        a,b,c,d = find_raw.call_args_list
        self.assertEquals(a[1]['max_time_ms'],None)
        self.assertEquals(b[1]['max_time_ms'],0)
        self.assertEquals(c[1]['max_time_ms'],-1)
        self.assertEquals(d[1]['max_time_ms'],1000)

if __name__ == '__main__':
    unittest.main()
