import unittest
import pymongo

from mongoengine import *
from mongoengine.connection import _get_db


class DocumentTest(unittest.TestCase):
    
    def setUp(self):
        connect(db='mongoenginetest')
        self.db = _get_db()

        class Person(Document):
            name = StringField()
            age = IntField()
        self.Person = Person

    def test_drop_collection(self):
        """Ensure that the collection may be dropped from the database.
        """
        self.Person(name='Test').save()

        collection = self.Person._meta['collection']
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
        self.assertTrue('_id' in Person._fields)
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

    def test_polymorphic_queries(self):
        """Ensure that the correct subclasses are returned from a query"""
        class Animal(Document): pass
        class Fish(Animal): pass
        class Mammal(Animal): pass
        class Human(Mammal): pass
        class Dog(Mammal): pass

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

    def test_inheritance(self):
        """Ensure that document may inherit fields from a superclass document.
        """
        class Employee(self.Person):
            salary = IntField()

        self.assertTrue('name' in Employee._fields)
        self.assertTrue('salary' in Employee._fields)
        self.assertEqual(Employee._meta['collection'], 
                         self.Person._meta['collection'])

    def test_creation(self):
        """Ensure that document may be created using keyword arguments.
        """
        person = self.Person(name="Test User", age=30)
        self.assertEqual(person.name, "Test User")
        self.assertEqual(person.age, 30)

    def test_dictionary_access(self):
        """Ensure that dictionary-style field access works properly.
        """
        person = self.Person(name='Test User', age=30)
        self.assertEquals(person['name'], 'Test User')

        self.assertRaises(KeyError, person.__getitem__, 'salary')
        self.assertRaises(KeyError, person.__setitem__, 'salary', 50)

        person['name'] = 'Another User'
        self.assertEquals(person['name'], 'Another User')

        # Length = length(assigned fields + _id)
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
        self.assertFalse('_id' in Comment._fields)
        self.assertFalse(hasattr(Comment, '_meta'))

    def test_save(self):
        """Ensure that a document may be saved in the database.
        """
        # Create person object and save it to the database
        person = self.Person(name='Test User', age=30)
        person.save()
        # Ensure that the object is in the database
        collection = self.db[self.Person._meta['collection']]
        person_obj = collection.find_one({'name': 'Test User'})
        self.assertEqual(person_obj['name'], 'Test User')
        self.assertEqual(person_obj['age'], 30)
        self.assertEqual(person_obj['_id'], person._id)

    def test_save_custom_id(self):
        """Ensure that a document may be saved with a custom _id.
        """
        # Create person object and save it to the database
        person = self.Person(name='Test User', age=30, 
                             _id='497ce96f395f2f052a494fd4')
        person.save()
        # Ensure that the object is in the database with the correct _id
        collection = self.db[self.Person._meta['collection']]
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

        post = BlogPost(content='Went for a walk today...')
        post.tags = tags = ['fun', 'leisure']
        comments = [Comment(content='Good for you'), Comment(content='Yay.')]
        post.comments = comments
        post.save()

        collection = self.db[BlogPost._meta['collection']]
        post_obj = collection.find_one()
        self.assertEqual(post_obj['tags'], tags)
        for comment_obj, comment in zip(post_obj['comments'], comments):
            self.assertEqual(comment_obj['content'], comment['content'])

        BlogPost.drop_collection()

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
        collection = self.db[self.Person._meta['collection']]
        employee_obj = collection.find_one({'name': 'Test Employee'})
        self.assertEqual(employee_obj['name'], 'Test Employee')
        self.assertEqual(employee_obj['age'], 50)
        # Ensure that the 'details' embedded object saved correctly
        self.assertEqual(employee_obj['details']['position'], 'Developer')

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

        author = self.Person.objects(name='Test User').first()
        self.assertEqual(author.age, 25)

        BlogPost.drop_collection()

    def tearDown(self):
        self.Person.drop_collection()


if __name__ == '__main__':
    unittest.main()
