import unittest

from mongomap import *
from mongomap.connection import _get_db


class DocumentTest(unittest.TestCase):
    
    def setUp(self):
        connect(db='mongomaptest')

        class Person(Document):
            name = StringField()
            age = IntField()
        self.Person = Person

        self.db = _get_db()
        self.db.drop_collection(self.Person._meta['collection'])

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
        # Test iteration over fields
        fields = list(Person())
        self.assertTrue('name' in fields and 'age' in fields)
        # Ensure Document isn't treated like an actual document
        self.assertFalse(hasattr(Document, '_fields'))

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

        self.assertEquals(len(person), 2)

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

    def tearDown(self):
        self.db.drop_collection(self.Person._meta['collection'])


if __name__ == '__main__':
    unittest.main()
