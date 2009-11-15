import unittest

from mongomap.document import Document
from mongomap.fields import StringField, IntField


class DocumentTest(unittest.TestCase):

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

    def test_inheritance(self):
        """Ensure that document may inherit fields from a superclass document.
        """
        class Person(Document):
            name = StringField()

        class Employee(Person):
            salary = IntField()

        self.assertTrue('name' in Employee._fields)
        self.assertTrue('salary' in Employee._fields)
        self.assertEqual(Employee._meta['collection'], 
                         Person._meta['collection'])

    def test_creation(self):
        """Ensure that document may be created using keyword arguments.
        """
        class Person(Document):
            name = StringField()
            age = IntField()
        
        person = Person(name="Test User", age=30)
        self.assertEqual(person.name, "Test User")
        self.assertEqual(person.age, 30)


if __name__ == '__main__':
    unittest.main()
