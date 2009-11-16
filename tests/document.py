import unittest

from mongomap import Document, StringField, IntField


class DocumentTest(unittest.TestCase):
    
    def setUp(self):
        class Person(Document):
            name = StringField()
            age = IntField()
        self.Person = Person

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



if __name__ == '__main__':
    unittest.main()
