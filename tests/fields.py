import unittest

from mongomap import *


class FieldTest(unittest.TestCase):

    def test_default_values(self):
        """Ensure that default field values are used when creating a document.
        """
        class Person(Document):
            name = StringField()
            age = IntField(default=30)
            userid = StringField(default=lambda: 'test')

        person = Person(name='Test Person')
        self.assertEqual(person._data['age'], 30)
        self.assertEqual(person._data['userid'], 'test')

    def test_string_validation(self):
        """Ensure that invalid values cannot be assigned to string fields.
        """
        class Person(Document):
            name = StringField(max_length=20)
            userid = StringField(r'[0-9a-z_]+$')

        person = Person()
        # Test regex validation on userid
        self.assertRaises(ValidationError, person.__setattr__, 'userid',
                          'test.User')
        person.userid = 'test_user'
        self.assertEqual(person.userid, 'test_user')

        # Test max length validation on name
        self.assertRaises(ValidationError, person.__setattr__, 'name',
                          'Name that is more than twenty characters')
        person.name = 'Shorter name'
        self.assertEqual(person.name, 'Shorter name')

    def test_int_validation(self):
        """Ensure that invalid values cannot be assigned to int fields.
        """
        class Person(Document):
            age = IntField(min_value=0, max_value=110)

        person = Person()
        person.age = 50
        self.assertRaises(ValidationError, person.__setattr__, 'age', -1)
        self.assertRaises(ValidationError, person.__setattr__, 'age', 120)
        self.assertRaises(ValidationError, person.__setattr__, 'age', 'ten')


if __name__ == '__main__':
    unittest.main()
