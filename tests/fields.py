import unittest

from mongomap import *


class FieldTest(unittest.TestCase):

    def setUp(self):
        connect(db='mongomaptest')

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

    def test_required_values(self):
        """Ensure that required field constraints are enforced.
        """
        class Person(Document):
            name = StringField(required=True)
            age = IntField(required=True)
            userid = StringField()

        self.assertRaises(ValidationError, Person, name="Test User")
        self.assertRaises(ValidationError, Person, age=30)

        person = Person(name="Test User", age=30, userid="testuser")
        self.assertRaises(ValidationError, person.__setattr__, 'name', None)
        self.assertRaises(ValidationError, person.__setattr__, 'age', None)
        person.userid = None

    def test_object_id_validation(self):
        """Ensure that invalid values cannot be assigned to string fields.
        """
        class Person(Document):
            name = StringField()
        
        person = Person(name='Test User')
        self.assertRaises(AttributeError, getattr, person, '_id')
        self.assertRaises(ValidationError, person.__setattr__, '_id', 47)
        self.assertRaises(ValidationError, person.__setattr__, '_id', 'abc')
        person._id = '497ce96f395f2f052a494fd4'

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

    def test_embedded_document_validation(self):
        """Ensure that invalid embedded documents cannot be assigned to
        embedded document fields.
        """
        class Comment(EmbeddedDocument):
            content = StringField()

        class PersonPreferences(EmbeddedDocument):
            food = StringField()
            number = IntField()

        class Person(Document):
            name = StringField()
            preferences = EmbeddedDocumentField(PersonPreferences)

        person = Person(name='Test User')
        self.assertRaises(ValidationError, person.__setattr__, 'preferences', 
                          'My preferences')
        self.assertRaises(ValidationError, person.__setattr__, 'preferences', 
                          Comment(content='Nice blog post...'))
        person.preferences = PersonPreferences(food='Cheese', number=47)
        self.assertEqual(person.preferences.food, 'Cheese')

    def test_embedded_document_inheritance(self):
        """Ensure that subclasses of embedded documents may be provided to 
        EmbeddedDocumentFields of the superclass' type.
        """
        class User(EmbeddedDocument):
            name = StringField()

        class PowerUser(User):
            power = IntField()

        class BlogPost(Document):
            content = StringField()
            author = EmbeddedDocumentField(User)
        
        post = BlogPost(content='What I did today...')
        post.author = User(name='Test User')
        post.author = PowerUser(name='Test User', power=47)


if __name__ == '__main__':
    unittest.main()
