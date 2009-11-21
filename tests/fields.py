import unittest

from mongoengine import *
from mongoengine.connection import _get_db


class FieldTest(unittest.TestCase):

    def setUp(self):
        connect(db='mongoenginetest')
        self.db = _get_db()

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
        self.assertRaises(ValidationError, person.__setattr__, 'name', 34)

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

    def test_list_validation(self):
        """Ensure that a list field only accepts lists with valid elements.
        """
        class Comment(EmbeddedDocument):
            content = StringField()

        class BlogPost(Document):
            content = StringField()
            comments = ListField(EmbeddedDocumentField(Comment))
            tags = ListField(StringField())

        post = BlogPost(content='Went for a walk today...')
        self.assertRaises(ValidationError, post.__setattr__, 'tags', 'fun')
        self.assertRaises(ValidationError, post.__setattr__, 'tags', [1, 2])
        post.tags = ['fun', 'leisure']
        post.tags = ('fun', 'leisure')

        comments = [Comment(content='Good for you'), Comment(content='Yay.')]
        self.assertRaises(ValidationError, post.__setattr__, 'comments', ['a'])
        self.assertRaises(ValidationError, post.__setattr__, 'comments', 'Yay')
        self.assertRaises(ValidationError, post.__setattr__, 'comments', 'Yay')
        post.comments = comments

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

    def test_reference_validation(self):
        """Ensure that invalid embedded documents cannot be assigned to
        embedded document fields.
        """
        class User(Document):
            name = StringField()

        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(User)

        self.assertRaises(ValidationError, ReferenceField, EmbeddedDocument)

        user = User(name='Test User')

        post1 = BlogPost(content='Chips and gravy taste good.')
        post1.author = user
        self.assertRaises(ValidationError, post1.save)

        post2 = BlogPost(content='Chips and chilli taste good.')
        self.assertRaises(ValidationError, post1.__setattr__, 'author', post2)

        user.save()
        post1.author = user
        post1.save()

        post2.save()
        self.assertRaises(ValidationError, post1.__setattr__, 'author', post2)

        User.drop_collection()
        BlogPost.drop_collection()


if __name__ == '__main__':
    unittest.main()
