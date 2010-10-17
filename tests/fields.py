import unittest
import datetime
from decimal import Decimal

import pymongo
import gridfs

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

        person = Person(name="Test User")
        self.assertRaises(ValidationError, person.validate)
        person = Person(age=30)
        self.assertRaises(ValidationError, person.validate)

    def test_object_id_validation(self):
        """Ensure that invalid values cannot be assigned to string fields.
        """
        class Person(Document):
            name = StringField()
        
        person = Person(name='Test User')
        self.assertEqual(person.id, None)

        person.id = 47
        self.assertRaises(ValidationError, person.validate)

        person.id = 'abc'
        self.assertRaises(ValidationError, person.validate)

        person.id = '497ce96f395f2f052a494fd4'
        person.validate()

    def test_string_validation(self):
        """Ensure that invalid values cannot be assigned to string fields.
        """
        class Person(Document):
            name = StringField(max_length=20)
            userid = StringField(r'[0-9a-z_]+$')

        person = Person(name=34)
        self.assertRaises(ValidationError, person.validate)

        # Test regex validation on userid
        person = Person(userid='test.User')
        self.assertRaises(ValidationError, person.validate)

        person.userid = 'test_user'
        self.assertEqual(person.userid, 'test_user')
        person.validate()

        # Test max length validation on name
        person = Person(name='Name that is more than twenty characters')
        self.assertRaises(ValidationError, person.validate)

        person.name = 'Shorter name'
        person.validate()

    def test_url_validation(self):
        """Ensure that URLFields validate urls properly.
        """
        class Link(Document):
            url = URLField()

        link = Link()
        link.url = 'google'
        self.assertRaises(ValidationError, link.validate)

        link.url = 'http://www.google.com:8080'
        link.validate()
        
    def test_int_validation(self):
        """Ensure that invalid values cannot be assigned to int fields.
        """
        class Person(Document):
            age = IntField(min_value=0, max_value=110)

        person = Person()
        person.age = 50
        person.validate()

        person.age = -1
        self.assertRaises(ValidationError, person.validate)
        person.age = 120
        self.assertRaises(ValidationError, person.validate)
        person.age = 'ten'
        self.assertRaises(ValidationError, person.validate)

    def test_float_validation(self):
        """Ensure that invalid values cannot be assigned to float fields.
        """
        class Person(Document):
            height = FloatField(min_value=0.1, max_value=3.5)

        person = Person()
        person.height = 1.89
        person.validate()

        person.height = '2.0'
        self.assertRaises(ValidationError, person.validate)
        person.height = 0.01
        self.assertRaises(ValidationError, person.validate)
        person.height = 4.0
        self.assertRaises(ValidationError, person.validate)
        
    def test_decimal_validation(self):
        """Ensure that invalid values cannot be assigned to decimal fields.
        """
        class Person(Document):
            height = DecimalField(min_value=Decimal('0.1'), 
                                  max_value=Decimal('3.5'))

        Person.drop_collection()

        person = Person()
        person.height = Decimal('1.89')
        person.save()
        person.reload()
        self.assertEqual(person.height, Decimal('1.89'))

        person.height = '2.0'
        person.save()
        person.height = 0.01
        self.assertRaises(ValidationError, person.validate)
        person.height = Decimal('0.01')
        self.assertRaises(ValidationError, person.validate)
        person.height = Decimal('4.0')
        self.assertRaises(ValidationError, person.validate)

        Person.drop_collection()

    def test_boolean_validation(self):
        """Ensure that invalid values cannot be assigned to boolean fields.
        """
        class Person(Document):
            admin = BooleanField()

        person = Person()
        person.admin = True
        person.validate()

        person.admin = 2
        self.assertRaises(ValidationError, person.validate)
        person.admin = 'Yes'
        self.assertRaises(ValidationError, person.validate)

    def test_datetime_validation(self):
        """Ensure that invalid values cannot be assigned to datetime fields.
        """
        class LogEntry(Document):
            time = DateTimeField()

        log = LogEntry()
        log.time = datetime.datetime.now()
        log.validate()

        log.time = -1
        self.assertRaises(ValidationError, log.validate)
        log.time = '1pm'
        self.assertRaises(ValidationError, log.validate)

    def test_list_validation(self):
        """Ensure that a list field only accepts lists with valid elements.
        """
        class User(Document):
            pass

        class Comment(EmbeddedDocument):
            content = StringField()

        class BlogPost(Document):
            content = StringField()
            comments = ListField(EmbeddedDocumentField(Comment))
            tags = ListField(StringField())
            authors = ListField(ReferenceField(User))

        post = BlogPost(content='Went for a walk today...')
        post.validate()

        post.tags = 'fun'
        self.assertRaises(ValidationError, post.validate)
        post.tags = [1, 2]
        self.assertRaises(ValidationError, post.validate)

        post.tags = ['fun', 'leisure']
        post.validate()
        post.tags = ('fun', 'leisure')
        post.validate()

        post.comments = ['a']
        self.assertRaises(ValidationError, post.validate)
        post.comments = 'yay'
        self.assertRaises(ValidationError, post.validate)

        comments = [Comment(content='Good for you'), Comment(content='Yay.')]
        post.comments = comments
        post.validate()

        post.authors = [Comment()]
        self.assertRaises(ValidationError, post.validate)

        post.authors = [User()]
        post.validate()

    def test_sorted_list_sorting(self):
        """Ensure that a sorted list field properly sorts values.
        """
        class Comment(EmbeddedDocument):
            order = IntField()
            content = StringField()

        class BlogPost(Document):
            content = StringField()
            comments = SortedListField(EmbeddedDocumentField(Comment),
                                       ordering='order')
            tags = SortedListField(StringField())

        post = BlogPost(content='Went for a walk today...')
        post.save()

        post.tags = ['leisure', 'fun']
        post.save()
        post.reload()
        self.assertEqual(post.tags, ['fun', 'leisure'])
        
        comment1 = Comment(content='Good for you', order=1)
        comment2 = Comment(content='Yay.', order=0)
        comments = [comment1, comment2]
        post.comments = comments
        post.save()
        post.reload()
        self.assertEqual(post.comments[0].content, comment2.content)
        self.assertEqual(post.comments[1].content, comment1.content)

        BlogPost.drop_collection()

    def test_dict_validation(self):
        """Ensure that dict types work as expected.
        """
        class BlogPost(Document):
            info = DictField()

        post = BlogPost()
        post.info = 'my post'
        self.assertRaises(ValidationError, post.validate)

        post.info = ['test', 'test']
        self.assertRaises(ValidationError, post.validate)

        post.info = {'$title': 'test'}
        self.assertRaises(ValidationError, post.validate)

        post.info = {'the.title': 'test'}
        self.assertRaises(ValidationError, post.validate)

        post.info = {'title': 'test'}
        post.validate()

    def test_embedded_document_validation(self):
        """Ensure that invalid embedded documents cannot be assigned to
        embedded document fields.
        """
        class Comment(EmbeddedDocument):
            content = StringField()

        class PersonPreferences(EmbeddedDocument):
            food = StringField(required=True)
            number = IntField()

        class Person(Document):
            name = StringField()
            preferences = EmbeddedDocumentField(PersonPreferences)

        person = Person(name='Test User')
        person.preferences = 'My Preferences'
        self.assertRaises(ValidationError, person.validate)

        # Check that only the right embedded doc works
        person.preferences = Comment(content='Nice blog post...')
        self.assertRaises(ValidationError, person.validate)

        # Check that the embedded doc is valid
        person.preferences = PersonPreferences()
        self.assertRaises(ValidationError, person.validate)

        person.preferences = PersonPreferences(food='Cheese', number=47)
        self.assertEqual(person.preferences.food, 'Cheese')
        person.validate()

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
        """Ensure that invalid docment objects cannot be assigned to reference
        fields.
        """
        class User(Document):
            name = StringField()

        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(User)

        User.drop_collection()
        BlogPost.drop_collection()

        self.assertRaises(ValidationError, ReferenceField, EmbeddedDocument)

        user = User(name='Test User')

        # Ensure that the referenced object must have been saved
        post1 = BlogPost(content='Chips and gravy taste good.')
        post1.author = user
        self.assertRaises(ValidationError, post1.save)

        # Check that an invalid object type cannot be used
        post2 = BlogPost(content='Chips and chilli taste good.')
        post1.author = post2
        self.assertRaises(ValidationError, post1.validate)

        user.save()
        post1.author = user
        post1.save()

        post2.save()
        post1.author = post2
        self.assertRaises(ValidationError, post1.validate)

        User.drop_collection()
        BlogPost.drop_collection()
    
    def test_list_item_dereference(self):
        """Ensure that DBRef items in ListFields are dereferenced.
        """
        class User(Document):
            name = StringField()

        class Group(Document):
            members = ListField(ReferenceField(User))

        User.drop_collection()
        Group.drop_collection()

        user1 = User(name='user1')
        user1.save()
        user2 = User(name='user2')
        user2.save()

        group = Group(members=[user1, user2])
        group.save()

        group_obj = Group.objects.first()

        self.assertEqual(group_obj.members[0].name, user1.name)
        self.assertEqual(group_obj.members[1].name, user2.name)

        User.drop_collection()
        Group.drop_collection()

    def test_recursive_reference(self):
        """Ensure that ReferenceFields can reference their own documents.
        """
        class Employee(Document):
            name = StringField()
            boss = ReferenceField('self')
            friends = ListField(ReferenceField('self'))

        bill = Employee(name='Bill Lumbergh')
        bill.save()

        michael = Employee(name='Michael Bolton')
        michael.save()

        samir = Employee(name='Samir Nagheenanajar')
        samir.save()

        friends = [michael, samir]
        peter = Employee(name='Peter Gibbons', boss=bill, friends=friends)
        peter.save()

        peter = Employee.objects.with_id(peter.id)
        self.assertEqual(peter.boss, bill)
        self.assertEqual(peter.friends, friends)

    def test_recursive_embedding(self):
        """Ensure that EmbeddedDocumentFields can contain their own documents.
        """
        class Tree(Document):
            name = StringField()
            children = ListField(EmbeddedDocumentField('TreeNode'))

        class TreeNode(EmbeddedDocument):
            name = StringField()
            children = ListField(EmbeddedDocumentField('self'))
        
        tree = Tree(name="Tree")

        first_child = TreeNode(name="Child 1")
        tree.children.append(first_child)

        second_child = TreeNode(name="Child 2")
        first_child.children.append(second_child)
        
        third_child = TreeNode(name="Child 3")
        first_child.children.append(third_child)

        tree.save()

        tree_obj = Tree.objects.first()
        self.assertEqual(len(tree.children), 1)
        self.assertEqual(tree.children[0].name, first_child.name)
        self.assertEqual(tree.children[0].children[0].name, second_child.name)
        self.assertEqual(tree.children[0].children[1].name, third_child.name)

    def test_undefined_reference(self):
        """Ensure that ReferenceFields may reference undefined Documents.
        """
        class Product(Document):
            name = StringField()
            company = ReferenceField('Company')

        class Company(Document):
            name = StringField()

        ten_gen = Company(name='10gen')
        ten_gen.save()
        mongodb = Product(name='MongoDB', company=ten_gen)
        mongodb.save()

        obj = Product.objects(company=ten_gen).first()
        self.assertEqual(obj, mongodb)
        self.assertEqual(obj.company, ten_gen)

    def test_reference_query_conversion(self):
        """Ensure that ReferenceFields can be queried using objects and values
        of the type of the primary key of the referenced object.
        """
        class Member(Document):
            user_num = IntField(primary_key=True)

        class BlogPost(Document):
            title = StringField()
            author = ReferenceField(Member)

        Member.drop_collection()
        BlogPost.drop_collection()

        m1 = Member(user_num=1)
        m1.save()
        m2 = Member(user_num=2)
        m2.save()

        post1 = BlogPost(title='post 1', author=m1)
        post1.save()

        post2 = BlogPost(title='post 2', author=m2)
        post2.save()

        post = BlogPost.objects(author=m1).first()
        self.assertEqual(post.id, post1.id)

        post = BlogPost.objects(author=m2).first()
        self.assertEqual(post.id, post2.id)

        Member.drop_collection()
        BlogPost.drop_collection()
        
    def test_generic_reference(self):
        """Ensure that a GenericReferenceField properly dereferences items.
        """
        class Link(Document):
            title = StringField()
            meta = {'allow_inheritance': False}
            
        class Post(Document):
            title = StringField()
            
        class Bookmark(Document):
            bookmark_object = GenericReferenceField()
            
        Link.drop_collection()
        Post.drop_collection()
        Bookmark.drop_collection()
    
        link_1 = Link(title="Pitchfork")
        link_1.save()
    
        post_1 = Post(title="Behind the Scenes of the Pavement Reunion")
        post_1.save()
        
        bm = Bookmark(bookmark_object=post_1)
        bm.save()
        
        bm = Bookmark.objects(bookmark_object=post_1).first()
        
        self.assertEqual(bm.bookmark_object, post_1)
        self.assertTrue(isinstance(bm.bookmark_object, Post))
        
        bm.bookmark_object = link_1
        bm.save()
        
        bm = Bookmark.objects(bookmark_object=link_1).first()
        
        self.assertEqual(bm.bookmark_object, link_1)
        self.assertTrue(isinstance(bm.bookmark_object, Link))
    
        Link.drop_collection()
        Post.drop_collection()
        Bookmark.drop_collection()

    def test_generic_reference_list(self):
        """Ensure that a ListField properly dereferences generic references.
        """
        class Link(Document):
            title = StringField()
    
        class Post(Document):
            title = StringField()
    
        class User(Document):
            bookmarks = ListField(GenericReferenceField())
    
        Link.drop_collection()
        Post.drop_collection()
        User.drop_collection()
    
        link_1 = Link(title="Pitchfork")
        link_1.save()
    
        post_1 = Post(title="Behind the Scenes of the Pavement Reunion")
        post_1.save()
    
        user = User(bookmarks=[post_1, link_1])
        user.save()
        
        user = User.objects(bookmarks__all=[post_1, link_1]).first()
        
        self.assertEqual(user.bookmarks[0], post_1)
        self.assertEqual(user.bookmarks[1], link_1)
        
        Link.drop_collection()
        Post.drop_collection()
        User.drop_collection()

    def test_binary_fields(self):
        """Ensure that binary fields can be stored and retrieved.
        """
        class Attachment(Document):
            content_type = StringField()
            blob = BinaryField()

        BLOB = '\xe6\x00\xc4\xff\x07'
        MIME_TYPE = 'application/octet-stream'

        Attachment.drop_collection()

        attachment = Attachment(content_type=MIME_TYPE, blob=BLOB)
        attachment.save()

        attachment_1 = Attachment.objects().first()
        self.assertEqual(MIME_TYPE, attachment_1.content_type)
        self.assertEqual(BLOB, attachment_1.blob)

        Attachment.drop_collection()

    def test_binary_validation(self):
        """Ensure that invalid values cannot be assigned to binary fields.
        """
        class Attachment(Document):
            blob = BinaryField()

        class AttachmentRequired(Document):
            blob = BinaryField(required=True)

        class AttachmentSizeLimit(Document):
            blob = BinaryField(max_bytes=4)

        Attachment.drop_collection()
        AttachmentRequired.drop_collection()
        AttachmentSizeLimit.drop_collection()

        attachment = Attachment()
        attachment.validate()
        attachment.blob = 2
        self.assertRaises(ValidationError, attachment.validate)

        attachment_required = AttachmentRequired()
        self.assertRaises(ValidationError, attachment_required.validate)
        attachment_required.blob = '\xe6\x00\xc4\xff\x07'
        attachment_required.validate()

        attachment_size_limit = AttachmentSizeLimit(blob='\xe6\x00\xc4\xff\x07')
        self.assertRaises(ValidationError, attachment_size_limit.validate)
        attachment_size_limit.blob = '\xe6\x00\xc4\xff'
        attachment_size_limit.validate()

        Attachment.drop_collection()
        AttachmentRequired.drop_collection()
        AttachmentSizeLimit.drop_collection()

    def test_choices_validation(self):
        """Ensure that value is in a container of allowed values.
        """
        class Shirt(Document):
            size = StringField(max_length=3, choices=('S','M','L','XL','XXL'))

        Shirt.drop_collection()

        shirt = Shirt()
        shirt.validate()

        shirt.size = "S"
        shirt.validate()

        shirt.size = "XS"
        self.assertRaises(ValidationError, shirt.validate)

        Shirt.drop_collection()

    def test_file_fields(self):
        """Ensure that file fields can be written to and their data retrieved
        """
        class PutFile(Document):
            file = FileField()

        class StreamFile(Document):
            file = FileField()

        class SetFile(Document):
            file = FileField()

        text = 'Hello, World!'
        more_text = 'Foo Bar'
        content_type = 'text/plain'

        PutFile.drop_collection()
        StreamFile.drop_collection()
        SetFile.drop_collection()

        putfile = PutFile()
        putfile.file.put(text, content_type=content_type)
        putfile.save()
        putfile.validate()
        result = PutFile.objects.first()
        self.assertTrue(putfile == result)
        self.assertEquals(result.file.read(), text)
        self.assertEquals(result.file.content_type, content_type)
        result.file.delete() # Remove file from GridFS

        streamfile = StreamFile()
        streamfile.file.new_file(content_type=content_type)
        streamfile.file.write(text)
        streamfile.file.write(more_text)
        streamfile.file.close()
        streamfile.save()
        streamfile.validate()
        result = StreamFile.objects.first()
        self.assertTrue(streamfile == result)
        self.assertEquals(result.file.read(), text + more_text)
        self.assertEquals(result.file.content_type, content_type)
        result.file.delete()

        # Ensure deleted file returns None
        self.assertTrue(result.file.read() == None)

        setfile = SetFile()
        setfile.file = text
        setfile.save()
        setfile.validate()
        result = SetFile.objects.first()
        self.assertTrue(setfile == result)
        self.assertEquals(result.file.read(), text)

        # Try replacing file with new one
        result.file.replace(more_text)
        result.save()
        result.validate()
        result = SetFile.objects.first()
        self.assertTrue(setfile == result)
        self.assertEquals(result.file.read(), more_text)
        result.file.delete() 

        PutFile.drop_collection()
        StreamFile.drop_collection()
        SetFile.drop_collection()

        # Make sure FileField is optional and not required
        class DemoFile(Document):
            file = FileField()
        d = DemoFile.objects.create()

    def test_file_uniqueness(self):
        """Ensure that each instance of a FileField is unique
        """
        class TestFile(Document):
            name = StringField()
            file = FileField()

        # First instance
        testfile = TestFile()
        testfile.name = "Hello, World!"
        testfile.file.put('Hello, World!')
        testfile.save()

        # Second instance
        testfiledupe = TestFile()
        data = testfiledupe.file.read() # Should be None

        self.assertTrue(testfile.name != testfiledupe.name)
        self.assertTrue(testfile.file.read() != data)

        TestFile.drop_collection()

    def test_geo_indexes(self):
        """Ensure that indexes are created automatically for GeoPointFields.
        """
        class Event(Document):
            title = StringField()
            location = GeoPointField()

        Event.drop_collection()
        event = Event(title="Coltrane Motion @ Double Door",
                      location=[41.909889, -87.677137])
        event.save()

        info = Event.objects._collection.index_information()
        self.assertTrue(u'location_2d' in info)
        self.assertTrue(info[u'location_2d']['key'] == [(u'location', u'2d')])

        Event.drop_collection()

    def test_ensure_unique_default_instances(self):
        """Ensure that every field has it's own unique default instance."""
        class D(Document):
            data = DictField()
            data2 = DictField(default=lambda: {})

        d1 = D()
        d1.data['foo'] = 'bar'
        d1.data2['foo'] = 'bar'
        d2 = D()
        self.assertEqual(d2.data, {})
        self.assertEqual(d2.data2, {})

if __name__ == '__main__':
    unittest.main()
