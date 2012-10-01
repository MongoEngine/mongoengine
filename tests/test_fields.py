# -*- coding: utf-8 -*-
from __future__ import with_statement
import datetime
import os
import unittest
import uuid
import tempfile

from decimal import Decimal

from bson import Binary, DBRef, ObjectId
import gridfs

from nose.plugins.skip import SkipTest
from mongoengine import *
from mongoengine.connection import get_db
from mongoengine.base import _document_registry, NotRegistered
from mongoengine.python_support import PY3, b, StringIO, bin_type

TEST_IMAGE_PATH = os.path.join(os.path.dirname(__file__), 'mongoengine.png')

class FieldTest(unittest.TestCase):

    def setUp(self):
        connect(db='mongoenginetest')
        self.db = get_db()

    def tearDown(self):
        self.db.drop_collection('fs.files')
        self.db.drop_collection('fs.chunks')

    def test_default_values(self):
        """Ensure that default field values are used when creating a document.
        """
        class Person(Document):
            name = StringField()
            age = IntField(default=30, help_text="Your real age")
            userid = StringField(default=lambda: 'test', verbose_name="User Identity")

        person = Person(name='Test Person')
        self.assertEqual(person._data['age'], 30)
        self.assertEqual(person._data['userid'], 'test')
        self.assertEqual(person._fields['name'].help_text, None)
        self.assertEqual(person._fields['age'].help_text, "Your real age")
        self.assertEqual(person._fields['userid'].verbose_name, "User Identity")

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

    def test_not_required_handles_none_in_update(self):
        """Ensure that every fields should accept None if required is False.
        """

        class HandleNoneFields(Document):
            str_fld = StringField()
            int_fld = IntField()
            flt_fld = FloatField()
            comp_dt_fld = ComplexDateTimeField()

        HandleNoneFields.drop_collection()

        doc = HandleNoneFields()
        doc.str_fld = u'spam ham egg'
        doc.int_fld = 42
        doc.flt_fld = 4.2
        doc.com_dt_fld = datetime.datetime.utcnow()
        doc.save()

        res = HandleNoneFields.objects(id=doc.id).update(
            set__str_fld=None,
            set__int_fld=None,
            set__flt_fld=None,
            set__comp_dt_fld=None,
        )
        self.assertEqual(res, 1)

        # Retrive data from db and verify it.
        ret = HandleNoneFields.objects.all()[0]
        self.assertEqual(ret.str_fld, None)
        self.assertEqual(ret.int_fld, None)
        self.assertEqual(ret.flt_fld, None)

        # Return current time if retrived value is None.
        self.assertTrue(isinstance(ret.comp_dt_fld, datetime.datetime))

    def test_not_required_handles_none_from_database(self):
        """Ensure that every fields can handle null values from the database.
        """

        class HandleNoneFields(Document):
            str_fld = StringField(required=True)
            int_fld = IntField(required=True)
            flt_fld = FloatField(required=True)
            comp_dt_fld = ComplexDateTimeField(required=True)

        HandleNoneFields.drop_collection()

        doc = HandleNoneFields()
        doc.str_fld = u'spam ham egg'
        doc.int_fld = 42
        doc.flt_fld = 4.2
        doc.com_dt_fld = datetime.datetime.utcnow()
        doc.save()

        collection = self.db[HandleNoneFields._get_collection_name()]
        obj = collection.update({"_id": doc.id}, {"$unset": {
            "str_fld": 1,
            "int_fld": 1,
            "flt_fld": 1,
            "comp_dt_fld": 1}
        })

        # Retrive data from db and verify it.
        ret = HandleNoneFields.objects.all()[0]

        self.assertEqual(ret.str_fld, None)
        self.assertEqual(ret.int_fld, None)
        self.assertEqual(ret.flt_fld, None)
        # Return current time if retrived value is None.
        self.assertTrue(isinstance(ret.comp_dt_fld, datetime.datetime))

        self.assertRaises(ValidationError, ret.validate)

    def test_int_and_float_ne_operator(self):
        class TestDocument(Document):
            int_fld = IntField()
            float_fld = FloatField()

        TestDocument.drop_collection()

        TestDocument(int_fld=None, float_fld=None).save()
        TestDocument(int_fld=1, float_fld=1).save()

        self.assertEqual(1, TestDocument.objects(int_fld__ne=None).count())
        self.assertEqual(1, TestDocument.objects(float_fld__ne=None).count())

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

    def test_uuid_field_string(self):
        """Test UUID fields storing as String
        """
        class Person(Document):
            api_key = UUIDField(binary=False)

        Person.drop_collection()

        uu = uuid.uuid4()
        Person(api_key=uu).save()
        self.assertEqual(1, Person.objects(api_key=uu).count())
        self.assertEqual(uu, Person.objects.first().api_key)

        person = Person()
        valid = (uuid.uuid4(), uuid.uuid1())
        for api_key in valid:
            person.api_key = api_key
            person.validate()

        invalid = ('9d159858-549b-4975-9f98-dd2f987c113g',
                   '9d159858-549b-4975-9f98-dd2f987c113')
        for api_key in invalid:
            person.api_key = api_key
            self.assertRaises(ValidationError, person.validate)

    def test_uuid_field_binary(self):
        """Test UUID fields storing as Binary object
        """
        class Person(Document):
            api_key = UUIDField(binary=True)

        Person.drop_collection()

        uu = uuid.uuid4()
        Person(api_key=uu).save()
        self.assertEqual(1, Person.objects(api_key=uu).count())
        self.assertEqual(uu, Person.objects.first().api_key)

        person = Person()
        valid = (uuid.uuid4(), uuid.uuid1())
        for api_key in valid:
            person.api_key = api_key
            person.validate()

        invalid = ('9d159858-549b-4975-9f98-dd2f987c113g',
                   '9d159858-549b-4975-9f98-dd2f987c113')
        for api_key in invalid:
            person.api_key = api_key
            self.assertRaises(ValidationError, person.validate)


    def test_datetime_validation(self):
        """Ensure that invalid values cannot be assigned to datetime fields.
        """
        class LogEntry(Document):
            time = DateTimeField()

        log = LogEntry()
        log.time = datetime.datetime.now()
        log.validate()

        log.time = datetime.date.today()
        log.validate()

        log.time = -1
        self.assertRaises(ValidationError, log.validate)
        log.time = '1pm'
        self.assertRaises(ValidationError, log.validate)

    def test_datetime(self):
        """Tests showing pymongo datetime fields handling of microseconds.
        Microseconds are rounded to the nearest millisecond and pre UTC
        handling is wonky.

        See: http://api.mongodb.org/python/current/api/bson/son.html#dt
        """
        class LogEntry(Document):
            date = DateTimeField()

        LogEntry.drop_collection()

        # Test can save dates
        log = LogEntry()
        log.date = datetime.date.today()
        log.save()
        log.reload()
        self.assertEqual(log.date.date(), datetime.date.today())

        LogEntry.drop_collection()

        # Post UTC - microseconds are rounded (down) nearest millisecond and dropped
        d1 = datetime.datetime(1970, 01, 01, 00, 00, 01, 999)
        d2 = datetime.datetime(1970, 01, 01, 00, 00, 01)
        log = LogEntry()
        log.date = d1
        log.save()
        log.reload()
        self.assertNotEqual(log.date, d1)
        self.assertEqual(log.date, d2)

        # Post UTC - microseconds are rounded (down) nearest millisecond
        d1 = datetime.datetime(1970, 01, 01, 00, 00, 01, 9999)
        d2 = datetime.datetime(1970, 01, 01, 00, 00, 01, 9000)
        log.date = d1
        log.save()
        log.reload()
        self.assertNotEqual(log.date, d1)
        self.assertEqual(log.date, d2)

        if not PY3:
            # Pre UTC dates microseconds below 1000 are dropped
            # This does not seem to be true in PY3
            d1 = datetime.datetime(1969, 12, 31, 23, 59, 59, 999)
            d2 = datetime.datetime(1969, 12, 31, 23, 59, 59)
            log.date = d1
            log.save()
            log.reload()
            self.assertNotEqual(log.date, d1)
            self.assertEqual(log.date, d2)

        LogEntry.drop_collection()

    def test_complexdatetime_storage(self):
        """Tests for complex datetime fields - which can handle microseconds
        without rounding.
        """
        class LogEntry(Document):
            date = ComplexDateTimeField()

        LogEntry.drop_collection()

        # Post UTC - microseconds are rounded (down) nearest millisecond and dropped - with default datetimefields
        d1 = datetime.datetime(1970, 01, 01, 00, 00, 01, 999)
        log = LogEntry()
        log.date = d1
        log.save()
        log.reload()
        self.assertEqual(log.date, d1)

        # Post UTC - microseconds are rounded (down) nearest millisecond - with default datetimefields
        d1 = datetime.datetime(1970, 01, 01, 00, 00, 01, 9999)
        log.date = d1
        log.save()
        log.reload()
        self.assertEqual(log.date, d1)

        # Pre UTC dates microseconds below 1000 are dropped - with default datetimefields
        d1 = datetime.datetime(1969, 12, 31, 23, 59, 59, 999)
        log.date = d1
        log.save()
        log.reload()
        self.assertEqual(log.date, d1)

        # Pre UTC microseconds above 1000 is wonky - with default datetimefields
        # log.date has an invalid microsecond value so I can't construct
        # a date to compare.
        for i in xrange(1001, 3113, 33):
            d1 = datetime.datetime(1969, 12, 31, 23, 59, 59, i)
            log.date = d1
            log.save()
            log.reload()
            self.assertEqual(log.date, d1)
            log1 = LogEntry.objects.get(date=d1)
            self.assertEqual(log, log1)

        LogEntry.drop_collection()

    def test_complexdatetime_usage(self):
        """Tests for complex datetime fields - which can handle microseconds
        without rounding.
        """
        class LogEntry(Document):
            date = ComplexDateTimeField()

        LogEntry.drop_collection()

        d1 = datetime.datetime(1970, 01, 01, 00, 00, 01, 999)
        log = LogEntry()
        log.date = d1
        log.save()

        log1 = LogEntry.objects.get(date=d1)
        self.assertEqual(log, log1)

        LogEntry.drop_collection()

        # create 60 log entries
        for i in xrange(1950, 2010):
            d = datetime.datetime(i, 01, 01, 00, 00, 01, 999)
            LogEntry(date=d).save()

        self.assertEqual(LogEntry.objects.count(), 60)

        # Test ordering
        logs = LogEntry.objects.order_by("date")
        count = logs.count()
        i = 0
        while i == count - 1:
            self.assertTrue(logs[i].date <= logs[i + 1].date)
            i += 1

        logs = LogEntry.objects.order_by("-date")
        count = logs.count()
        i = 0
        while i == count - 1:
            self.assertTrue(logs[i].date >= logs[i + 1].date)
            i += 1

        # Test searching
        logs = LogEntry.objects.filter(date__gte=datetime.datetime(1980, 1, 1))
        self.assertEqual(logs.count(), 30)

        logs = LogEntry.objects.filter(date__lte=datetime.datetime(1980, 1, 1))
        self.assertEqual(logs.count(), 30)

        logs = LogEntry.objects.filter(
            date__lte=datetime.datetime(2011, 1, 1),
            date__gte=datetime.datetime(2000, 1, 1),
        )
        self.assertEqual(logs.count(), 10)

        LogEntry.drop_collection()

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
            generic = ListField(GenericReferenceField())

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
        self.assertRaises(ValidationError, post.validate)

        user = User()
        user.save()
        post.authors = [user]
        post.validate()

        post.generic = [1, 2]
        self.assertRaises(ValidationError, post.validate)

        post.generic = [User(), Comment()]
        self.assertRaises(ValidationError, post.validate)

        post.generic = [Comment()]
        self.assertRaises(ValidationError, post.validate)

        post.generic = [user]
        post.validate()

        User.drop_collection()
        BlogPost.drop_collection()

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

    def test_reverse_list_sorting(self):
        '''Ensure that a reverse sorted list field properly sorts values'''

        class Category(EmbeddedDocument):
            count = IntField()
            name = StringField()

        class CategoryList(Document):
            categories = SortedListField(EmbeddedDocumentField(Category), ordering='count', reverse=True)
            name = StringField()

        catlist = CategoryList(name="Top categories")
        cat1 = Category(name='posts', count=10)
        cat2 = Category(name='food', count=100)
        cat3 = Category(name='drink', count=40)
        catlist.categories = [cat1, cat2, cat3]
        catlist.save()
        catlist.reload()

        self.assertEqual(catlist.categories[0].name, cat2.name)
        self.assertEqual(catlist.categories[1].name, cat3.name)
        self.assertEqual(catlist.categories[2].name, cat1.name)

        CategoryList.drop_collection()

    def test_list_field(self):
        """Ensure that list types work as expected.
        """
        class BlogPost(Document):
            info = ListField()

        BlogPost.drop_collection()

        post = BlogPost()
        post.info = 'my post'
        self.assertRaises(ValidationError, post.validate)

        post.info = {'title': 'test'}
        self.assertRaises(ValidationError, post.validate)

        post.info = ['test']
        post.save()

        post = BlogPost()
        post.info = [{'test': 'test'}]
        post.save()

        post = BlogPost()
        post.info = [{'test': 3}]
        post.save()

        self.assertEqual(BlogPost.objects.count(), 3)
        self.assertEqual(BlogPost.objects.filter(info__exact='test').count(), 1)
        self.assertEqual(BlogPost.objects.filter(info__0__test='test').count(), 1)

        # Confirm handles non strings or non existing keys
        self.assertEqual(BlogPost.objects.filter(info__0__test__exact='5').count(), 0)
        self.assertEqual(BlogPost.objects.filter(info__100__test__exact='test').count(), 0)
        BlogPost.drop_collection()

    def test_list_field_passed_in_value(self):
        class Foo(Document):
            bars = ListField(ReferenceField("Bar"))

        class Bar(Document):
            text = StringField()

        bar = Bar(text="hi")
        bar.save()

        foo = Foo(bars=[])
        foo.bars.append(bar)
        self.assertEqual(repr(foo.bars), '[<Bar: Bar object>]')


    def test_list_field_strict(self):
        """Ensure that list field handles validation if provided a strict field type."""

        class Simple(Document):
            mapping = ListField(field=IntField())

        Simple.drop_collection()

        e = Simple()
        e.mapping = [1]
        e.save()

        def create_invalid_mapping():
            e.mapping = ["abc"]
            e.save()

        self.assertRaises(ValidationError, create_invalid_mapping)

        Simple.drop_collection()

    def test_list_field_rejects_strings(self):
        """Strings aren't valid list field data types"""

        class Simple(Document):
            mapping = ListField()

        Simple.drop_collection()
        e = Simple()
        e.mapping = 'hello world'

        self.assertRaises(ValidationError, e.save)

    def test_complex_field_required(self):
        """Ensure required cant be None / Empty"""

        class Simple(Document):
            mapping = ListField(required=True)

        Simple.drop_collection()
        e = Simple()
        e.mapping = []

        self.assertRaises(ValidationError, e.save)

        class Simple(Document):
            mapping = DictField(required=True)

        Simple.drop_collection()
        e = Simple()
        e.mapping = {}

        self.assertRaises(ValidationError, e.save)

    def test_list_field_complex(self):
        """Ensure that the list fields can handle the complex types."""

        class SettingBase(EmbeddedDocument):
            pass

        class StringSetting(SettingBase):
            value = StringField()

        class IntegerSetting(SettingBase):
            value = IntField()

        class Simple(Document):
            mapping = ListField()

        Simple.drop_collection()
        e = Simple()
        e.mapping.append(StringSetting(value='foo'))
        e.mapping.append(IntegerSetting(value=42))
        e.mapping.append({'number': 1, 'string': 'Hi!', 'float': 1.001,
                          'complex': IntegerSetting(value=42), 'list':
                          [IntegerSetting(value=42), StringSetting(value='foo')]})
        e.save()

        e2 = Simple.objects.get(id=e.id)
        self.assertTrue(isinstance(e2.mapping[0], StringSetting))
        self.assertTrue(isinstance(e2.mapping[1], IntegerSetting))

        # Test querying
        self.assertEqual(Simple.objects.filter(mapping__1__value=42).count(), 1)
        self.assertEqual(Simple.objects.filter(mapping__2__number=1).count(), 1)
        self.assertEqual(Simple.objects.filter(mapping__2__complex__value=42).count(), 1)
        self.assertEqual(Simple.objects.filter(mapping__2__list__0__value=42).count(), 1)
        self.assertEqual(Simple.objects.filter(mapping__2__list__1__value='foo').count(), 1)

        # Confirm can update
        Simple.objects().update(set__mapping__1=IntegerSetting(value=10))
        self.assertEqual(Simple.objects.filter(mapping__1__value=10).count(), 1)

        Simple.objects().update(
            set__mapping__2__list__1=StringSetting(value='Boo'))
        self.assertEqual(Simple.objects.filter(mapping__2__list__1__value='foo').count(), 0)
        self.assertEqual(Simple.objects.filter(mapping__2__list__1__value='Boo').count(), 1)

        Simple.drop_collection()

    def test_dict_field(self):
        """Ensure that dict types work as expected.
        """
        class BlogPost(Document):
            info = DictField()

        BlogPost.drop_collection()

        post = BlogPost()
        post.info = 'my post'
        self.assertRaises(ValidationError, post.validate)

        post.info = ['test', 'test']
        self.assertRaises(ValidationError, post.validate)

        post.info = {'$title': 'test'}
        self.assertRaises(ValidationError, post.validate)

        post.info = {'the.title': 'test'}
        self.assertRaises(ValidationError, post.validate)

        post.info = {1: 'test'}
        self.assertRaises(ValidationError, post.validate)

        post.info = {'title': 'test'}
        post.save()

        post = BlogPost()
        post.info = {'details': {'test': 'test'}}
        post.save()

        post = BlogPost()
        post.info = {'details': {'test': 3}}
        post.save()

        self.assertEqual(BlogPost.objects.count(), 3)
        self.assertEqual(BlogPost.objects.filter(info__title__exact='test').count(), 1)
        self.assertEqual(BlogPost.objects.filter(info__details__test__exact='test').count(), 1)

        # Confirm handles non strings or non existing keys
        self.assertEqual(BlogPost.objects.filter(info__details__test__exact=5).count(), 0)
        self.assertEqual(BlogPost.objects.filter(info__made_up__test__exact='test').count(), 0)

        post = BlogPost.objects.create(info={'title': 'original'})
        post.info.update({'title': 'updated'})
        post.save()
        post.reload()
        self.assertEqual('updated', post.info['title'])

        BlogPost.drop_collection()

    def test_dictfield_strict(self):
        """Ensure that dict field handles validation if provided a strict field type."""

        class Simple(Document):
            mapping = DictField(field=IntField())

        Simple.drop_collection()

        e = Simple()
        e.mapping['someint'] = 1
        e.save()

        def create_invalid_mapping():
            e.mapping['somestring'] = "abc"
            e.save()

        self.assertRaises(ValidationError, create_invalid_mapping)

        Simple.drop_collection()

    def test_dictfield_complex(self):
        """Ensure that the dict field can handle the complex types."""

        class SettingBase(EmbeddedDocument):
            pass

        class StringSetting(SettingBase):
            value = StringField()

        class IntegerSetting(SettingBase):
            value = IntField()

        class Simple(Document):
            mapping = DictField()

        Simple.drop_collection()
        e = Simple()
        e.mapping['somestring'] = StringSetting(value='foo')
        e.mapping['someint'] = IntegerSetting(value=42)
        e.mapping['nested_dict'] = {'number': 1, 'string': 'Hi!', 'float': 1.001,
                                 'complex': IntegerSetting(value=42), 'list':
                                [IntegerSetting(value=42), StringSetting(value='foo')]}
        e.save()

        e2 = Simple.objects.get(id=e.id)
        self.assertTrue(isinstance(e2.mapping['somestring'], StringSetting))
        self.assertTrue(isinstance(e2.mapping['someint'], IntegerSetting))

        # Test querying
        self.assertEqual(Simple.objects.filter(mapping__someint__value=42).count(), 1)
        self.assertEqual(Simple.objects.filter(mapping__nested_dict__number=1).count(), 1)
        self.assertEqual(Simple.objects.filter(mapping__nested_dict__complex__value=42).count(), 1)
        self.assertEqual(Simple.objects.filter(mapping__nested_dict__list__0__value=42).count(), 1)
        self.assertEqual(Simple.objects.filter(mapping__nested_dict__list__1__value='foo').count(), 1)

        # Confirm can update
        Simple.objects().update(
            set__mapping={"someint": IntegerSetting(value=10)})
        Simple.objects().update(
            set__mapping__nested_dict__list__1=StringSetting(value='Boo'))
        self.assertEqual(Simple.objects.filter(mapping__nested_dict__list__1__value='foo').count(), 0)
        self.assertEqual(Simple.objects.filter(mapping__nested_dict__list__1__value='Boo').count(), 1)

        Simple.drop_collection()

    def test_mapfield(self):
        """Ensure that the MapField handles the declared type."""

        class Simple(Document):
            mapping = MapField(IntField())

        Simple.drop_collection()

        e = Simple()
        e.mapping['someint'] = 1
        e.save()

        def create_invalid_mapping():
            e.mapping['somestring'] = "abc"
            e.save()

        self.assertRaises(ValidationError, create_invalid_mapping)

        def create_invalid_class():
            class NoDeclaredType(Document):
                mapping = MapField()

        self.assertRaises(ValidationError, create_invalid_class)

        Simple.drop_collection()

    def test_complex_mapfield(self):
        """Ensure that the MapField can handle complex declared types."""

        class SettingBase(EmbeddedDocument):
            pass

        class StringSetting(SettingBase):
            value = StringField()

        class IntegerSetting(SettingBase):
            value = IntField()

        class Extensible(Document):
            mapping = MapField(EmbeddedDocumentField(SettingBase))

        Extensible.drop_collection()

        e = Extensible()
        e.mapping['somestring'] = StringSetting(value='foo')
        e.mapping['someint'] = IntegerSetting(value=42)
        e.save()

        e2 = Extensible.objects.get(id=e.id)
        self.assertTrue(isinstance(e2.mapping['somestring'], StringSetting))
        self.assertTrue(isinstance(e2.mapping['someint'], IntegerSetting))

        def create_invalid_mapping():
            e.mapping['someint'] = 123
            e.save()

        self.assertRaises(ValidationError, create_invalid_mapping)

        Extensible.drop_collection()

    def test_embedded_mapfield_db_field(self):

        class Embedded(EmbeddedDocument):
            number = IntField(default=0, db_field='i')

        class Test(Document):
            my_map = MapField(field=EmbeddedDocumentField(Embedded), db_field='x')

        Test.drop_collection()

        test = Test()
        test.my_map['DICTIONARY_KEY'] = Embedded(number=1)
        test.save()

        Test.objects.update_one(inc__my_map__DICTIONARY_KEY__number=1)

        test = Test.objects.get()
        self.assertEqual(test.my_map['DICTIONARY_KEY'].number, 2)
        doc = self.db.test.find_one()
        self.assertEqual(doc['x']['DICTIONARY_KEY']['i'], 2)

    def test_map_field_lookup(self):
        """Ensure MapField lookups succeed on Fields without a lookup method"""

        class Log(Document):
            name = StringField()
            visited = MapField(DateTimeField())

        Log.drop_collection()
        Log(name="wilson", visited={'friends': datetime.datetime.now()}).save()

        self.assertEqual(1, Log.objects(
                                visited__friends__exists=True).count())

    def test_embedded_db_field(self):

        class Embedded(EmbeddedDocument):
            number = IntField(default=0, db_field='i')

        class Test(Document):
            embedded = EmbeddedDocumentField(Embedded, db_field='x')

        Test.drop_collection()

        test = Test()
        test.embedded = Embedded(number=1)
        test.save()

        Test.objects.update_one(inc__embedded__number=1)

        test = Test.objects.get()
        self.assertEqual(test.embedded.number, 2)
        doc = self.db.test.find_one()
        self.assertEqual(doc['x']['i'], 2)

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

    def test_dbref_reference_fields(self):

        class Person(Document):
            name = StringField()
            parent = ReferenceField('self', dbref=True)

        Person.drop_collection()

        p1 = Person(name="John").save()
        Person(name="Ross", parent=p1).save()

        col = Person._get_collection()
        data = col.find_one({'name': 'Ross'})
        self.assertEqual(data['parent'], DBRef('person', p1.pk))

        p = Person.objects.get(name="Ross")
        self.assertEqual(p.parent, p1)

    def test_objectid_reference_fields(self):

        class Person(Document):
            name = StringField()
            parent = ReferenceField('self', dbref=False)

        Person.drop_collection()

        p1 = Person(name="John").save()
        Person(name="Ross", parent=p1).save()

        col = Person._get_collection()
        data = col.find_one({'name': 'Ross'})
        self.assertEqual(data['parent'], p1.pk)

        p = Person.objects.get(name="Ross")
        self.assertEqual(p.parent, p1)

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

        Employee.drop_collection()
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

        Tree.drop_collection()
        tree = Tree(name="Tree")

        first_child = TreeNode(name="Child 1")
        tree.children.append(first_child)

        second_child = TreeNode(name="Child 2")
        first_child.children.append(second_child)
        tree.save()

        tree = Tree.objects.first()
        self.assertEqual(len(tree.children), 1)

        self.assertEqual(len(tree.children[0].children), 1)

        third_child = TreeNode(name="Child 3")
        tree.children[0].children.append(third_child)
        tree.save()

        self.assertEqual(len(tree.children), 1)
        self.assertEqual(tree.children[0].name, first_child.name)
        self.assertEqual(tree.children[0].children[0].name, second_child.name)
        self.assertEqual(tree.children[0].children[1].name, third_child.name)

        # Test updating
        tree.children[0].name = 'I am Child 1'
        tree.children[0].children[0].name = 'I am Child 2'
        tree.children[0].children[1].name = 'I am Child 3'
        tree.save()

        self.assertEqual(tree.children[0].name, 'I am Child 1')
        self.assertEqual(tree.children[0].children[0].name, 'I am Child 2')
        self.assertEqual(tree.children[0].children[1].name, 'I am Child 3')

        # Test removal
        self.assertEqual(len(tree.children[0].children), 2)
        del(tree.children[0].children[1])

        tree.save()
        self.assertEqual(len(tree.children[0].children), 1)

        tree.children[0].children.pop(0)
        tree.save()
        self.assertEqual(len(tree.children[0].children), 0)
        self.assertEqual(tree.children[0].children, [])

        tree.children[0].children.insert(0, third_child)
        tree.children[0].children.insert(0, second_child)
        tree.save()
        self.assertEqual(len(tree.children[0].children), 2)
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

        Product.drop_collection()
        Company.drop_collection()

        ten_gen = Company(name='10gen')
        ten_gen.save()
        mongodb = Product(name='MongoDB', company=ten_gen)
        mongodb.save()

        me = Product(name='MongoEngine')
        me.save()

        obj = Product.objects(company=ten_gen).first()
        self.assertEqual(obj, mongodb)
        self.assertEqual(obj.company, ten_gen)

        obj = Product.objects(company=None).first()
        self.assertEqual(obj, me)

        obj, created = Product.objects.get_or_create(company=None)

        self.assertEqual(created, False)
        self.assertEqual(obj, me)

    def test_reference_query_conversion(self):
        """Ensure that ReferenceFields can be queried using objects and values
        of the type of the primary key of the referenced object.
        """
        class Member(Document):
            user_num = IntField(primary_key=True)

        class BlogPost(Document):
            title = StringField()
            author = ReferenceField(Member, dbref=False)

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

    def test_reference_query_conversion_dbref(self):
        """Ensure that ReferenceFields can be queried using objects and values
        of the type of the primary key of the referenced object.
        """
        class Member(Document):
            user_num = IntField(primary_key=True)

        class BlogPost(Document):
            title = StringField()
            author = ReferenceField(Member, dbref=True)

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

    def test_generic_reference_document_not_registered(self):
        """Ensure dereferencing out of the document registry throws a
        `NotRegistered` error.
        """
        class Link(Document):
            title = StringField()

        class User(Document):
            bookmarks = ListField(GenericReferenceField())

        Link.drop_collection()
        User.drop_collection()

        link_1 = Link(title="Pitchfork")
        link_1.save()

        user = User(bookmarks=[link_1])
        user.save()

        # Mimic User and Link definitions being in a different file
        # and the Link model not being imported in the User file.
        del(_document_registry["Link"])

        user = User.objects.first()
        try:
            user.bookmarks
            raise AssertionError("Link was removed from the registry")
        except NotRegistered:
            pass

        Link.drop_collection()
        User.drop_collection()

    def test_generic_reference_is_none(self):

        class Person(Document):
            name = StringField()
            city = GenericReferenceField()

        Person.drop_collection()
        Person(name="Wilson Jr").save()

        self.assertEqual(repr(Person.objects(city=None)),
                            "[<Person: Person object>]")


    def test_generic_reference_choices(self):
        """Ensure that a GenericReferenceField can handle choices
        """
        class Link(Document):
            title = StringField()

        class Post(Document):
            title = StringField()

        class Bookmark(Document):
            bookmark_object = GenericReferenceField(choices=(Post,))

        Link.drop_collection()
        Post.drop_collection()
        Bookmark.drop_collection()

        link_1 = Link(title="Pitchfork")
        link_1.save()

        post_1 = Post(title="Behind the Scenes of the Pavement Reunion")
        post_1.save()

        bm = Bookmark(bookmark_object=link_1)
        self.assertRaises(ValidationError, bm.validate)

        bm = Bookmark(bookmark_object=post_1)
        bm.save()

        bm = Bookmark.objects.first()
        self.assertEqual(bm.bookmark_object, post_1)

    def test_generic_reference_list_choices(self):
        """Ensure that a ListField properly dereferences generic references and
        respects choices.
        """
        class Link(Document):
            title = StringField()

        class Post(Document):
            title = StringField()

        class User(Document):
            bookmarks = ListField(GenericReferenceField(choices=(Post,)))

        Link.drop_collection()
        Post.drop_collection()
        User.drop_collection()

        link_1 = Link(title="Pitchfork")
        link_1.save()

        post_1 = Post(title="Behind the Scenes of the Pavement Reunion")
        post_1.save()

        user = User(bookmarks=[link_1])
        self.assertRaises(ValidationError, user.validate)

        user = User(bookmarks=[post_1])
        user.save()

        user = User.objects.first()
        self.assertEqual(user.bookmarks, [post_1])

        Link.drop_collection()
        Post.drop_collection()
        User.drop_collection()

    def test_binary_fields(self):
        """Ensure that binary fields can be stored and retrieved.
        """
        class Attachment(Document):
            content_type = StringField()
            blob = BinaryField()

        BLOB = b('\xe6\x00\xc4\xff\x07')
        MIME_TYPE = 'application/octet-stream'

        Attachment.drop_collection()

        attachment = Attachment(content_type=MIME_TYPE, blob=BLOB)
        attachment.save()

        attachment_1 = Attachment.objects().first()
        self.assertEqual(MIME_TYPE, attachment_1.content_type)
        self.assertEqual(BLOB, bin_type(attachment_1.blob))

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
        attachment_required.blob = Binary(b('\xe6\x00\xc4\xff\x07'))
        attachment_required.validate()

        attachment_size_limit = AttachmentSizeLimit(blob=b('\xe6\x00\xc4\xff\x07'))
        self.assertRaises(ValidationError, attachment_size_limit.validate)
        attachment_size_limit.blob = b('\xe6\x00\xc4\xff')
        attachment_size_limit.validate()

        Attachment.drop_collection()
        AttachmentRequired.drop_collection()
        AttachmentSizeLimit.drop_collection()

    def test_binary_field_primary(self):

        class Attachment(Document):
            id = BinaryField(primary_key=True)

        Attachment.drop_collection()

        att = Attachment(id=uuid.uuid4().bytes).save()
        att.delete()

        self.assertEqual(0, Attachment.objects.count())

    def test_choices_validation(self):
        """Ensure that value is in a container of allowed values.
        """
        class Shirt(Document):
            size = StringField(max_length=3, choices=(('S', 'Small'), ('M', 'Medium'), ('L', 'Large'),
                                                      ('XL', 'Extra Large'), ('XXL', 'Extra Extra Large')))

        Shirt.drop_collection()

        shirt = Shirt()
        shirt.validate()

        shirt.size = "S"
        shirt.validate()

        shirt.size = "XS"
        self.assertRaises(ValidationError, shirt.validate)

        Shirt.drop_collection()

    def test_choices_get_field_display(self):
        """Test dynamic helper for returning the display value of a choices field.
        """
        class Shirt(Document):
            size = StringField(max_length=3, choices=(('S', 'Small'), ('M', 'Medium'), ('L', 'Large'),
                                                      ('XL', 'Extra Large'), ('XXL', 'Extra Extra Large')))
            style = StringField(max_length=3, choices=(('S', 'Small'), ('B', 'Baggy'), ('W', 'wide')), default='S')

        Shirt.drop_collection()

        shirt = Shirt()

        self.assertEqual(shirt.get_size_display(), None)
        self.assertEqual(shirt.get_style_display(), 'Small')

        shirt.size = "XXL"
        shirt.style = "B"
        self.assertEqual(shirt.get_size_display(), 'Extra Extra Large')
        self.assertEqual(shirt.get_style_display(), 'Baggy')

        # Set as Z - an invalid choice
        shirt.size = "Z"
        shirt.style = "Z"
        self.assertEqual(shirt.get_size_display(), 'Z')
        self.assertEqual(shirt.get_style_display(), 'Z')
        self.assertRaises(ValidationError, shirt.validate)

        Shirt.drop_collection()

    def test_simple_choices_validation(self):
        """Ensure that value is in a container of allowed values.
        """
        class Shirt(Document):
            size = StringField(max_length=3, choices=('S', 'M', 'L', 'XL', 'XXL'))

        Shirt.drop_collection()

        shirt = Shirt()
        shirt.validate()

        shirt.size = "S"
        shirt.validate()

        shirt.size = "XS"
        self.assertRaises(ValidationError, shirt.validate)

        Shirt.drop_collection()

    def test_simple_choices_get_field_display(self):
        """Test dynamic helper for returning the display value of a choices field.
        """
        class Shirt(Document):
            size = StringField(max_length=3, choices=('S', 'M', 'L', 'XL', 'XXL'))
            style = StringField(max_length=3, choices=('Small', 'Baggy', 'wide'), default='Small')

        Shirt.drop_collection()

        shirt = Shirt()

        self.assertEqual(shirt.get_size_display(), None)
        self.assertEqual(shirt.get_style_display(), 'Small')

        shirt.size = "XXL"
        shirt.style = "Baggy"
        self.assertEqual(shirt.get_size_display(), 'XXL')
        self.assertEqual(shirt.get_style_display(), 'Baggy')

        # Set as Z - an invalid choice
        shirt.size = "Z"
        shirt.style = "Z"
        self.assertEqual(shirt.get_size_display(), 'Z')
        self.assertEqual(shirt.get_style_display(), 'Z')
        self.assertRaises(ValidationError, shirt.validate)

        Shirt.drop_collection()

    def test_file_fields(self):
        """Ensure that file fields can be written to and their data retrieved
        """
        class PutFile(Document):
            the_file = FileField()

        class StreamFile(Document):
            the_file = FileField()

        class SetFile(Document):
            the_file = FileField()

        text = b('Hello, World!')
        more_text = b('Foo Bar')
        content_type = 'text/plain'

        PutFile.drop_collection()
        StreamFile.drop_collection()
        SetFile.drop_collection()

        putfile = PutFile()
        putfile.the_file.put(text, content_type=content_type)
        putfile.save()
        putfile.validate()
        result = PutFile.objects.first()
        self.assertTrue(putfile == result)
        self.assertEqual(result.the_file.read(), text)
        self.assertEqual(result.the_file.content_type, content_type)
        result.the_file.delete() # Remove file from GridFS
        PutFile.objects.delete()

        # Ensure file-like objects are stored
        putfile = PutFile()
        putstring = StringIO()
        putstring.write(text)
        putstring.seek(0)
        putfile.the_file.put(putstring, content_type=content_type)
        putfile.save()
        putfile.validate()
        result = PutFile.objects.first()
        self.assertTrue(putfile == result)
        self.assertEqual(result.the_file.read(), text)
        self.assertEqual(result.the_file.content_type, content_type)
        result.the_file.delete()

        streamfile = StreamFile()
        streamfile.the_file.new_file(content_type=content_type)
        streamfile.the_file.write(text)
        streamfile.the_file.write(more_text)
        streamfile.the_file.close()
        streamfile.save()
        streamfile.validate()
        result = StreamFile.objects.first()
        self.assertTrue(streamfile == result)
        self.assertEqual(result.the_file.read(), text + more_text)
        self.assertEqual(result.the_file.content_type, content_type)
        result.the_file.seek(0)
        self.assertEqual(result.the_file.tell(), 0)
        self.assertEqual(result.the_file.read(len(text)), text)
        self.assertEqual(result.the_file.tell(), len(text))
        self.assertEqual(result.the_file.read(len(more_text)), more_text)
        self.assertEqual(result.the_file.tell(), len(text + more_text))
        result.the_file.delete()

        # Ensure deleted file returns None
        self.assertTrue(result.the_file.read() == None)

        setfile = SetFile()
        setfile.the_file = text
        setfile.save()
        setfile.validate()
        result = SetFile.objects.first()
        self.assertTrue(setfile == result)
        self.assertEqual(result.the_file.read(), text)

        # Try replacing file with new one
        result.the_file.replace(more_text)
        result.save()
        result.validate()
        result = SetFile.objects.first()
        self.assertTrue(setfile == result)
        self.assertEqual(result.the_file.read(), more_text)
        result.the_file.delete()

        PutFile.drop_collection()
        StreamFile.drop_collection()
        SetFile.drop_collection()

        # Make sure FileField is optional and not required
        class DemoFile(Document):
            the_file = FileField()
        DemoFile.objects.create()


    def test_file_field_no_default(self):

        class GridDocument(Document):
            the_file = FileField()

        GridDocument.drop_collection()

        with tempfile.TemporaryFile() as f:
            f.write(b("Hello World!"))
            f.flush()

            # Test without default
            doc_a = GridDocument()
            doc_a.save()


            doc_b = GridDocument.objects.with_id(doc_a.id)
            doc_b.the_file.replace(f, filename='doc_b')
            doc_b.save()
            self.assertNotEqual(doc_b.the_file.grid_id, None)

            # Test it matches
            doc_c = GridDocument.objects.with_id(doc_b.id)
            self.assertEqual(doc_b.the_file.grid_id, doc_c.the_file.grid_id)

            # Test with default
            doc_d = GridDocument(the_file=b(''))
            doc_d.save()

            doc_e = GridDocument.objects.with_id(doc_d.id)
            self.assertEqual(doc_d.the_file.grid_id, doc_e.the_file.grid_id)

            doc_e.the_file.replace(f, filename='doc_e')
            doc_e.save()

            doc_f = GridDocument.objects.with_id(doc_e.id)
            self.assertEqual(doc_e.the_file.grid_id, doc_f.the_file.grid_id)

        db = GridDocument._get_db()
        grid_fs = gridfs.GridFS(db)
        self.assertEqual(['doc_b', 'doc_e'], grid_fs.list())

    def test_file_uniqueness(self):
        """Ensure that each instance of a FileField is unique
        """
        class TestFile(Document):
            name = StringField()
            the_file = FileField()

        # First instance
        test_file = TestFile()
        test_file.name = "Hello, World!"
        test_file.the_file.put(b('Hello, World!'))
        test_file.save()

        # Second instance
        test_file_dupe = TestFile()
        data = test_file_dupe.the_file.read() # Should be None

        self.assertTrue(test_file.name != test_file_dupe.name)
        self.assertTrue(test_file.the_file.read() != data)

        TestFile.drop_collection()

    def test_file_boolean(self):
        """Ensure that a boolean test of a FileField indicates its presence
        """
        class TestFile(Document):
            the_file = FileField()

        test_file = TestFile()
        self.assertFalse(bool(test_file.the_file))
        test_file.the_file = b('Hello, World!')
        test_file.the_file.content_type = 'text/plain'
        test_file.save()
        self.assertTrue(bool(test_file.the_file))

        TestFile.drop_collection()

    def test_file_cmp(self):
        """Test comparing against other types"""
        class TestFile(Document):
            the_file = FileField()

        test_file = TestFile()
        self.assertFalse(test_file.the_file in [{"test": 1}])

    def test_image_field(self):
        if PY3:
            raise SkipTest('PIL does not have Python 3 support')

        class TestImage(Document):
            image = ImageField()

        TestImage.drop_collection()

        t = TestImage()
        t.image.put(open(TEST_IMAGE_PATH, 'r'))
        t.save()

        t = TestImage.objects.first()

        self.assertEqual(t.image.format, 'PNG')

        w, h = t.image.size
        self.assertEqual(w, 371)
        self.assertEqual(h, 76)

        t.image.delete()

    def test_image_field_resize(self):
        if PY3:
            raise SkipTest('PIL does not have Python 3 support')

        class TestImage(Document):
            image = ImageField(size=(185, 37))

        TestImage.drop_collection()

        t = TestImage()
        t.image.put(open(TEST_IMAGE_PATH, 'r'))
        t.save()

        t = TestImage.objects.first()

        self.assertEqual(t.image.format, 'PNG')
        w, h = t.image.size

        self.assertEqual(w, 185)
        self.assertEqual(h, 37)

        t.image.delete()

    def test_image_field_resize_force(self):
        if PY3:
            raise SkipTest('PIL does not have Python 3 support')

        class TestImage(Document):
            image = ImageField(size=(185, 37, True))

        TestImage.drop_collection()

        t = TestImage()
        t.image.put(open(TEST_IMAGE_PATH, 'r'))
        t.save()

        t = TestImage.objects.first()

        self.assertEqual(t.image.format, 'PNG')
        w, h = t.image.size

        self.assertEqual(w, 185)
        self.assertEqual(h, 37)

        t.image.delete()

    def test_image_field_thumbnail(self):
        if PY3:
            raise SkipTest('PIL does not have Python 3 support')

        class TestImage(Document):
            image = ImageField(thumbnail_size=(92, 18))

        TestImage.drop_collection()

        t = TestImage()
        t.image.put(open(TEST_IMAGE_PATH, 'r'))
        t.save()

        t = TestImage.objects.first()

        self.assertEqual(t.image.thumbnail.format, 'PNG')
        self.assertEqual(t.image.thumbnail.width, 92)
        self.assertEqual(t.image.thumbnail.height, 18)

        t.image.delete()

    def test_file_multidb(self):
        register_connection('test_files', 'test_files')
        class TestFile(Document):
            name = StringField()
            the_file = FileField(db_alias="test_files",
                                 collection_name="macumba")

        TestFile.drop_collection()

        # delete old filesystem
        get_db("test_files").macumba.files.drop()
        get_db("test_files").macumba.chunks.drop()

        # First instance
        test_file = TestFile()
        test_file.name = "Hello, World!"
        test_file.the_file.put(b('Hello, World!'),
                          name="hello.txt")
        test_file.save()

        data = get_db("test_files").macumba.files.find_one()
        self.assertEqual(data.get('name'), 'hello.txt')

        test_file = TestFile.objects.first()
        self.assertEqual(test_file.the_file.read(),
                          b('Hello, World!'))

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

    def test_geo_embedded_indexes(self):
        """Ensure that indexes are created automatically for GeoPointFields on
        embedded documents.
        """
        class Venue(EmbeddedDocument):
            location = GeoPointField()
            name = StringField()

        class Event(Document):
            title = StringField()
            venue = EmbeddedDocumentField(Venue)

        Event.drop_collection()
        venue = Venue(name="Double Door", location=[41.909889, -87.677137])
        event = Event(title="Coltrane Motion", venue=venue)
        event.save()

        info = Event.objects._collection.index_information()
        self.assertTrue(u'location_2d' in info)
        self.assertTrue(info[u'location_2d']['key'] == [(u'location', u'2d')])

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

    def test_sequence_field(self):
        class Person(Document):
            id = SequenceField(primary_key=True)
            name = StringField()

        self.db['mongoengine.counters'].drop()
        Person.drop_collection()

        for x in xrange(10):
            p = Person(name="Person %s" % x)
            p.save()

        c = self.db['mongoengine.counters'].find_one({'_id': 'person.id'})
        self.assertEqual(c['next'], 10)

        ids = [i.id for i in Person.objects]
        self.assertEqual(ids, range(1, 11))

        c = self.db['mongoengine.counters'].find_one({'_id': 'person.id'})
        self.assertEqual(c['next'], 10)

    def test_sequence_field_sequence_name(self):
        class Person(Document):
            id = SequenceField(primary_key=True, sequence_name='jelly')
            name = StringField()

        self.db['mongoengine.counters'].drop()
        Person.drop_collection()

        for x in xrange(10):
            p = Person(name="Person %s" % x)
            p.save()

        c = self.db['mongoengine.counters'].find_one({'_id': 'jelly.id'})
        self.assertEqual(c['next'], 10)

        ids = [i.id for i in Person.objects]
        self.assertEqual(ids, range(1, 11))

        c = self.db['mongoengine.counters'].find_one({'_id': 'jelly.id'})
        self.assertEqual(c['next'], 10)

    def test_multiple_sequence_fields(self):
        class Person(Document):
            id = SequenceField(primary_key=True)
            counter = SequenceField()
            name = StringField()

        self.db['mongoengine.counters'].drop()
        Person.drop_collection()

        for x in xrange(10):
            p = Person(name="Person %s" % x)
            p.save()

        c = self.db['mongoengine.counters'].find_one({'_id': 'person.id'})
        self.assertEqual(c['next'], 10)

        ids = [i.id for i in Person.objects]
        self.assertEqual(ids, range(1, 11))

        counters = [i.counter for i in Person.objects]
        self.assertEqual(counters, range(1, 11))

        c = self.db['mongoengine.counters'].find_one({'_id': 'person.id'})
        self.assertEqual(c['next'], 10)

    def test_sequence_fields_reload(self):
        class Animal(Document):
            counter = SequenceField()
            type = StringField()

        self.db['mongoengine.counters'].drop()
        Animal.drop_collection()

        a = Animal(type="Boi")
        a.save()

        self.assertEqual(a.counter, 1)
        a.reload()
        self.assertEqual(a.counter, 1)

        a.counter = None
        self.assertEqual(a.counter, 2)
        a.save()

        self.assertEqual(a.counter, 2)

        a = Animal.objects.first()
        self.assertEqual(a.counter, 2)
        a.reload()
        self.assertEqual(a.counter, 2)

    def test_multiple_sequence_fields_on_docs(self):

        class Animal(Document):
            id = SequenceField(primary_key=True)

        class Person(Document):
            id = SequenceField(primary_key=True)

        self.db['mongoengine.counters'].drop()
        Animal.drop_collection()
        Person.drop_collection()

        for x in xrange(10):
            a = Animal(name="Animal %s" % x)
            a.save()
            p = Person(name="Person %s" % x)
            p.save()

        c = self.db['mongoengine.counters'].find_one({'_id': 'person.id'})
        self.assertEqual(c['next'], 10)

        c = self.db['mongoengine.counters'].find_one({'_id': 'animal.id'})
        self.assertEqual(c['next'], 10)

        ids = [i.id for i in Person.objects]
        self.assertEqual(ids, range(1, 11))

        id = [i.id for i in Animal.objects]
        self.assertEqual(id, range(1, 11))

        c = self.db['mongoengine.counters'].find_one({'_id': 'person.id'})
        self.assertEqual(c['next'], 10)

        c = self.db['mongoengine.counters'].find_one({'_id': 'animal.id'})
        self.assertEqual(c['next'], 10)

    def test_generic_embedded_document(self):
        class Car(EmbeddedDocument):
            name = StringField()

        class Dish(EmbeddedDocument):
            food = StringField(required=True)
            number = IntField()

        class Person(Document):
            name = StringField()
            like = GenericEmbeddedDocumentField()

        Person.drop_collection()

        person = Person(name='Test User')
        person.like = Car(name='Fiat')
        person.save()

        person = Person.objects.first()
        self.assertTrue(isinstance(person.like, Car))

        person.like = Dish(food="arroz", number=15)
        person.save()

        person = Person.objects.first()
        self.assertTrue(isinstance(person.like, Dish))

    def test_generic_embedded_document_choices(self):
        """Ensure you can limit GenericEmbeddedDocument choices
        """
        class Car(EmbeddedDocument):
            name = StringField()

        class Dish(EmbeddedDocument):
            food = StringField(required=True)
            number = IntField()

        class Person(Document):
            name = StringField()
            like = GenericEmbeddedDocumentField(choices=(Dish,))

        Person.drop_collection()

        person = Person(name='Test User')
        person.like = Car(name='Fiat')
        self.assertRaises(ValidationError, person.validate)

        person.like = Dish(food="arroz", number=15)
        person.save()

        person = Person.objects.first()
        self.assertTrue(isinstance(person.like, Dish))

    def test_generic_list_embedded_document_choices(self):
        """Ensure you can limit GenericEmbeddedDocument choices inside a list
        field
        """
        class Car(EmbeddedDocument):
            name = StringField()

        class Dish(EmbeddedDocument):
            food = StringField(required=True)
            number = IntField()

        class Person(Document):
            name = StringField()
            likes = ListField(GenericEmbeddedDocumentField(choices=(Dish,)))

        Person.drop_collection()

        person = Person(name='Test User')
        person.likes = [Car(name='Fiat')]
        self.assertRaises(ValidationError, person.validate)

        person.likes = [Dish(food="arroz", number=15)]
        person.save()

        person = Person.objects.first()
        self.assertTrue(isinstance(person.likes[0], Dish))

    def test_recursive_validation(self):
        """Ensure that a validation result to_dict is available.
        """
        class Author(EmbeddedDocument):
            name = StringField(required=True)

        class Comment(EmbeddedDocument):
            author = EmbeddedDocumentField(Author, required=True)
            content = StringField(required=True)

        class Post(Document):
            title = StringField(required=True)
            comments = ListField(EmbeddedDocumentField(Comment))

        bob = Author(name='Bob')
        post = Post(title='hello world')
        post.comments.append(Comment(content='hello', author=bob))
        post.comments.append(Comment(author=bob))

        self.assertRaises(ValidationError, post.validate)
        try:
            post.validate()
        except ValidationError, error:
            # ValidationError.errors property
            self.assertTrue(hasattr(error, 'errors'))
            self.assertTrue(isinstance(error.errors, dict))
            self.assertTrue('comments' in error.errors)
            self.assertTrue(1 in error.errors['comments'])
            self.assertTrue(isinstance(error.errors['comments'][1]['content'],
                            ValidationError))

            # ValidationError.schema property
            error_dict = error.to_dict()
            self.assertTrue(isinstance(error_dict, dict))
            self.assertTrue('comments' in error_dict)
            self.assertTrue(1 in error_dict['comments'])
            self.assertTrue('content' in error_dict['comments'][1])
            self.assertEqual(error_dict['comments'][1]['content'],
                              u'Field is required')


        post.comments[1].content = 'here we go'
        post.validate()


if __name__ == '__main__':
    unittest.main()
