# -*- coding: utf-8 -*-
import datetime
import unittest
import uuid
import sys

from nose.plugins.skip import SkipTest
import six

from decimal import Decimal

from bson import DBRef, ObjectId, SON

from mongoengine import *
from mongoengine.base import (BaseDict, BaseField, EmbeddedDocumentList,
                              _document_registry)

from tests.utils import MongoDBTestCase


class FieldTest(MongoDBTestCase):

    def test_default_values_nothing_set(self):
        """Ensure that default field values are used when creating
        a document.
        """
        class Person(Document):
            name = StringField()
            age = IntField(default=30, required=False)
            userid = StringField(default=lambda: 'test', required=True)
            created = DateTimeField(default=datetime.datetime.utcnow)
            day = DateField(default=datetime.date.today)

        person = Person(name="Ross")

        # Confirm saving now would store values
        data_to_be_saved = sorted(person.to_mongo().keys())
        self.assertEqual(data_to_be_saved,
            ['age', 'created', 'day', 'name', 'userid']
        )

        self.assertTrue(person.validate() is None)

        self.assertEqual(person.name, person.name)
        self.assertEqual(person.age, person.age)
        self.assertEqual(person.userid, person.userid)
        self.assertEqual(person.created, person.created)
        self.assertEqual(person.day, person.day)

        self.assertEqual(person._data['name'], person.name)
        self.assertEqual(person._data['age'], person.age)
        self.assertEqual(person._data['userid'], person.userid)
        self.assertEqual(person._data['created'], person.created)
        self.assertEqual(person._data['day'], person.day)

        # Confirm introspection changes nothing
        data_to_be_saved = sorted(person.to_mongo().keys())
        self.assertEqual(
            data_to_be_saved, ['age', 'created', 'day', 'name', 'userid'])

    def test_default_values_set_to_None(self):
        """Ensure that default field values are used even when
        we explcitly initialize the doc with None values.
        """
        class Person(Document):
            name = StringField()
            age = IntField(default=30, required=False)
            userid = StringField(default=lambda: 'test', required=True)
            created = DateTimeField(default=datetime.datetime.utcnow)

        # Trying setting values to None
        person = Person(name=None, age=None, userid=None, created=None)

        # Confirm saving now would store values
        data_to_be_saved = sorted(person.to_mongo().keys())
        self.assertEqual(data_to_be_saved, ['age', 'created', 'userid'])

        self.assertTrue(person.validate() is None)

        self.assertEqual(person.name, person.name)
        self.assertEqual(person.age, person.age)
        self.assertEqual(person.userid, person.userid)
        self.assertEqual(person.created, person.created)

        self.assertEqual(person._data['name'], person.name)
        self.assertEqual(person._data['age'], person.age)
        self.assertEqual(person._data['userid'], person.userid)
        self.assertEqual(person._data['created'], person.created)

        # Confirm introspection changes nothing
        data_to_be_saved = sorted(person.to_mongo().keys())
        self.assertEqual(data_to_be_saved, ['age', 'created', 'userid'])

    def test_default_values_when_setting_to_None(self):
        """Ensure that default field values are used when creating
        a document.
        """
        class Person(Document):
            name = StringField()
            age = IntField(default=30, required=False)
            userid = StringField(default=lambda: 'test', required=True)
            created = DateTimeField(default=datetime.datetime.utcnow)

        person = Person()
        person.name = None
        person.age = None
        person.userid = None
        person.created = None

        # Confirm saving now would store values
        data_to_be_saved = sorted(person.to_mongo().keys())
        self.assertEqual(data_to_be_saved, ['age', 'created', 'userid'])

        self.assertTrue(person.validate() is None)

        self.assertEqual(person.name, None)
        self.assertEqual(person.age, 30)
        self.assertEqual(person.userid, 'test')
        self.assertIsInstance(person.created, datetime.datetime)

        self.assertEqual(person._data['name'], person.name)
        self.assertEqual(person._data['age'], person.age)
        self.assertEqual(person._data['userid'], person.userid)
        self.assertEqual(person._data['created'], person.created)

        # Confirm introspection changes nothing
        data_to_be_saved = sorted(person.to_mongo().keys())
        self.assertEqual(data_to_be_saved, ['age', 'created', 'userid'])

    def test_default_value_is_not_used_when_changing_value_to_empty_list_for_strict_doc(self):
        """List field with default can be set to the empty list (strict)"""
        # Issue #1733
        class Doc(Document):
            x = ListField(IntField(), default=lambda: [42])

        doc = Doc(x=[1]).save()
        doc.x = []
        doc.save()
        reloaded = Doc.objects.get(id=doc.id)
        self.assertEqual(reloaded.x, [])

    def test_default_value_is_not_used_when_changing_value_to_empty_list_for_dyn_doc(self):
        """List field with default can be set to the empty list (dynamic)"""
        # Issue #1733
        class Doc(DynamicDocument):
            x = ListField(IntField(), default=lambda: [42])

        doc = Doc(x=[1]).save()
        doc.x = []
        doc.y = 2   # Was triggering the bug
        doc.save()
        reloaded = Doc.objects.get(id=doc.id)
        self.assertEqual(reloaded.x, [])

    def test_default_values_when_deleting_value(self):
        """Ensure that default field values are used after non-default
        values are explicitly deleted.
        """
        class Person(Document):
            name = StringField()
            age = IntField(default=30, required=False)
            userid = StringField(default=lambda: 'test', required=True)
            created = DateTimeField(default=datetime.datetime.utcnow)

        person = Person(name="Ross", age=50, userid='different',
                        created=datetime.datetime(2014, 6, 12))
        del person.name
        del person.age
        del person.userid
        del person.created

        data_to_be_saved = sorted(person.to_mongo().keys())
        self.assertEqual(data_to_be_saved, ['age', 'created', 'userid'])

        self.assertTrue(person.validate() is None)

        self.assertEqual(person.name, None)
        self.assertEqual(person.age, 30)
        self.assertEqual(person.userid, 'test')
        self.assertIsInstance(person.created, datetime.datetime)
        self.assertNotEqual(person.created, datetime.datetime(2014, 6, 12))

        self.assertEqual(person._data['name'], person.name)
        self.assertEqual(person._data['age'], person.age)
        self.assertEqual(person._data['userid'], person.userid)
        self.assertEqual(person._data['created'], person.created)

        # Confirm introspection changes nothing
        data_to_be_saved = sorted(person.to_mongo().keys())
        self.assertEqual(data_to_be_saved, ['age', 'created', 'userid'])

    def test_required_values(self):
        """Ensure that required field constraints are enforced."""
        class Person(Document):
            name = StringField(required=True)
            age = IntField(required=True)
            userid = StringField()

        person = Person(name="Test User")
        self.assertRaises(ValidationError, person.validate)
        person = Person(age=30)
        self.assertRaises(ValidationError, person.validate)

    def test_not_required_handles_none_in_update(self):
        """Ensure that every fields should accept None if required is
        False.
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
        self.assertIsNone(ret.str_fld)
        self.assertIsNone(ret.int_fld)
        self.assertIsNone(ret.flt_fld)

        self.assertIsNone(ret.comp_dt_fld)

    def test_not_required_handles_none_from_database(self):
        """Ensure that every field can handle null values from the
        database.
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
        doc.comp_dt_fld = datetime.datetime.utcnow()
        doc.save()

        # Unset all the fields
        obj = HandleNoneFields._get_collection().update({"_id": doc.id}, {
            "$unset": {
                "str_fld": 1,
                "int_fld": 1,
                "flt_fld": 1,
                "comp_dt_fld": 1
            }
        })

        # Retrive data from db and verify it.
        ret = HandleNoneFields.objects.first()
        self.assertIsNone(ret.str_fld)
        self.assertIsNone(ret.int_fld)
        self.assertIsNone(ret.flt_fld)
        self.assertIsNone(ret.comp_dt_fld)

        # Retrieved object shouldn't pass validation when a re-save is
        # attempted.
        self.assertRaises(ValidationError, ret.validate)

    def test_object_id_validation(self):
        """Ensure that invalid values cannot be assigned to an
        ObjectIdField.
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
        """Ensure that invalid values cannot be assigned to string fields."""
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

    def test_decimal_validation(self):
        """Ensure that invalid values cannot be assigned to decimal fields.
        """
        class Person(Document):
            height = DecimalField(min_value=Decimal('0.1'),
                                  max_value=Decimal('3.5'))

        Person.drop_collection()

        Person(height=Decimal('1.89')).save()
        person = Person.objects.first()
        self.assertEqual(person.height, Decimal('1.89'))

        person.height = '2.0'
        person.save()
        person.height = 0.01
        self.assertRaises(ValidationError, person.validate)
        person.height = Decimal('0.01')
        self.assertRaises(ValidationError, person.validate)
        person.height = Decimal('4.0')
        self.assertRaises(ValidationError, person.validate)
        person.height = 'something invalid'
        self.assertRaises(ValidationError, person.validate)

        person_2 = Person(height='something invalid')
        self.assertRaises(ValidationError, person_2.validate)

    def test_db_field_validation(self):
        """Ensure that db_field doesn't accept invalid values."""

        # dot in the name
        with self.assertRaises(ValueError):
            class User(Document):
                name = StringField(db_field='user.name')

        # name starting with $
        with self.assertRaises(ValueError):
            class User(Document):
                name = StringField(db_field='$name')

        # name containing a null character
        with self.assertRaises(ValueError):
            class User(Document):
                name = StringField(db_field='name\0')

    def test_decimal_comparison(self):
        class Person(Document):
            money = DecimalField()

        Person.drop_collection()

        Person(money=6).save()
        Person(money=8).save()
        Person(money=10).save()

        self.assertEqual(2, Person.objects(money__gt=Decimal("7")).count())
        self.assertEqual(2, Person.objects(money__gt=7).count())
        self.assertEqual(2, Person.objects(money__gt="7").count())

    def test_decimal_storage(self):
        class Person(Document):
            float_value = DecimalField(precision=4)
            string_value = DecimalField(precision=4, force_string=True)

        Person.drop_collection()
        values_to_store = [10, 10.1, 10.11, "10.111", Decimal("10.1111"), Decimal("10.11111")]
        for store_at_creation in [True, False]:
            for value in values_to_store:
                # to_python is called explicitly if values were sent in the kwargs of __init__
                if store_at_creation:
                    Person(float_value=value, string_value=value).save()
                else:
                    person = Person.objects.create()
                    person.float_value = value
                    person.string_value = value
                    person.save()

        # How its stored
        expected = [
            {'float_value': 10.0, 'string_value': '10.0000'},
            {'float_value': 10.1, 'string_value': '10.1000'},
            {'float_value': 10.11, 'string_value': '10.1100'},
            {'float_value': 10.111, 'string_value': '10.1110'},
            {'float_value': 10.1111, 'string_value': '10.1111'},
            {'float_value': 10.1111, 'string_value': '10.1111'}]
        expected.extend(expected)
        actual = list(Person.objects.exclude('id').as_pymongo())
        self.assertEqual(expected, actual)

        # How it comes out locally
        expected = [Decimal('10.0000'), Decimal('10.1000'), Decimal('10.1100'),
                    Decimal('10.1110'), Decimal('10.1111'), Decimal('10.1111')]
        expected.extend(expected)
        for field_name in ['float_value', 'string_value']:
            actual = list(Person.objects().scalar(field_name))
            self.assertEqual(expected, actual)

    def test_boolean_validation(self):
        """Ensure that invalid values cannot be assigned to boolean
        fields.
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
        person.admin = 'False'
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
        """Test UUID fields storing as Binary object."""
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

    def test_list_validation(self):
        """Ensure that a list field only accepts lists with valid elements."""
        AccessLevelChoices = (
            ('a', u'Administration'),
            ('b', u'Manager'),
            ('c', u'Staff'),
        )

        class User(Document):
            pass

        class Comment(EmbeddedDocument):
            content = StringField()

        class BlogPost(Document):
            content = StringField()
            comments = ListField(EmbeddedDocumentField(Comment))
            tags = ListField(StringField())
            authors = ListField(ReferenceField(User))
            authors_as_lazy = ListField(LazyReferenceField(User))
            generic = ListField(GenericReferenceField())
            generic_as_lazy = ListField(GenericLazyReferenceField())
            access_list = ListField(choices=AccessLevelChoices, display_sep=', ')

        User.drop_collection()
        BlogPost.drop_collection()

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

        post.access_list = 'a,b'
        self.assertRaises(ValidationError, post.validate)

        post.access_list = ['c', 'd']
        self.assertRaises(ValidationError, post.validate)

        post.access_list = ['a', 'b']
        post.validate()

        self.assertEqual(post.get_access_list_display(), u'Administration, Manager')

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

        post.authors_as_lazy = [Comment()]
        self.assertRaises(ValidationError, post.validate)

        post.authors_as_lazy = [User()]
        self.assertRaises(ValidationError, post.validate)

        post.authors_as_lazy = [user]
        post.validate()

        post.generic = [1, 2]
        self.assertRaises(ValidationError, post.validate)

        post.generic = [User(), Comment()]
        self.assertRaises(ValidationError, post.validate)

        post.generic = [Comment()]
        self.assertRaises(ValidationError, post.validate)

        post.generic = [user]
        post.validate()

        post.generic_as_lazy = [1, 2]
        self.assertRaises(ValidationError, post.validate)

        post.generic_as_lazy = [User(), Comment()]
        self.assertRaises(ValidationError, post.validate)

        post.generic_as_lazy = [Comment()]
        self.assertRaises(ValidationError, post.validate)

        post.generic_as_lazy = [user]
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

        BlogPost.drop_collection()

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

        post.comments[0].order = 2
        post.save()
        post.reload()

        self.assertEqual(post.comments[0].content, comment1.content)
        self.assertEqual(post.comments[1].content, comment2.content)

    def test_reverse_list_sorting(self):
        """Ensure that a reverse sorted list field properly sorts values"""

        class Category(EmbeddedDocument):
            count = IntField()
            name = StringField()

        class CategoryList(Document):
            categories = SortedListField(EmbeddedDocumentField(Category),
                                         ordering='count', reverse=True)
            name = StringField()

        CategoryList.drop_collection()

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

    def test_list_field(self):
        """Ensure that list types work as expected."""
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
        self.assertEqual(
            BlogPost.objects.filter(info__exact='test').count(), 1)
        self.assertEqual(
            BlogPost.objects.filter(info__0__test='test').count(), 1)

        # Confirm handles non strings or non existing keys
        self.assertEqual(
            BlogPost.objects.filter(info__0__test__exact='5').count(), 0)
        self.assertEqual(
            BlogPost.objects.filter(info__100__test__exact='test').count(), 0)

        # test queries by list
        post = BlogPost()
        post.info = ['1', '2']
        post.save()
        post = BlogPost.objects(info=['1', '2']).get()
        post.info += ['3', '4']
        post.save()
        self.assertEqual(BlogPost.objects(info=['1', '2', '3', '4']).count(), 1)
        post = BlogPost.objects(info=['1', '2', '3', '4']).get()
        post.info *= 2
        post.save()
        self.assertEqual(BlogPost.objects(info=['1', '2', '3', '4', '1', '2', '3', '4']).count(), 1)

    def test_list_field_manipulative_operators(self):
        """Ensure that ListField works with standard list operators that manipulate the list.
        """
        class BlogPost(Document):
            ref = StringField()
            info = ListField(StringField())

        BlogPost.drop_collection()

        post = BlogPost()
        post.ref = "1234"
        post.info = ['0', '1', '2', '3', '4', '5']
        post.save()

        def reset_post():
            post.info = ['0', '1', '2', '3', '4', '5']
            post.save()

        # '__add__(listB)'
        # listA+listB
        # operator.add(listA, listB)
        reset_post()
        temp = ['a', 'b']
        post.info = post.info + temp
        self.assertEqual(post.info, ['0', '1', '2', '3', '4', '5', 'a', 'b'])
        post.save()
        post.reload()
        self.assertEqual(post.info, ['0', '1', '2', '3', '4', '5', 'a', 'b'])

        # '__delitem__(index)'
        # aka 'del list[index]'
        # aka 'operator.delitem(list, index)'
        reset_post()
        del post.info[2]  # del from middle ('2')
        self.assertEqual(post.info, ['0', '1', '3', '4', '5'])
        post.save()
        post.reload()
        self.assertEqual(post.info, ['0', '1', '3', '4', '5'])

        # '__delitem__(slice(i, j))'
        # aka 'del list[i:j]'
        # aka 'operator.delitem(list, slice(i,j))'
        reset_post()
        del post.info[1:3]  # removes '1', '2'
        self.assertEqual(post.info, ['0', '3', '4', '5'])
        post.save()
        post.reload()
        self.assertEqual(post.info, ['0', '3', '4', '5'])

        # '__iadd__'
        # aka 'list += list'
        reset_post()
        temp = ['a', 'b']
        post.info += temp
        self.assertEqual(post.info, ['0', '1', '2', '3', '4', '5', 'a', 'b'])
        post.save()
        post.reload()
        self.assertEqual(post.info, ['0', '1', '2', '3', '4', '5', 'a', 'b'])

        # '__imul__'
        # aka 'list *= number'
        reset_post()
        post.info *= 2
        self.assertEqual(post.info, ['0', '1', '2', '3', '4', '5', '0', '1', '2', '3', '4', '5'])
        post.save()
        post.reload()
        self.assertEqual(post.info, ['0', '1', '2', '3', '4', '5', '0', '1', '2', '3', '4', '5'])

        # '__mul__'
        # aka 'listA*listB'
        reset_post()
        post.info = post.info * 2
        self.assertEqual(post.info, ['0', '1', '2', '3', '4', '5', '0', '1', '2', '3', '4', '5'])
        post.save()
        post.reload()
        self.assertEqual(post.info, ['0', '1', '2', '3', '4', '5', '0', '1', '2', '3', '4', '5'])

        # '__rmul__'
        # aka 'listB*listA'
        reset_post()
        post.info = 2 * post.info
        self.assertEqual(post.info, ['0', '1', '2', '3', '4', '5', '0', '1', '2', '3', '4', '5'])
        post.save()
        post.reload()
        self.assertEqual(post.info, ['0', '1', '2', '3', '4', '5', '0', '1', '2', '3', '4', '5'])

        # '__setitem__(index, value)'
        # aka 'list[index]=value'
        # aka 'setitem(list, value)'
        reset_post()
        post.info[4] = 'a'
        self.assertEqual(post.info, ['0', '1', '2', '3', 'a', '5'])
        post.save()
        post.reload()
        self.assertEqual(post.info, ['0', '1', '2', '3', 'a', '5'])

        # __setitem__(index, value) with a negative index
        reset_post()
        post.info[-2] = 'a'
        self.assertEqual(post.info, ['0', '1', '2', '3', 'a', '5'])
        post.save()
        post.reload()
        self.assertEqual(post.info, ['0', '1', '2', '3', 'a', '5'])

        # '__setitem__(slice(i, j), listB)'
        # aka 'listA[i:j] = listB'
        # aka 'setitem(listA, slice(i, j), listB)'
        reset_post()
        post.info[1:3] = ['h', 'e', 'l', 'l', 'o']
        self.assertEqual(post.info, ['0', 'h', 'e', 'l', 'l', 'o', '3', '4', '5'])
        post.save()
        post.reload()
        self.assertEqual(post.info, ['0', 'h', 'e', 'l', 'l', 'o', '3', '4', '5'])

        # '__setitem__(slice(i, j), listB)' with negative i and j
        reset_post()
        post.info[-5:-3] = ['h', 'e', 'l', 'l', 'o']
        self.assertEqual(post.info, ['0', 'h', 'e', 'l', 'l', 'o', '3', '4', '5'])
        post.save()
        post.reload()
        self.assertEqual(post.info, ['0', 'h', 'e', 'l', 'l', 'o', '3', '4', '5'])

        # negative

        # 'append'
        reset_post()
        post.info.append('h')
        self.assertEqual(post.info, ['0', '1', '2', '3', '4', '5', 'h'])
        post.save()
        post.reload()
        self.assertEqual(post.info, ['0', '1', '2', '3', '4', '5', 'h'])

        # 'extend'
        reset_post()
        post.info.extend(['h', 'e', 'l', 'l', 'o'])
        self.assertEqual(post.info, ['0', '1', '2', '3', '4', '5', 'h', 'e', 'l', 'l', 'o'])
        post.save()
        post.reload()
        self.assertEqual(post.info, ['0', '1', '2', '3', '4', '5', 'h', 'e', 'l', 'l', 'o'])
        # 'insert'

        # 'pop'
        reset_post()
        x = post.info.pop(2)
        y = post.info.pop()
        self.assertEqual(post.info, ['0', '1', '3', '4'])
        self.assertEqual(x, '2')
        self.assertEqual(y, '5')
        post.save()
        post.reload()
        self.assertEqual(post.info, ['0', '1', '3', '4'])

        # 'remove'
        reset_post()
        post.info.remove('2')
        self.assertEqual(post.info, ['0', '1', '3', '4', '5'])
        post.save()
        post.reload()
        self.assertEqual(post.info, ['0', '1', '3', '4', '5'])

        # 'reverse'
        reset_post()
        post.info.reverse()
        self.assertEqual(post.info, ['5', '4', '3', '2', '1', '0'])
        post.save()
        post.reload()
        self.assertEqual(post.info, ['5', '4', '3', '2', '1', '0'])

        # 'sort': though this operator method does manipulate the list, it is
        # tested in the 'test_list_field_lexicograpic_operators' function

    def test_list_field_invalid_operators(self):
        class BlogPost(Document):
            ref = StringField()
            info = ListField(StringField())

        post = BlogPost()
        post.ref = "1234"
        post.info = ['0', '1', '2', '3', '4', '5']

        # '__hash__'
        # aka 'hash(list)'
        self.assertRaises(TypeError, lambda: hash(post.info))

    def test_list_field_lexicographic_operators(self):
        """Ensure that ListField works with standard list operators that
        do lexigraphic ordering.
        """
        class BlogPost(Document):
            ref = StringField()
            text_info = ListField(StringField())
            oid_info = ListField(ObjectIdField())
            bool_info = ListField(BooleanField())

        BlogPost.drop_collection()

        blogSmall = BlogPost(ref="small")
        blogSmall.text_info = ["a", "a", "a"]
        blogSmall.bool_info = [False, False]
        blogSmall.save()
        blogSmall.reload()

        blogLargeA = BlogPost(ref="big")
        blogLargeA.text_info = ["a", "z", "j"]
        blogLargeA.bool_info = [False, True]
        blogLargeA.save()
        blogLargeA.reload()

        blogLargeB = BlogPost(ref="big2")
        blogLargeB.text_info = ["a", "z", "j"]
        blogLargeB.oid_info = [
            "54495ad94c934721ede76f90",
            "54495ad94c934721ede76d23",
            "54495ad94c934721ede76d00"
        ]
        blogLargeB.bool_info = [False, True]
        blogLargeB.save()
        blogLargeB.reload()

        # '__eq__' aka '=='
        self.assertEqual(blogLargeA.text_info, blogLargeB.text_info)
        self.assertEqual(blogLargeA.bool_info, blogLargeB.bool_info)

        # '__ge__' aka '>='
        self.assertGreaterEqual(blogLargeA.text_info, blogSmall.text_info)
        self.assertGreaterEqual(blogLargeA.text_info, blogLargeB.text_info)
        self.assertGreaterEqual(blogLargeA.bool_info, blogSmall.bool_info)
        self.assertGreaterEqual(blogLargeA.bool_info, blogLargeB.bool_info)

        # '__gt__' aka '>'
        self.assertGreaterEqual(blogLargeA.text_info, blogSmall.text_info)
        self.assertGreaterEqual(blogLargeA.bool_info, blogSmall.bool_info)

        # '__le__' aka '<='
        self.assertLessEqual(blogSmall.text_info, blogLargeB.text_info)
        self.assertLessEqual(blogLargeA.text_info, blogLargeB.text_info)
        self.assertLessEqual(blogSmall.bool_info, blogLargeB.bool_info)
        self.assertLessEqual(blogLargeA.bool_info, blogLargeB.bool_info)

        # '__lt__' aka '<'
        self.assertLess(blogSmall.text_info, blogLargeB.text_info)
        self.assertLess(blogSmall.bool_info, blogLargeB.bool_info)

        # '__ne__' aka '!='
        self.assertNotEqual(blogSmall.text_info, blogLargeB.text_info)
        self.assertNotEqual(blogSmall.bool_info, blogLargeB.bool_info)

        # 'sort'
        blogLargeB.bool_info = [True, False, True, False]
        blogLargeB.text_info.sort()
        blogLargeB.oid_info.sort()
        blogLargeB.bool_info.sort()
        sorted_target_list = [
            ObjectId("54495ad94c934721ede76d00"),
            ObjectId("54495ad94c934721ede76d23"),
            ObjectId("54495ad94c934721ede76f90")
        ]
        self.assertEqual(blogLargeB.text_info, ["a", "j", "z"])
        self.assertEqual(blogLargeB.oid_info, sorted_target_list)
        self.assertEqual(blogLargeB.bool_info, [False, False, True, True])
        blogLargeB.save()
        blogLargeB.reload()
        self.assertEqual(blogLargeB.text_info, ["a", "j", "z"])
        self.assertEqual(blogLargeB.oid_info, sorted_target_list)
        self.assertEqual(blogLargeB.bool_info, [False, False, True, True])

    def test_list_assignment(self):
        """Ensure that list field element assignment and slicing work."""
        class BlogPost(Document):
            info = ListField()

        BlogPost.drop_collection()

        post = BlogPost()
        post.info = ['e1', 'e2', 3, '4', 5]
        post.save()

        post.info[0] = 1
        post.save()
        post.reload()
        self.assertEqual(post.info[0], 1)

        post.info[1:3] = ['n2', 'n3']
        post.save()
        post.reload()
        self.assertEqual(post.info, [1, 'n2', 'n3', '4', 5])

        post.info[-1] = 'n5'
        post.save()
        post.reload()
        self.assertEqual(post.info, [1, 'n2', 'n3', '4', 'n5'])

        post.info[-2] = 4
        post.save()
        post.reload()
        self.assertEqual(post.info, [1, 'n2', 'n3', 4, 'n5'])

        post.info[1:-1] = [2]
        post.save()
        post.reload()
        self.assertEqual(post.info, [1, 2, 'n5'])

        post.info[:-1] = [1, 'n2', 'n3', 4]
        post.save()
        post.reload()
        self.assertEqual(post.info, [1, 'n2', 'n3', 4, 'n5'])

        post.info[-4:3] = [2, 3]
        post.save()
        post.reload()
        self.assertEqual(post.info, [1, 2, 3, 4, 'n5'])

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
        """Ensure that list field handles validation if provided
        a strict field type.
        """
        class Simple(Document):
            mapping = ListField(field=IntField())

        Simple.drop_collection()

        e = Simple()
        e.mapping = [1]
        e.save()

        # try creating an invalid mapping
        with self.assertRaises(ValidationError):
            e.mapping = ["abc"]
            e.save()

    def test_list_field_rejects_strings(self):
        """Strings aren't valid list field data types."""
        class Simple(Document):
            mapping = ListField()

        Simple.drop_collection()

        e = Simple()
        e.mapping = 'hello world'
        self.assertRaises(ValidationError, e.save)

    def test_complex_field_required(self):
        """Ensure required cant be None / Empty."""
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

    def test_complex_field_same_value_not_changed(self):
        """If a complex field is set to the same value, it should not
        be marked as changed.
        """
        class Simple(Document):
            mapping = ListField()

        Simple.drop_collection()

        e = Simple().save()
        e.mapping = []
        self.assertEqual([], e._changed_fields)

        class Simple(Document):
            mapping = DictField()

        Simple.drop_collection()

        e = Simple().save()
        e.mapping = {}
        self.assertEqual([], e._changed_fields)

    def test_slice_marks_field_as_changed(self):
        class Simple(Document):
            widgets = ListField()

        simple = Simple(widgets=[1, 2, 3, 4]).save()
        simple.widgets[:3] = []
        self.assertEqual(['widgets'], simple._changed_fields)
        simple.save()

        simple = simple.reload()
        self.assertEqual(simple.widgets, [4])

    def test_del_slice_marks_field_as_changed(self):
        class Simple(Document):
            widgets = ListField()

        simple = Simple(widgets=[1, 2, 3, 4]).save()
        del simple.widgets[:3]
        self.assertEqual(['widgets'], simple._changed_fields)
        simple.save()

        simple = simple.reload()
        self.assertEqual(simple.widgets, [4])

    def test_list_field_with_negative_indices(self):
        class Simple(Document):
            widgets = ListField()

        simple = Simple(widgets=[1, 2, 3, 4]).save()
        simple.widgets[-1] = 5
        self.assertEqual(['widgets.3'], simple._changed_fields)
        simple.save()

        simple = simple.reload()
        self.assertEqual(simple.widgets, [1, 2, 3, 5])

    def test_list_field_complex(self):
        """Ensure that the list fields can handle the complex types."""
        class SettingBase(EmbeddedDocument):
            meta = {'allow_inheritance': True}

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
                          'complex': IntegerSetting(value=42),
                          'list': [IntegerSetting(value=42),
                                   StringSetting(value='foo')]})
        e.save()

        e2 = Simple.objects.get(id=e.id)
        self.assertIsInstance(e2.mapping[0], StringSetting)
        self.assertIsInstance(e2.mapping[1], IntegerSetting)

        # Test querying
        self.assertEqual(
            Simple.objects.filter(mapping__1__value=42).count(), 1)
        self.assertEqual(
            Simple.objects.filter(mapping__2__number=1).count(), 1)
        self.assertEqual(
            Simple.objects.filter(mapping__2__complex__value=42).count(), 1)
        self.assertEqual(
            Simple.objects.filter(mapping__2__list__0__value=42).count(), 1)
        self.assertEqual(
            Simple.objects.filter(mapping__2__list__1__value='foo').count(), 1)

        # Confirm can update
        Simple.objects().update(set__mapping__1=IntegerSetting(value=10))
        self.assertEqual(
            Simple.objects.filter(mapping__1__value=10).count(), 1)

        Simple.objects().update(
            set__mapping__2__list__1=StringSetting(value='Boo'))
        self.assertEqual(
            Simple.objects.filter(mapping__2__list__1__value='foo').count(), 0)
        self.assertEqual(
            Simple.objects.filter(mapping__2__list__1__value='Boo').count(), 1)

    def test_dict_field(self):
        """Ensure that dict types work as expected."""
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

        post.info = {'nested': {'$title': 'test'}}
        self.assertRaises(ValidationError, post.validate)

        post.info = {'the.title': 'test'}
        self.assertRaises(ValidationError, post.validate)

        post.info = {'nested': {'the.title': 'test'}}
        self.assertRaises(ValidationError, post.validate)

        post.info = {1: 'test'}
        self.assertRaises(ValidationError, post.validate)

        post.info = {'title': 'test'}
        post.save()

        post = BlogPost()
        post.info = {'title': 'dollar_sign', 'details': {'te$t': 'test'}}
        post.save()

        post = BlogPost()
        post.info = {'details': {'test': 'test'}}
        post.save()

        post = BlogPost()
        post.info = {'details': {'test': 3}}
        post.save()

        self.assertEqual(BlogPost.objects.count(), 4)
        self.assertEqual(
            BlogPost.objects.filter(info__title__exact='test').count(), 1)
        self.assertEqual(
            BlogPost.objects.filter(info__details__test__exact='test').count(), 1)

        post = BlogPost.objects.filter(info__title__exact='dollar_sign').first()
        self.assertIn('te$t', post['info']['details'])

        # Confirm handles non strings or non existing keys
        self.assertEqual(
            BlogPost.objects.filter(info__details__test__exact=5).count(), 0)
        self.assertEqual(
            BlogPost.objects.filter(info__made_up__test__exact='test').count(), 0)

        post = BlogPost.objects.create(info={'title': 'original'})
        post.info.update({'title': 'updated'})
        post.save()
        post.reload()
        self.assertEqual('updated', post.info['title'])

        post.info.setdefault('authors', [])
        post.save()
        post.reload()
        self.assertEqual([], post.info['authors'])

    def test_dictfield_dump_document(self):
        """Ensure a DictField can handle another document's dump."""
        class Doc(Document):
            field = DictField()

        class ToEmbed(Document):
            id = IntField(primary_key=True, default=1)
            recursive = DictField()

        class ToEmbedParent(Document):
            id = IntField(primary_key=True, default=1)
            recursive = DictField()

            meta = {'allow_inheritance': True}

        class ToEmbedChild(ToEmbedParent):
            pass

        to_embed_recursive = ToEmbed(id=1).save()
        to_embed = ToEmbed(
            id=2, recursive=to_embed_recursive.to_mongo().to_dict()).save()
        doc = Doc(field=to_embed.to_mongo().to_dict())
        doc.save()
        assert isinstance(doc.field, dict)
        assert doc.field == {'_id': 2, 'recursive': {'_id': 1, 'recursive': {}}}
        # Same thing with a Document with a _cls field
        to_embed_recursive = ToEmbedChild(id=1).save()
        to_embed_child = ToEmbedChild(
            id=2, recursive=to_embed_recursive.to_mongo().to_dict()).save()
        doc = Doc(field=to_embed_child.to_mongo().to_dict())
        doc.save()
        assert isinstance(doc.field, dict)
        assert doc.field == {
            '_id': 2, '_cls': 'ToEmbedParent.ToEmbedChild',
            'recursive': {'_id': 1, '_cls': 'ToEmbedParent.ToEmbedChild', 'recursive': {}}
        }

    def test_dictfield_strict(self):
        """Ensure that dict field handles validation if provided a strict field type."""
        class Simple(Document):
            mapping = DictField(field=IntField())

        Simple.drop_collection()

        e = Simple()
        e.mapping['someint'] = 1
        e.save()

        # try creating an invalid mapping
        with self.assertRaises(ValidationError):
            e.mapping['somestring'] = "abc"
            e.save()

    def test_dictfield_complex(self):
        """Ensure that the dict field can handle the complex types."""
        class SettingBase(EmbeddedDocument):
            meta = {'allow_inheritance': True}

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
        e.mapping['nested_dict'] = {'number': 1, 'string': 'Hi!',
                                    'float': 1.001,
                                    'complex': IntegerSetting(value=42),
                                    'list': [IntegerSetting(value=42),
                                             StringSetting(value='foo')]}
        e.save()

        e2 = Simple.objects.get(id=e.id)
        self.assertIsInstance(e2.mapping['somestring'], StringSetting)
        self.assertIsInstance(e2.mapping['someint'], IntegerSetting)

        # Test querying
        self.assertEqual(
            Simple.objects.filter(mapping__someint__value=42).count(), 1)
        self.assertEqual(
            Simple.objects.filter(mapping__nested_dict__number=1).count(), 1)
        self.assertEqual(
            Simple.objects.filter(mapping__nested_dict__complex__value=42).count(), 1)
        self.assertEqual(
            Simple.objects.filter(mapping__nested_dict__list__0__value=42).count(), 1)
        self.assertEqual(
            Simple.objects.filter(mapping__nested_dict__list__1__value='foo').count(), 1)

        # Confirm can update
        Simple.objects().update(
            set__mapping={"someint": IntegerSetting(value=10)})
        Simple.objects().update(
            set__mapping__nested_dict__list__1=StringSetting(value='Boo'))
        self.assertEqual(
            Simple.objects.filter(mapping__nested_dict__list__1__value='foo').count(), 0)
        self.assertEqual(
            Simple.objects.filter(mapping__nested_dict__list__1__value='Boo').count(), 1)

    def test_atomic_update_dict_field(self):
        """Ensure that the entire DictField can be atomically updated."""
        class Simple(Document):
            mapping = DictField(field=ListField(IntField(required=True)))

        Simple.drop_collection()

        e = Simple()
        e.mapping['someints'] = [1, 2]
        e.save()
        e.update(set__mapping={"ints": [3, 4]})
        e.reload()
        self.assertEqual(BaseDict, type(e.mapping))
        self.assertEqual({"ints": [3, 4]}, e.mapping)

        # try creating an invalid mapping
        with self.assertRaises(ValueError):
            e.update(set__mapping={"somestrings": ["foo", "bar", ]})

    def test_dictfield_with_referencefield_complex_nesting_cases(self):
        """Ensure complex nesting inside DictField handles dereferencing of ReferenceField(dbref=True | False)"""
        # Relates to Issue #1453
        class Doc(Document):
            s = StringField()

        class Simple(Document):
            mapping0 = DictField(ReferenceField(Doc, dbref=True))
            mapping1 = DictField(ReferenceField(Doc, dbref=False))
            mapping2 = DictField(ListField(ReferenceField(Doc, dbref=True)))
            mapping3 = DictField(ListField(ReferenceField(Doc, dbref=False)))
            mapping4 = DictField(DictField(field=ReferenceField(Doc, dbref=True)))
            mapping5 = DictField(DictField(field=ReferenceField(Doc, dbref=False)))
            mapping6 = DictField(ListField(DictField(ReferenceField(Doc, dbref=True))))
            mapping7 = DictField(ListField(DictField(ReferenceField(Doc, dbref=False))))
            mapping8 = DictField(ListField(DictField(ListField(ReferenceField(Doc, dbref=True)))))
            mapping9 = DictField(ListField(DictField(ListField(ReferenceField(Doc, dbref=False)))))

        Doc.drop_collection()
        Simple.drop_collection()

        d = Doc(s='aa').save()
        e = Simple()
        e.mapping0['someint'] = e.mapping1['someint'] = d
        e.mapping2['someint'] = e.mapping3['someint'] = [d]
        e.mapping4['someint'] = e.mapping5['someint'] = {'d': d}
        e.mapping6['someint'] = e.mapping7['someint'] = [{'d': d}]
        e.mapping8['someint'] = e.mapping9['someint'] = [{'d': [d]}]
        e.save()

        s = Simple.objects.first()
        self.assertIsInstance(s.mapping0['someint'], Doc)
        self.assertIsInstance(s.mapping1['someint'], Doc)
        self.assertIsInstance(s.mapping2['someint'][0], Doc)
        self.assertIsInstance(s.mapping3['someint'][0], Doc)
        self.assertIsInstance(s.mapping4['someint']['d'], Doc)
        self.assertIsInstance(s.mapping5['someint']['d'], Doc)
        self.assertIsInstance(s.mapping6['someint'][0]['d'], Doc)
        self.assertIsInstance(s.mapping7['someint'][0]['d'], Doc)
        self.assertIsInstance(s.mapping8['someint'][0]['d'][0], Doc)
        self.assertIsInstance(s.mapping9['someint'][0]['d'][0], Doc)

    def test_mapfield(self):
        """Ensure that the MapField handles the declared type."""
        class Simple(Document):
            mapping = MapField(IntField())

        Simple.drop_collection()

        e = Simple()
        e.mapping['someint'] = 1
        e.save()

        with self.assertRaises(ValidationError):
            e.mapping['somestring'] = "abc"
            e.save()

        with self.assertRaises(ValidationError):
            class NoDeclaredType(Document):
                mapping = MapField()

    def test_complex_mapfield(self):
        """Ensure that the MapField can handle complex declared types."""
        class SettingBase(EmbeddedDocument):
            meta = {"allow_inheritance": True}

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
        self.assertIsInstance(e2.mapping['somestring'], StringSetting)
        self.assertIsInstance(e2.mapping['someint'], IntegerSetting)

        with self.assertRaises(ValidationError):
            e.mapping['someint'] = 123
            e.save()

    def test_embedded_mapfield_db_field(self):
        class Embedded(EmbeddedDocument):
            number = IntField(default=0, db_field='i')

        class Test(Document):
            my_map = MapField(field=EmbeddedDocumentField(Embedded),
                              db_field='x')

        Test.drop_collection()

        test = Test()
        test.my_map['DICTIONARY_KEY'] = Embedded(number=1)
        test.save()

        Test.objects.update_one(inc__my_map__DICTIONARY_KEY__number=1)

        test = Test.objects.get()
        self.assertEqual(test.my_map['DICTIONARY_KEY'].number, 2)
        doc = self.db.test.find_one()
        self.assertEqual(doc['x']['DICTIONARY_KEY']['i'], 2)

    def test_mapfield_numerical_index(self):
        """Ensure that MapField accept numeric strings as indexes."""
        class Embedded(EmbeddedDocument):
            name = StringField()

        class Test(Document):
            my_map = MapField(EmbeddedDocumentField(Embedded))

        Test.drop_collection()

        test = Test()
        test.my_map['1'] = Embedded(name='test')
        test.save()
        test.my_map['1'].name = 'test updated'
        test.save()

    def test_map_field_lookup(self):
        """Ensure MapField lookups succeed on Fields without a lookup
        method.
        """
        class Action(EmbeddedDocument):
            operation = StringField()
            object = StringField()

        class Log(Document):
            name = StringField()
            visited = MapField(DateTimeField())
            actions = MapField(EmbeddedDocumentField(Action))

        Log.drop_collection()
        Log(name="wilson", visited={'friends': datetime.datetime.now()},
            actions={'friends': Action(operation='drink', object='beer')}).save()

        self.assertEqual(1, Log.objects(
            visited__friends__exists=True).count())

        self.assertEqual(1, Log.objects(
            actions__friends__operation='drink',
            actions__friends__object='beer').count())

    def test_map_field_unicode(self):
        class Info(EmbeddedDocument):
            description = StringField()
            value_list = ListField(field=StringField())

        class BlogPost(Document):
            info_dict = MapField(field=EmbeddedDocumentField(Info))

        BlogPost.drop_collection()

        tree = BlogPost(info_dict={
            u"éééé": {
                'description': u"VALUE: éééé"
            }
        })

        tree.save()

        self.assertEqual(
            BlogPost.objects.get(id=tree.id).info_dict[u"éééé"].description,
            u"VALUE: éééé"
        )

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

    def test_double_embedded_db_field(self):
        """Make sure multiple layers of embedded docs resolve db fields
        properly and can be initialized using dicts.
        """
        class C(EmbeddedDocument):
            txt = StringField()

        class B(EmbeddedDocument):
            c = EmbeddedDocumentField(C, db_field='fc')

        class A(Document):
            b = EmbeddedDocumentField(B, db_field='fb')

        a = A(
            b=B(
                c=C(txt='hi')
            )
        )
        a.validate()

        a = A(b={'c': {'txt': 'hi'}})
        a.validate()

    def test_double_embedded_db_field_from_son(self):
        """Make sure multiple layers of embedded docs resolve db fields
        from SON properly.
        """
        class C(EmbeddedDocument):
            txt = StringField()

        class B(EmbeddedDocument):
            c = EmbeddedDocumentField(C, db_field='fc')

        class A(Document):
            b = EmbeddedDocumentField(B, db_field='fb')

        a = A._from_son(SON([
            ('fb', SON([
                ('fc', SON([
                    ('txt', 'hi')
                ]))
            ]))
        ]))
        self.assertEqual(a.b.c.txt, 'hi')

    def test_embedded_document_field_cant_reference_using_a_str_if_it_does_not_exist_yet(self):
        raise SkipTest("Using a string reference in an EmbeddedDocumentField does not work if the class isnt registerd yet")

        class MyDoc2(Document):
            emb = EmbeddedDocumentField('MyDoc')

        class MyDoc(EmbeddedDocument):
            name = StringField()

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

        Person.drop_collection()

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
        """Ensure that subclasses of embedded documents may be provided
        to EmbeddedDocumentFields of the superclass' type.
        """
        class User(EmbeddedDocument):
            name = StringField()

            meta = {'allow_inheritance': True}

        class PowerUser(User):
            power = IntField()

        class BlogPost(Document):
            content = StringField()
            author = EmbeddedDocumentField(User)

        BlogPost.drop_collection()

        post = BlogPost(content='What I did today...')
        post.author = PowerUser(name='Test User', power=47)
        post.save()

        self.assertEqual(47, BlogPost.objects.first().author.power)

    def test_embedded_document_inheritance_with_list(self):
        """Ensure that nested list of subclassed embedded documents is
        handled correctly.
        """
        class Group(EmbeddedDocument):
            name = StringField()
            content = ListField(StringField())

        class Basedoc(Document):
            groups = ListField(EmbeddedDocumentField(Group))
            meta = {'abstract': True}

        class User(Basedoc):
            doctype = StringField(require=True, default='userdata')

        User.drop_collection()

        content = ['la', 'le', 'lu']
        group = Group(name='foo', content=content)
        foobar = User(groups=[group])
        foobar.save()

        self.assertEqual(content, User.objects.first().groups[0].content)

    def test_reference_miss(self):
        """Ensure an exception is raised when dereferencing an unknown
        document.
        """
        class Foo(Document):
            pass

        class Bar(Document):
            ref = ReferenceField(Foo)
            generic_ref = GenericReferenceField()

        Foo.drop_collection()
        Bar.drop_collection()

        foo = Foo().save()
        bar = Bar(ref=foo, generic_ref=foo).save()

        # Reference is no longer valid
        foo.delete()
        bar = Bar.objects.get()
        self.assertRaises(DoesNotExist, getattr, bar, 'ref')
        self.assertRaises(DoesNotExist, getattr, bar, 'generic_ref')

        # When auto_dereference is disabled, there is no trouble returning DBRef
        bar = Bar.objects.get()
        expected = foo.to_dbref()
        bar._fields['ref']._auto_dereference = False
        self.assertEqual(bar.ref, expected)
        bar._fields['generic_ref']._auto_dereference = False
        self.assertEqual(bar.generic_ref, {'_ref': expected, '_cls': 'Foo'})

    def test_reference_validation(self):
        """Ensure that invalid document objects cannot be assigned to
        reference fields.
        """
        class User(Document):
            name = StringField()

        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(User)

        User.drop_collection()
        BlogPost.drop_collection()

        # Make sure ReferenceField only accepts a document class or a string
        # with a document class name.
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

        # Ensure ObjectID's are accepted as references
        user_object_id = user.pk
        post3 = BlogPost(content="Chips and curry sauce taste good.")
        post3.author = user_object_id
        post3.save()

        # Make sure referencing a saved document of the right type works
        user.save()
        post1.author = user
        post1.save()

        # Make sure referencing a saved document of the *wrong* type fails
        post2.save()
        post1.author = post2
        self.assertRaises(ValidationError, post1.validate)

    def test_objectid_reference_fields(self):
        """Make sure storing Object ID references works."""
        class Person(Document):
            name = StringField()
            parent = ReferenceField('self')

        Person.drop_collection()

        p1 = Person(name="John").save()
        Person(name="Ross", parent=p1.pk).save()

        p = Person.objects.get(name="Ross")
        self.assertEqual(p.parent, p1)

    def test_dbref_reference_fields(self):
        """Make sure storing references as bson.dbref.DBRef works."""
        class Person(Document):
            name = StringField()
            parent = ReferenceField('self', dbref=True)

        Person.drop_collection()

        p1 = Person(name="John").save()
        Person(name="Ross", parent=p1).save()

        self.assertEqual(
            Person._get_collection().find_one({'name': 'Ross'})['parent'],
            DBRef('person', p1.pk)
        )

        p = Person.objects.get(name="Ross")
        self.assertEqual(p.parent, p1)

    def test_dbref_to_mongo(self):
        """Make sure that calling to_mongo on a ReferenceField which
        has dbref=False, but actually actually contains a DBRef returns
        an ID of that DBRef.
        """
        class Person(Document):
            name = StringField()
            parent = ReferenceField('self', dbref=False)

        p = Person(
            name='Steve',
            parent=DBRef('person', 'abcdefghijklmnop')
        )
        self.assertEqual(p.to_mongo(), SON([
            ('name', u'Steve'),
            ('parent', 'abcdefghijklmnop')
        ]))

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
        class TreeNode(EmbeddedDocument):
            name = StringField()
            children = ListField(EmbeddedDocumentField('self'))

        class Tree(Document):
            name = StringField()
            children = ListField(EmbeddedDocumentField('TreeNode'))

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

        obj = Product.objects.get(company=None)
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

    def test_drop_abstract_document(self):
        """Ensure that an abstract document cannot be dropped given it
        has no underlying collection.
        """
        class AbstractDoc(Document):
            name = StringField()
            meta = {"abstract": True}

        self.assertRaises(OperationError, AbstractDoc.drop_collection)

    def test_reference_class_with_abstract_parent(self):
        """Ensure that a class with an abstract parent can be referenced.
        """
        class Sibling(Document):
            name = StringField()
            meta = {"abstract": True}

        class Sister(Sibling):
            pass

        class Brother(Sibling):
            sibling = ReferenceField(Sibling)

        Sister.drop_collection()
        Brother.drop_collection()

        sister = Sister(name="Alice")
        sister.save()
        brother = Brother(name="Bob", sibling=sister)
        brother.save()

        self.assertEquals(Brother.objects[0].sibling.name, sister.name)

    def test_reference_abstract_class(self):
        """Ensure that an abstract class instance cannot be used in the
        reference of that abstract class.
        """
        class Sibling(Document):
            name = StringField()
            meta = {"abstract": True}

        class Sister(Sibling):
            pass

        class Brother(Sibling):
            sibling = ReferenceField(Sibling)

        Sister.drop_collection()
        Brother.drop_collection()

        sister = Sibling(name="Alice")
        brother = Brother(name="Bob", sibling=sister)
        self.assertRaises(ValidationError, brother.save)

    def test_abstract_reference_base_type(self):
        """Ensure that an an abstract reference fails validation when given a
        Document that does not inherit from the abstract type.
        """
        class Sibling(Document):
            name = StringField()
            meta = {"abstract": True}

        class Brother(Sibling):
            sibling = ReferenceField(Sibling)

        class Mother(Document):
            name = StringField()

        Brother.drop_collection()
        Mother.drop_collection()

        mother = Mother(name="Carol")
        mother.save()
        brother = Brother(name="Bob", sibling=mother)
        self.assertRaises(ValidationError, brother.save)

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
        self.assertIsInstance(bm.bookmark_object, Post)

        bm.bookmark_object = link_1
        bm.save()

        bm = Bookmark.objects(bookmark_object=link_1).first()

        self.assertEqual(bm.bookmark_object, link_1)
        self.assertIsInstance(bm.bookmark_object, Link)

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

    def test_generic_reference_is_none(self):

        class Person(Document):
            name = StringField()
            city = GenericReferenceField()

        Person.drop_collection()

        Person(name="Wilson Jr").save()
        self.assertEqual(repr(Person.objects(city=None)),
                         "[<Person: Person object>]")

    def test_generic_reference_choices(self):
        """Ensure that a GenericReferenceField can handle choices."""
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

    def test_generic_reference_string_choices(self):
        """Ensure that a GenericReferenceField can handle choices as strings
        """
        class Link(Document):
            title = StringField()

        class Post(Document):
            title = StringField()

        class Bookmark(Document):
            bookmark_object = GenericReferenceField(choices=('Post', Link))

        Link.drop_collection()
        Post.drop_collection()
        Bookmark.drop_collection()

        link_1 = Link(title="Pitchfork")
        link_1.save()

        post_1 = Post(title="Behind the Scenes of the Pavement Reunion")
        post_1.save()

        bm = Bookmark(bookmark_object=link_1)
        bm.save()

        bm = Bookmark(bookmark_object=post_1)
        bm.save()

        bm = Bookmark(bookmark_object=bm)
        self.assertRaises(ValidationError, bm.validate)

    def test_generic_reference_choices_no_dereference(self):
        """Ensure that a GenericReferenceField can handle choices on
        non-derefenreced (i.e. DBRef) elements
        """
        class Post(Document):
            title = StringField()

        class Bookmark(Document):
            bookmark_object = GenericReferenceField(choices=(Post, ))
            other_field = StringField()

        Post.drop_collection()
        Bookmark.drop_collection()

        post_1 = Post(title="Behind the Scenes of the Pavement Reunion")
        post_1.save()

        bm = Bookmark(bookmark_object=post_1)
        bm.save()

        bm = Bookmark.objects.get(id=bm.id)
        # bookmark_object is now a DBRef
        bm.other_field = 'dummy_change'
        bm.save()

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

    def test_generic_reference_list_item_modification(self):
        """Ensure that modifications of related documents (through generic reference) don't influence on querying
        """
        class Post(Document):
            title = StringField()

        class User(Document):
            username = StringField()
            bookmarks = ListField(GenericReferenceField())

        Post.drop_collection()
        User.drop_collection()

        post_1 = Post(title="Behind the Scenes of the Pavement Reunion")
        post_1.save()

        user = User(bookmarks=[post_1])
        user.save()

        post_1.title = "Title was modified"
        user.username = "New username"
        user.save()

        user = User.objects(bookmarks__all=[post_1]).first()

        self.assertNotEqual(user, None)
        self.assertEqual(user.bookmarks[0], post_1)

    def test_generic_reference_filter_by_dbref(self):
        """Ensure we can search for a specific generic reference by
        providing its ObjectId.
        """
        class Doc(Document):
            ref = GenericReferenceField()

        Doc.drop_collection()

        doc1 = Doc.objects.create()
        doc2 = Doc.objects.create(ref=doc1)

        doc = Doc.objects.get(ref=DBRef('doc', doc1.pk))
        self.assertEqual(doc, doc2)

    def test_generic_reference_is_not_tracked_in_parent_doc(self):
        """Ensure that modifications of related documents (through generic reference) don't influence
        the owner changed fields (#1934)
        """
        class Doc1(Document):
            name = StringField()

        class Doc2(Document):
            ref = GenericReferenceField()
            refs = ListField(GenericReferenceField())

        Doc1.drop_collection()
        Doc2.drop_collection()

        doc1 = Doc1(name='garbage1').save()
        doc11 = Doc1(name='garbage11').save()
        doc2 = Doc2(ref=doc1, refs=[doc11]).save()

        doc2.ref.name = 'garbage2'
        self.assertEqual(doc2._get_changed_fields(), [])

        doc2.refs[0].name = 'garbage3'
        self.assertEqual(doc2._get_changed_fields(), [])
        self.assertEqual(doc2._delta(), ({}, {}))

    def test_generic_reference_field(self):
        """Ensure we can search for a specific generic reference by
        providing its DBRef.
        """
        class Doc(Document):
            ref = GenericReferenceField()

        Doc.drop_collection()

        doc1 = Doc.objects.create()
        doc2 = Doc.objects.create(ref=doc1)

        self.assertIsInstance(doc1.pk, ObjectId)

        doc = Doc.objects.get(ref=doc1.pk)
        self.assertEqual(doc, doc2)

    def test_choices_allow_using_sets_as_choices(self):
        """Ensure that sets can be used when setting choices
        """
        class Shirt(Document):
            size = StringField(choices={'M', 'L'})

        Shirt(size='M').validate()

    def test_choices_validation_allow_no_value(self):
        """Ensure that .validate passes and no value was provided
        for a field setup with choices
        """
        class Shirt(Document):
            size = StringField(choices=('S', 'M'))

        shirt = Shirt()
        shirt.validate()

    def test_choices_validation_accept_possible_value(self):
        """Ensure that value is in a container of allowed values.
        """
        class Shirt(Document):
            size = StringField(choices=('S', 'M'))

        shirt = Shirt(size='S')
        shirt.validate()

    def test_choices_validation_reject_unknown_value(self):
        """Ensure that unallowed value are rejected upon validation
        """
        class Shirt(Document):
            size = StringField(choices=('S', 'M'))

        shirt = Shirt(size="XS")
        with self.assertRaises(ValidationError):
            shirt.validate()

    def test_choices_validation_documents(self):
        """
        Ensure fields with document choices validate given a valid choice.
        """
        class UserComments(EmbeddedDocument):
            author = StringField()
            message = StringField()

        class BlogPost(Document):
            comments = ListField(
                GenericEmbeddedDocumentField(choices=(UserComments,))
            )

        # Ensure Validation Passes
        BlogPost(comments=[
            UserComments(author='user2', message='message2'),
        ]).save()

    def test_choices_validation_documents_invalid(self):
        """
        Ensure fields with document choices validate given an invalid choice.
        This should throw a ValidationError exception.
        """
        class UserComments(EmbeddedDocument):
            author = StringField()
            message = StringField()

        class ModeratorComments(EmbeddedDocument):
            author = StringField()
            message = StringField()

        class BlogPost(Document):
            comments = ListField(
                GenericEmbeddedDocumentField(choices=(UserComments,))
            )

        # Single Entry Failure
        post = BlogPost(comments=[
            ModeratorComments(author='mod1', message='message1'),
        ])
        self.assertRaises(ValidationError, post.save)

        # Mixed Entry Failure
        post = BlogPost(comments=[
            ModeratorComments(author='mod1', message='message1'),
            UserComments(author='user2', message='message2'),
        ])
        self.assertRaises(ValidationError, post.save)

    def test_choices_validation_documents_inheritance(self):
        """
        Ensure fields with document choices validate given subclass of choice.
        """
        class Comments(EmbeddedDocument):
            meta = {
                'abstract': True
            }
            author = StringField()
            message = StringField()

        class UserComments(Comments):
            pass

        class BlogPost(Document):
            comments = ListField(
                GenericEmbeddedDocumentField(choices=(Comments,))
            )

        # Save Valid EmbeddedDocument Type
        BlogPost(comments=[
            UserComments(author='user2', message='message2'),
        ]).save()

    def test_choices_get_field_display(self):
        """Test dynamic helper for returning the display value of a choices
        field.
        """
        class Shirt(Document):
            size = StringField(max_length=3, choices=(
                ('S', 'Small'), ('M', 'Medium'), ('L', 'Large'),
                ('XL', 'Extra Large'), ('XXL', 'Extra Extra Large')))
            style = StringField(max_length=3, choices=(
                ('S', 'Small'), ('B', 'Baggy'), ('W', 'Wide')), default='W')

        Shirt.drop_collection()

        shirt1 = Shirt()
        shirt2 = Shirt()

        # Make sure get_<field>_display returns the default value (or None)
        self.assertEqual(shirt1.get_size_display(), None)
        self.assertEqual(shirt1.get_style_display(), 'Wide')

        shirt1.size = 'XXL'
        shirt1.style = 'B'
        shirt2.size = 'M'
        shirt2.style = 'S'
        self.assertEqual(shirt1.get_size_display(), 'Extra Extra Large')
        self.assertEqual(shirt1.get_style_display(), 'Baggy')
        self.assertEqual(shirt2.get_size_display(), 'Medium')
        self.assertEqual(shirt2.get_style_display(), 'Small')

        # Set as Z - an invalid choice
        shirt1.size = 'Z'
        shirt1.style = 'Z'
        self.assertEqual(shirt1.get_size_display(), 'Z')
        self.assertEqual(shirt1.get_style_display(), 'Z')
        self.assertRaises(ValidationError, shirt1.validate)

    def test_simple_choices_validation(self):
        """Ensure that value is in a container of allowed values.
        """
        class Shirt(Document):
            size = StringField(max_length=3,
                               choices=('S', 'M', 'L', 'XL', 'XXL'))

        Shirt.drop_collection()

        shirt = Shirt()
        shirt.validate()

        shirt.size = "S"
        shirt.validate()

        shirt.size = "XS"
        self.assertRaises(ValidationError, shirt.validate)

    def test_simple_choices_get_field_display(self):
        """Test dynamic helper for returning the display value of a choices
        field.
        """
        class Shirt(Document):
            size = StringField(max_length=3,
                               choices=('S', 'M', 'L', 'XL', 'XXL'))
            style = StringField(max_length=3,
                                choices=('Small', 'Baggy', 'wide'),
                                default='Small')

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

    def test_simple_choices_validation_invalid_value(self):
        """Ensure that error messages are correct.
        """
        SIZES = ('S', 'M', 'L', 'XL', 'XXL')
        COLORS = (('R', 'Red'), ('B', 'Blue'))
        SIZE_MESSAGE = u"Value must be one of ('S', 'M', 'L', 'XL', 'XXL')"
        COLOR_MESSAGE = u"Value must be one of ['R', 'B']"

        class Shirt(Document):
            size = StringField(max_length=3, choices=SIZES)
            color = StringField(max_length=1, choices=COLORS)

        Shirt.drop_collection()

        shirt = Shirt()
        shirt.validate()

        shirt.size = "S"
        shirt.color = "R"
        shirt.validate()

        shirt.size = "XS"
        shirt.color = "G"

        try:
            shirt.validate()
        except ValidationError as error:
            # get the validation rules
            error_dict = error.to_dict()
            self.assertEqual(error_dict['size'], SIZE_MESSAGE)
            self.assertEqual(error_dict['color'], COLOR_MESSAGE)

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

        for x in range(10):
            Person(name="Person %s" % x).save()

        c = self.db['mongoengine.counters'].find_one({'_id': 'person.id'})
        self.assertEqual(c['next'], 10)

        ids = [i.id for i in Person.objects]
        self.assertEqual(ids, range(1, 11))

        c = self.db['mongoengine.counters'].find_one({'_id': 'person.id'})
        self.assertEqual(c['next'], 10)

        Person.id.set_next_value(1000)
        c = self.db['mongoengine.counters'].find_one({'_id': 'person.id'})
        self.assertEqual(c['next'], 1000)

    def test_sequence_field_get_next_value(self):
        class Person(Document):
            id = SequenceField(primary_key=True)
            name = StringField()

        self.db['mongoengine.counters'].drop()
        Person.drop_collection()

        for x in range(10):
            Person(name="Person %s" % x).save()

        self.assertEqual(Person.id.get_next_value(), 11)
        self.db['mongoengine.counters'].drop()

        self.assertEqual(Person.id.get_next_value(), 1)

        class Person(Document):
            id = SequenceField(primary_key=True, value_decorator=str)
            name = StringField()

        self.db['mongoengine.counters'].drop()
        Person.drop_collection()

        for x in range(10):
            Person(name="Person %s" % x).save()

        self.assertEqual(Person.id.get_next_value(), '11')
        self.db['mongoengine.counters'].drop()

        self.assertEqual(Person.id.get_next_value(), '1')

    def test_sequence_field_sequence_name(self):
        class Person(Document):
            id = SequenceField(primary_key=True, sequence_name='jelly')
            name = StringField()

        self.db['mongoengine.counters'].drop()
        Person.drop_collection()

        for x in range(10):
            Person(name="Person %s" % x).save()

        c = self.db['mongoengine.counters'].find_one({'_id': 'jelly.id'})
        self.assertEqual(c['next'], 10)

        ids = [i.id for i in Person.objects]
        self.assertEqual(ids, range(1, 11))

        c = self.db['mongoengine.counters'].find_one({'_id': 'jelly.id'})
        self.assertEqual(c['next'], 10)

        Person.id.set_next_value(1000)
        c = self.db['mongoengine.counters'].find_one({'_id': 'jelly.id'})
        self.assertEqual(c['next'], 1000)

    def test_multiple_sequence_fields(self):
        class Person(Document):
            id = SequenceField(primary_key=True)
            counter = SequenceField()
            name = StringField()

        self.db['mongoengine.counters'].drop()
        Person.drop_collection()

        for x in range(10):
            Person(name="Person %s" % x).save()

        c = self.db['mongoengine.counters'].find_one({'_id': 'person.id'})
        self.assertEqual(c['next'], 10)

        ids = [i.id for i in Person.objects]
        self.assertEqual(ids, range(1, 11))

        counters = [i.counter for i in Person.objects]
        self.assertEqual(counters, range(1, 11))

        c = self.db['mongoengine.counters'].find_one({'_id': 'person.id'})
        self.assertEqual(c['next'], 10)

        Person.id.set_next_value(1000)
        c = self.db['mongoengine.counters'].find_one({'_id': 'person.id'})
        self.assertEqual(c['next'], 1000)

        Person.counter.set_next_value(999)
        c = self.db['mongoengine.counters'].find_one({'_id': 'person.counter'})
        self.assertEqual(c['next'], 999)

    def test_sequence_fields_reload(self):
        class Animal(Document):
            counter = SequenceField()
            name = StringField()

        self.db['mongoengine.counters'].drop()
        Animal.drop_collection()

        a = Animal(name="Boi").save()

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
            name = StringField()

        class Person(Document):
            id = SequenceField(primary_key=True)
            name = StringField()

        self.db['mongoengine.counters'].drop()
        Animal.drop_collection()
        Person.drop_collection()

        for x in range(10):
            Animal(name="Animal %s" % x).save()
            Person(name="Person %s" % x).save()

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

    def test_sequence_field_value_decorator(self):
        class Person(Document):
            id = SequenceField(primary_key=True, value_decorator=str)
            name = StringField()

        self.db['mongoengine.counters'].drop()
        Person.drop_collection()

        for x in range(10):
            p = Person(name="Person %s" % x)
            p.save()

        c = self.db['mongoengine.counters'].find_one({'_id': 'person.id'})
        self.assertEqual(c['next'], 10)

        ids = [i.id for i in Person.objects]
        self.assertEqual(ids, map(str, range(1, 11)))

        c = self.db['mongoengine.counters'].find_one({'_id': 'person.id'})
        self.assertEqual(c['next'], 10)

    def test_embedded_sequence_field(self):
        class Comment(EmbeddedDocument):
            id = SequenceField()
            content = StringField(required=True)

        class Post(Document):
            title = StringField(required=True)
            comments = ListField(EmbeddedDocumentField(Comment))

        self.db['mongoengine.counters'].drop()
        Post.drop_collection()

        Post(title="MongoEngine",
             comments=[Comment(content="NoSQL Rocks"),
                       Comment(content="MongoEngine Rocks")]).save()
        c = self.db['mongoengine.counters'].find_one({'_id': 'comment.id'})
        self.assertEqual(c['next'], 2)
        post = Post.objects.first()
        self.assertEqual(1, post.comments[0].id)
        self.assertEqual(2, post.comments[1].id)

    def test_inherited_sequencefield(self):
        class Base(Document):
            name = StringField()
            counter = SequenceField()
            meta = {'abstract': True}

        class Foo(Base):
            pass

        class Bar(Base):
            pass

        bar = Bar(name='Bar')
        bar.save()

        foo = Foo(name='Foo')
        foo.save()

        self.assertTrue('base.counter' in
                        self.db['mongoengine.counters'].find().distinct('_id'))
        self.assertFalse(('foo.counter' or 'bar.counter') in
                         self.db['mongoengine.counters'].find().distinct('_id'))
        self.assertNotEqual(foo.counter, bar.counter)
        self.assertEqual(foo._fields['counter'].owner_document, Base)
        self.assertEqual(bar._fields['counter'].owner_document, Base)

    def test_no_inherited_sequencefield(self):
        class Base(Document):
            name = StringField()
            meta = {'abstract': True}

        class Foo(Base):
            counter = SequenceField()

        class Bar(Base):
            counter = SequenceField()

        bar = Bar(name='Bar')
        bar.save()

        foo = Foo(name='Foo')
        foo.save()

        self.assertFalse('base.counter' in
                         self.db['mongoengine.counters'].find().distinct('_id'))
        self.assertTrue(('foo.counter' and 'bar.counter') in
                         self.db['mongoengine.counters'].find().distinct('_id'))
        self.assertEqual(foo.counter, bar.counter)
        self.assertEqual(foo._fields['counter'].owner_document, Foo)
        self.assertEqual(bar._fields['counter'].owner_document, Bar)

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
        self.assertIsInstance(person.like, Car)

        person.like = Dish(food="arroz", number=15)
        person.save()

        person = Person.objects.first()
        self.assertIsInstance(person.like, Dish)

    def test_generic_embedded_document_choices(self):
        """Ensure you can limit GenericEmbeddedDocument choices."""
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
        self.assertIsInstance(person.like, Dish)

    def test_generic_list_embedded_document_choices(self):
        """Ensure you can limit GenericEmbeddedDocument choices inside
        a list field.
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
        self.assertIsInstance(person.likes[0], Dish)

    def test_recursive_validation(self):
        """Ensure that a validation result to_dict is available."""
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
        except ValidationError as error:
            # ValidationError.errors property
            self.assertTrue(hasattr(error, 'errors'))
            self.assertIsInstance(error.errors, dict)
            self.assertIn('comments', error.errors)
            self.assertIn(1, error.errors['comments'])
            self.assertIsInstance(error.errors['comments'][1]['content'], ValidationError)

            # ValidationError.schema property
            error_dict = error.to_dict()
            self.assertIsInstance(error_dict, dict)
            self.assertIn('comments', error_dict)
            self.assertIn(1, error_dict['comments'])
            self.assertIn('content', error_dict['comments'][1])
            self.assertEqual(error_dict['comments'][1]['content'],
                             u'Field is required')

        post.comments[1].content = 'here we go'
        post.validate()

    def test_email_field(self):
        class User(Document):
            email = EmailField()

        user = User(email='ross@example.com')
        user.validate()

        user = User(email='ross@example.co.uk')
        user.validate()

        user = User(email=('Kofq@rhom0e4klgauOhpbpNdogawnyIKvQS0wk2mjqrgGQ5S'
                           'aJIazqqWkm7.net'))
        user.validate()

        user = User(email='new-tld@example.technology')
        user.validate()

        user = User(email='ross@example.com.')
        self.assertRaises(ValidationError, user.validate)

        # unicode domain
        user = User(email=u'user@пример.рф')
        user.validate()

        # invalid unicode domain
        user = User(email=u'user@пример')
        self.assertRaises(ValidationError, user.validate)

        # invalid data type
        user = User(email=123)
        self.assertRaises(ValidationError, user.validate)

    def test_email_field_unicode_user(self):
        # Don't run this test on pypy3, which doesn't support unicode regex:
        # https://bitbucket.org/pypy/pypy/issues/1821/regular-expression-doesnt-find-unicode
        if sys.version_info[:2] == (3, 2):
            raise SkipTest('unicode email addresses are not supported on PyPy 3')

        class User(Document):
            email = EmailField()

        # unicode user shouldn't validate by default...
        user = User(email=u'Dörte@Sörensen.example.com')
        self.assertRaises(ValidationError, user.validate)

        # ...but it should be fine with allow_utf8_user set to True
        class User(Document):
            email = EmailField(allow_utf8_user=True)

        user = User(email=u'Dörte@Sörensen.example.com')
        user.validate()

    def test_email_field_domain_whitelist(self):
        class User(Document):
            email = EmailField()

        # localhost domain shouldn't validate by default...
        user = User(email='me@localhost')
        self.assertRaises(ValidationError, user.validate)

        # ...but it should be fine if it's whitelisted
        class User(Document):
            email = EmailField(domain_whitelist=['localhost'])

        user = User(email='me@localhost')
        user.validate()

    def test_email_field_ip_domain(self):
        class User(Document):
            email = EmailField()

        valid_ipv4 = 'email@[127.0.0.1]'
        valid_ipv6 = 'email@[2001:dB8::1]'
        invalid_ip = 'email@[324.0.0.1]'

        # IP address as a domain shouldn't validate by default...
        user = User(email=valid_ipv4)
        self.assertRaises(ValidationError, user.validate)

        user = User(email=valid_ipv6)
        self.assertRaises(ValidationError, user.validate)

        user = User(email=invalid_ip)
        self.assertRaises(ValidationError, user.validate)

        # ...but it should be fine with allow_ip_domain set to True
        class User(Document):
            email = EmailField(allow_ip_domain=True)

        user = User(email=valid_ipv4)
        user.validate()

        user = User(email=valid_ipv6)
        user.validate()

        # invalid IP should still fail validation
        user = User(email=invalid_ip)
        self.assertRaises(ValidationError, user.validate)

    def test_email_field_honors_regex(self):
        class User(Document):
            email = EmailField(regex=r'\w+@example.com')

        # Fails regex validation
        user = User(email='me@foo.com')
        self.assertRaises(ValidationError, user.validate)

        # Passes regex validation
        user = User(email='me@example.com')
        self.assertIsNone(user.validate())

    def test_tuples_as_tuples(self):
        """Ensure that tuples remain tuples when they are inside
        a ComplexBaseField.
        """
        class EnumField(BaseField):

            def __init__(self, **kwargs):
                super(EnumField, self).__init__(**kwargs)

            def to_mongo(self, value):
                return value

            def to_python(self, value):
                return tuple(value)

        class TestDoc(Document):
            items = ListField(EnumField())

        TestDoc.drop_collection()

        tuples = [(100, 'Testing')]
        doc = TestDoc()
        doc.items = tuples
        doc.save()
        x = TestDoc.objects().get()
        self.assertIsNotNone(x)
        self.assertEqual(len(x.items), 1)
        self.assertIn(tuple(x.items[0]), tuples)
        self.assertIn(x.items[0], tuples)

    def test_dynamic_fields_class(self):
        class Doc2(Document):
            field_1 = StringField(db_field='f')

        class Doc(Document):
            my_id = IntField(required=True, unique=True, primary_key=True)
            embed_me = DynamicField(db_field='e')
            field_x = StringField(db_field='x')

        Doc.drop_collection()
        Doc2.drop_collection()

        doc2 = Doc2(field_1="hello")
        doc = Doc(my_id=1, embed_me=doc2, field_x="x")
        self.assertRaises(OperationError, doc.save)

        doc2.save()
        doc.save()

        doc = Doc.objects.get()
        self.assertEqual(doc.embed_me.field_1, "hello")

    def test_dynamic_fields_embedded_class(self):
        class Embed(EmbeddedDocument):
            field_1 = StringField(db_field='f')

        class Doc(Document):
            my_id = IntField(required=True, unique=True, primary_key=True)
            embed_me = DynamicField(db_field='e')
            field_x = StringField(db_field='x')

        Doc.drop_collection()

        Doc(my_id=1, embed_me=Embed(field_1="hello"), field_x="x").save()

        doc = Doc.objects.get()
        self.assertEqual(doc.embed_me.field_1, "hello")

    def test_dynamicfield_dump_document(self):
        """Ensure a DynamicField can handle another document's dump."""
        class Doc(Document):
            field = DynamicField()

        class ToEmbed(Document):
            id = IntField(primary_key=True, default=1)
            recursive = DynamicField()

        class ToEmbedParent(Document):
            id = IntField(primary_key=True, default=1)
            recursive = DynamicField()

            meta = {'allow_inheritance': True}

        class ToEmbedChild(ToEmbedParent):
            pass

        to_embed_recursive = ToEmbed(id=1).save()
        to_embed = ToEmbed(id=2, recursive=to_embed_recursive).save()
        doc = Doc(field=to_embed)
        doc.save()
        assert isinstance(doc.field, ToEmbed)
        assert doc.field == to_embed
        # Same thing with a Document with a _cls field
        to_embed_recursive = ToEmbedChild(id=1).save()
        to_embed_child = ToEmbedChild(id=2, recursive=to_embed_recursive).save()
        doc = Doc(field=to_embed_child)
        doc.save()
        assert isinstance(doc.field, ToEmbedChild)
        assert doc.field == to_embed_child

    def test_dict_field_invalid_dict_value(self):
        class DictFieldTest(Document):
            dictionary = DictField(required=True)

        DictFieldTest.drop_collection()

        test = DictFieldTest(dictionary=None)
        test.dictionary  # Just access to test getter
        self.assertRaises(ValidationError, test.validate)

        test = DictFieldTest(dictionary=False)
        test.dictionary  # Just access to test getter
        self.assertRaises(ValidationError, test.validate)

    def test_dict_field_raises_validation_error_if_wrongly_assign_embedded_doc(self):
        class DictFieldTest(Document):
            dictionary = DictField(required=True)

        DictFieldTest.drop_collection()

        class Embedded(EmbeddedDocument):
            name = StringField()

        embed = Embedded(name='garbage')
        doc = DictFieldTest(dictionary=embed)
        with self.assertRaises(ValidationError) as ctx_err:
            doc.validate()
        self.assertIn("'dictionary'", str(ctx_err.exception))
        self.assertIn('Only dictionaries may be used in a DictField', str(ctx_err.exception))

    def test_cls_field(self):
        class Animal(Document):
            meta = {'allow_inheritance': True}

        class Fish(Animal):
            pass

        class Mammal(Animal):
            pass

        class Dog(Mammal):
            pass

        class Human(Mammal):
            pass

        Animal.objects.delete()
        Dog().save()
        Fish().save()
        Human().save()
        self.assertEquals(Animal.objects(_cls__in=["Animal.Mammal.Dog", "Animal.Fish"]).count(), 2)
        self.assertEquals(Animal.objects(_cls__in=["Animal.Fish.Guppy"]).count(), 0)

    def test_sparse_field(self):
        class Doc(Document):
            name = StringField(required=False, unique=True, sparse=True)

        # This would raise an exception in a non-sparse unique index
        Doc().save()
        Doc().save()

    def test_undefined_field_exception(self):
        """Tests if a `FieldDoesNotExist` exception is raised when
        trying to instantiate a document with a field that's not
        defined.
        """
        class Doc(Document):
            foo = StringField()

        with self.assertRaises(FieldDoesNotExist):
            Doc(bar='test')

    def test_undefined_field_exception_with_strict(self):
        """Tests if a `FieldDoesNotExist` exception is raised when
        trying to instantiate a document with a field that's not
        defined, even when strict is set to False.
        """
        class Doc(Document):
            foo = StringField()
            meta = {'strict': False}

        with self.assertRaises(FieldDoesNotExist):
            Doc(bar='test')


class EmbeddedDocumentListFieldTestCase(MongoDBTestCase):

    def setUp(self):
        """
        Create two BlogPost entries in the database, each with
        several EmbeddedDocuments.
        """
        class Comments(EmbeddedDocument):
            author = StringField()
            message = StringField()

        class BlogPost(Document):
            comments = EmbeddedDocumentListField(Comments)

        BlogPost.drop_collection()

        self.Comments = Comments
        self.BlogPost = BlogPost

        self.post1 = self.BlogPost(comments=[
            self.Comments(author='user1', message='message1'),
            self.Comments(author='user2', message='message1')
        ]).save()

        self.post2 = self.BlogPost(comments=[
            self.Comments(author='user2', message='message2'),
            self.Comments(author='user2', message='message3'),
            self.Comments(author='user3', message='message1')
        ]).save()

    def test_fails_upon_validate_if_provide_a_doc_instead_of_a_list_of_doc(self):
        # Relates to Issue #1464
        comment = self.Comments(author='John')

        class Title(Document):
            content = StringField()

        # Test with an embeddedDocument instead of a list(embeddedDocument)
        # It's an edge case but it used to fail with a vague error, making it difficult to troubleshoot it
        post = self.BlogPost(comments=comment)
        with self.assertRaises(ValidationError) as ctx_err:
            post.validate()
        self.assertIn("'comments'", str(ctx_err.exception))
        self.assertIn('Only lists and tuples may be used in a list field', str(ctx_err.exception))

        # Test with a Document
        post = self.BlogPost(comments=Title(content='garbage'))
        with self.assertRaises(ValidationError) as e:
            post.validate()
        self.assertIn("'comments'", str(ctx_err.exception))
        self.assertIn('Only lists and tuples may be used in a list field', str(ctx_err.exception))

    def test_no_keyword_filter(self):
        """
        Tests the filter method of a List of Embedded Documents
        with a no keyword.
        """
        filtered = self.post1.comments.filter()

        # Ensure nothing was changed
        self.assertListEqual(filtered, self.post1.comments)

    def test_single_keyword_filter(self):
        """
        Tests the filter method of a List of Embedded Documents
        with a single keyword.
        """
        filtered = self.post1.comments.filter(author='user1')

        # Ensure only 1 entry was returned.
        self.assertEqual(len(filtered), 1)

        # Ensure the entry returned is the correct entry.
        self.assertEqual(filtered[0].author, 'user1')

    def test_multi_keyword_filter(self):
        """
        Tests the filter method of a List of Embedded Documents
        with multiple keywords.
        """
        filtered = self.post2.comments.filter(
            author='user2', message='message2'
        )

        # Ensure only 1 entry was returned.
        self.assertEqual(len(filtered), 1)

        # Ensure the entry returned is the correct entry.
        self.assertEqual(filtered[0].author, 'user2')
        self.assertEqual(filtered[0].message, 'message2')

    def test_chained_filter(self):
        """
        Tests chained filter methods of a List of Embedded Documents
        """
        filtered = self.post2.comments.filter(author='user2').filter(
            message='message2'
        )

        # Ensure only 1 entry was returned.
        self.assertEqual(len(filtered), 1)

        # Ensure the entry returned is the correct entry.
        self.assertEqual(filtered[0].author, 'user2')
        self.assertEqual(filtered[0].message, 'message2')

    def test_unknown_keyword_filter(self):
        """
        Tests the filter method of a List of Embedded Documents
        when the keyword is not a known keyword.
        """
        with self.assertRaises(AttributeError):
            self.post2.comments.filter(year=2)

    def test_no_keyword_exclude(self):
        """
        Tests the exclude method of a List of Embedded Documents
        with a no keyword.
        """
        filtered = self.post1.comments.exclude()

        # Ensure everything was removed
        self.assertListEqual(filtered, [])

    def test_single_keyword_exclude(self):
        """
        Tests the exclude method of a List of Embedded Documents
        with a single keyword.
        """
        excluded = self.post1.comments.exclude(author='user1')

        # Ensure only 1 entry was returned.
        self.assertEqual(len(excluded), 1)

        # Ensure the entry returned is the correct entry.
        self.assertEqual(excluded[0].author, 'user2')

    def test_multi_keyword_exclude(self):
        """
        Tests the exclude method of a List of Embedded Documents
        with multiple keywords.
        """
        excluded = self.post2.comments.exclude(
            author='user3', message='message1'
        )

        # Ensure only 2 entries were returned.
        self.assertEqual(len(excluded), 2)

        # Ensure the entries returned are the correct entries.
        self.assertEqual(excluded[0].author, 'user2')
        self.assertEqual(excluded[1].author, 'user2')

    def test_non_matching_exclude(self):
        """
        Tests the exclude method of a List of Embedded Documents
        when the keyword does not match any entries.
        """
        excluded = self.post2.comments.exclude(author='user4')

        # Ensure the 3 entries still exist.
        self.assertEqual(len(excluded), 3)

    def test_unknown_keyword_exclude(self):
        """
        Tests the exclude method of a List of Embedded Documents
        when the keyword is not a known keyword.
        """
        with self.assertRaises(AttributeError):
            self.post2.comments.exclude(year=2)

    def test_chained_filter_exclude(self):
        """
        Tests the exclude method after a filter method of a List of
        Embedded Documents.
        """
        excluded = self.post2.comments.filter(author='user2').exclude(
            message='message2'
        )

        # Ensure only 1 entry was returned.
        self.assertEqual(len(excluded), 1)

        # Ensure the entry returned is the correct entry.
        self.assertEqual(excluded[0].author, 'user2')
        self.assertEqual(excluded[0].message, 'message3')

    def test_count(self):
        """
        Tests the count method of a List of Embedded Documents.
        """
        self.assertEqual(self.post1.comments.count(), 2)
        self.assertEqual(self.post1.comments.count(), len(self.post1.comments))

    def test_filtered_count(self):
        """
        Tests the filter + count method of a List of Embedded Documents.
        """
        count = self.post1.comments.filter(author='user1').count()
        self.assertEqual(count, 1)

    def test_single_keyword_get(self):
        """
        Tests the get method of a List of Embedded Documents using a
        single keyword.
        """
        comment = self.post1.comments.get(author='user1')
        self.assertIsInstance(comment, self.Comments)
        self.assertEqual(comment.author, 'user1')

    def test_multi_keyword_get(self):
        """
        Tests the get method of a List of Embedded Documents using
        multiple keywords.
        """
        comment = self.post2.comments.get(author='user2', message='message2')
        self.assertIsInstance(comment, self.Comments)
        self.assertEqual(comment.author, 'user2')
        self.assertEqual(comment.message, 'message2')

    def test_no_keyword_multiple_return_get(self):
        """
        Tests the get method of a List of Embedded Documents without
        a keyword to return multiple documents.
        """
        with self.assertRaises(MultipleObjectsReturned):
            self.post1.comments.get()

    def test_keyword_multiple_return_get(self):
        """
        Tests the get method of a List of Embedded Documents with a keyword
        to return multiple documents.
        """
        with self.assertRaises(MultipleObjectsReturned):
            self.post2.comments.get(author='user2')

    def test_unknown_keyword_get(self):
        """
        Tests the get method of a List of Embedded Documents with an
        unknown keyword.
        """
        with self.assertRaises(AttributeError):
            self.post2.comments.get(year=2020)

    def test_no_result_get(self):
        """
        Tests the get method of a List of Embedded Documents where get
        returns no results.
        """
        with self.assertRaises(DoesNotExist):
            self.post1.comments.get(author='user3')

    def test_first(self):
        """
        Tests the first method of a List of Embedded Documents to
        ensure it returns the first comment.
        """
        comment = self.post1.comments.first()

        # Ensure a Comment object was returned.
        self.assertIsInstance(comment, self.Comments)
        self.assertEqual(comment, self.post1.comments[0])

    def test_create(self):
        """
        Test the create method of a List of Embedded Documents.
        """
        comment = self.post1.comments.create(
            author='user4', message='message1'
        )
        self.post1.save()

        # Ensure the returned value is the comment object.
        self.assertIsInstance(comment, self.Comments)
        self.assertEqual(comment.author, 'user4')
        self.assertEqual(comment.message, 'message1')

        # Ensure the new comment was actually saved to the database.
        self.assertIn(
            comment,
            self.BlogPost.objects(comments__author='user4')[0].comments
        )

    def test_filtered_create(self):
        """
        Test the create method of a List of Embedded Documents chained
        to a call to the filter method. Filtering should have no effect
        on creation.
        """
        comment = self.post1.comments.filter(author='user1').create(
            author='user4', message='message1'
        )
        self.post1.save()

        # Ensure the returned value is the comment object.
        self.assertIsInstance(comment, self.Comments)
        self.assertEqual(comment.author, 'user4')
        self.assertEqual(comment.message, 'message1')

        # Ensure the new comment was actually saved to the database.
        self.assertIn(
            comment,
            self.BlogPost.objects(comments__author='user4')[0].comments
        )

    def test_no_keyword_update(self):
        """
        Tests the update method of a List of Embedded Documents with
        no keywords.
        """
        original = list(self.post1.comments)
        number = self.post1.comments.update()
        self.post1.save()

        # Ensure that nothing was altered.
        self.assertIn(
            original[0],
            self.BlogPost.objects(id=self.post1.id)[0].comments
        )

        self.assertIn(
            original[1],
            self.BlogPost.objects(id=self.post1.id)[0].comments
        )

        # Ensure the method returned 0 as the number of entries
        # modified
        self.assertEqual(number, 0)

    def test_single_keyword_update(self):
        """
        Tests the update method of a List of Embedded Documents with
        a single keyword.
        """
        number = self.post1.comments.update(author='user4')
        self.post1.save()

        comments = self.BlogPost.objects(id=self.post1.id)[0].comments

        # Ensure that the database was updated properly.
        self.assertEqual(comments[0].author, 'user4')
        self.assertEqual(comments[1].author, 'user4')

        # Ensure the method returned 2 as the number of entries
        # modified
        self.assertEqual(number, 2)

    def test_unicode(self):
        """
        Tests that unicode strings handled correctly
        """
        post = self.BlogPost(comments=[
            self.Comments(author='user1', message=u'сообщение'),
            self.Comments(author='user2', message=u'хабарлама')
        ]).save()
        self.assertEqual(post.comments.get(message=u'сообщение').author,
                         'user1')

    def test_save(self):
        """
        Tests the save method of a List of Embedded Documents.
        """
        comments = self.post1.comments
        new_comment = self.Comments(author='user4')
        comments.append(new_comment)
        comments.save()

        # Ensure that the new comment has been added to the database.
        self.assertIn(
            new_comment,
            self.BlogPost.objects(id=self.post1.id)[0].comments
        )

    def test_delete(self):
        """
        Tests the delete method of a List of Embedded Documents.
        """
        number = self.post1.comments.delete()
        self.post1.save()

        # Ensure that all the comments under post1 were deleted in the
        # database.
        self.assertListEqual(
            self.BlogPost.objects(id=self.post1.id)[0].comments, []
        )

        # Ensure that post1 comments were deleted from the list.
        self.assertListEqual(self.post1.comments, [])

        # Ensure that comments still returned a EmbeddedDocumentList object.
        self.assertIsInstance(self.post1.comments, EmbeddedDocumentList)

        # Ensure that the delete method returned 2 as the number of entries
        # deleted from the database
        self.assertEqual(number, 2)

    def test_empty_list_embedded_documents_with_unique_field(self):
        """
        Tests that only one document with an empty list of embedded documents
        that have a unique field can be saved, but if the unique field is
        also sparse than multiple documents with an empty list can be saved.
        """
        class EmbeddedWithUnique(EmbeddedDocument):
            number = IntField(unique=True)

        class A(Document):
            my_list = ListField(EmbeddedDocumentField(EmbeddedWithUnique))

        A(my_list=[]).save()
        with self.assertRaises(NotUniqueError):
            A(my_list=[]).save()

        class EmbeddedWithSparseUnique(EmbeddedDocument):
            number = IntField(unique=True, sparse=True)

        class B(Document):
            my_list = ListField(EmbeddedDocumentField(EmbeddedWithSparseUnique))

        A.drop_collection()
        B.drop_collection()

        B(my_list=[]).save()
        B(my_list=[]).save()

    def test_filtered_delete(self):
        """
        Tests the delete method of a List of Embedded Documents
        after the filter method has been called.
        """
        comment = self.post1.comments[1]
        number = self.post1.comments.filter(author='user2').delete()
        self.post1.save()

        # Ensure that only the user2 comment was deleted.
        self.assertNotIn(
            comment, self.BlogPost.objects(id=self.post1.id)[0].comments
        )
        self.assertEqual(
            len(self.BlogPost.objects(id=self.post1.id)[0].comments), 1
        )

        # Ensure that the user2 comment no longer exists in the list.
        self.assertNotIn(comment, self.post1.comments)
        self.assertEqual(len(self.post1.comments), 1)

        # Ensure that the delete method returned 1 as the number of entries
        # deleted from the database
        self.assertEqual(number, 1)

    def test_custom_data(self):
        """
        Tests that custom data is saved in the field object
        and doesn't interfere with the rest of field functionalities.
        """
        custom_data = {'a': 'a_value', 'b': [1, 2]}

        class CustomData(Document):
            a_field = IntField()
            c_field = IntField(custom_data=custom_data)

        CustomData.drop_collection()

        a1 = CustomData(a_field=1, c_field=2).save()
        self.assertEqual(2, a1.c_field)
        self.assertFalse(hasattr(a1.c_field, 'custom_data'))
        self.assertTrue(hasattr(CustomData.c_field, 'custom_data'))
        self.assertEqual(custom_data['a'], CustomData.c_field.custom_data['a'])


class TestEmbeddedDocumentField(MongoDBTestCase):
    def test___init___(self):
        class MyDoc(EmbeddedDocument):
            name = StringField()

        field = EmbeddedDocumentField(MyDoc)
        self.assertEqual(field.document_type_obj, MyDoc)

        field2 = EmbeddedDocumentField('MyDoc')
        self.assertEqual(field2.document_type_obj, 'MyDoc')

    def test___init___throw_error_if_document_type_is_not_EmbeddedDocument(self):
        with self.assertRaises(ValidationError):
            EmbeddedDocumentField(dict)

    def test_document_type_throw_error_if_not_EmbeddedDocument_subclass(self):

        class MyDoc(Document):
            name = StringField()

        emb = EmbeddedDocumentField('MyDoc')
        with self.assertRaises(ValidationError) as ctx:
            emb.document_type
        self.assertIn('Invalid embedded document class provided to an EmbeddedDocumentField', str(ctx.exception))

    def test_embedded_document_field_only_allow_subclasses_of_embedded_document(self):
        # Relates to #1661
        class MyDoc(Document):
            name = StringField()

        with self.assertRaises(ValidationError):
            class MyFailingDoc(Document):
                emb = EmbeddedDocumentField(MyDoc)

        with self.assertRaises(ValidationError):
            class MyFailingdoc2(Document):
                emb = EmbeddedDocumentField('MyDoc')


class CachedReferenceFieldTest(MongoDBTestCase):

    def test_cached_reference_field_get_and_save(self):
        """
        Tests #1047: CachedReferenceField creates DBRefs on to_python,
        but can't save them on to_mongo.
        """
        class Animal(Document):
            name = StringField()
            tag = StringField()

        class Ocorrence(Document):
            person = StringField()
            animal = CachedReferenceField(Animal)

        Animal.drop_collection()
        Ocorrence.drop_collection()

        Ocorrence(person="testte",
                  animal=Animal(name="Leopard", tag="heavy").save()).save()
        p = Ocorrence.objects.get()
        p.person = 'new_testte'
        p.save()

    def test_cached_reference_fields(self):
        class Animal(Document):
            name = StringField()
            tag = StringField()

        class Ocorrence(Document):
            person = StringField()
            animal = CachedReferenceField(
                Animal, fields=['tag'])

        Animal.drop_collection()
        Ocorrence.drop_collection()

        a = Animal(name="Leopard", tag="heavy")
        a.save()

        self.assertEqual(Animal._cached_reference_fields, [Ocorrence.animal])
        o = Ocorrence(person="teste", animal=a)
        o.save()

        p = Ocorrence(person="Wilson")
        p.save()

        self.assertEqual(Ocorrence.objects(animal=None).count(), 1)

        self.assertEqual(
            a.to_mongo(fields=['tag']), {'tag': 'heavy', "_id": a.pk})

        self.assertEqual(o.to_mongo()['animal']['tag'], 'heavy')

        # counts
        Ocorrence(person="teste 2").save()
        Ocorrence(person="teste 3").save()

        count = Ocorrence.objects(animal__tag='heavy').count()
        self.assertEqual(count, 1)

        ocorrence = Ocorrence.objects(animal__tag='heavy').first()
        self.assertEqual(ocorrence.person, "teste")
        self.assertIsInstance(ocorrence.animal, Animal)

    def test_cached_reference_field_decimal(self):
        class PersonAuto(Document):
            name = StringField()
            salary = DecimalField()

        class SocialTest(Document):
            group = StringField()
            person = CachedReferenceField(
                PersonAuto,
                fields=('salary',))

        PersonAuto.drop_collection()
        SocialTest.drop_collection()

        p = PersonAuto(name="Alberto", salary=Decimal('7000.00'))
        p.save()

        s = SocialTest(group="dev", person=p)
        s.save()

        self.assertEqual(
            SocialTest.objects._collection.find_one({'person.salary': 7000.00}), {
                '_id': s.pk,
                'group': s.group,
                'person': {
                    '_id': p.pk,
                    'salary': 7000.00
                }
            })

    def test_cached_reference_field_reference(self):
        class Group(Document):
            name = StringField()

        class Person(Document):
            name = StringField()
            group = ReferenceField(Group)

        class SocialData(Document):
            obs = StringField()
            tags = ListField(
                StringField())
            person = CachedReferenceField(
                Person,
                fields=('group',))

        Group.drop_collection()
        Person.drop_collection()
        SocialData.drop_collection()

        g1 = Group(name='dev')
        g1.save()

        g2 = Group(name="designers")
        g2.save()

        p1 = Person(name="Alberto", group=g1)
        p1.save()

        p2 = Person(name="Andre", group=g1)
        p2.save()

        p3 = Person(name="Afro design", group=g2)
        p3.save()

        s1 = SocialData(obs="testing 123", person=p1, tags=['tag1', 'tag2'])
        s1.save()

        s2 = SocialData(obs="testing 321", person=p3, tags=['tag3', 'tag4'])
        s2.save()

        self.assertEqual(SocialData.objects._collection.find_one(
            {'tags': 'tag2'}), {
                '_id': s1.pk,
                'obs': 'testing 123',
                'tags': ['tag1', 'tag2'],
                'person': {
                    '_id': p1.pk,
                    'group': g1.pk
                }
        })

        self.assertEqual(SocialData.objects(person__group=g2).count(), 1)
        self.assertEqual(SocialData.objects(person__group=g2).first(), s2)

    def test_cached_reference_field_push_with_fields(self):
        class Product(Document):
            name = StringField()

        Product.drop_collection()

        class Basket(Document):
            products = ListField(CachedReferenceField(Product, fields=['name']))

        Basket.drop_collection()
        product1 = Product(name='abc').save()
        product2 = Product(name='def').save()
        basket = Basket(products=[product1]).save()
        self.assertEqual(
            Basket.objects._collection.find_one(),
            {
                '_id': basket.pk,
                'products': [
                    {
                        '_id': product1.pk,
                        'name': product1.name
                    }
                ]
            }
        )
        # push to list
        basket.update(push__products=product2)
        basket.reload()
        self.assertEqual(
            Basket.objects._collection.find_one(),
            {
                '_id': basket.pk,
                'products': [
                    {
                        '_id': product1.pk,
                        'name': product1.name
                    },
                    {
                        '_id': product2.pk,
                        'name': product2.name
                    }
                ]
            }
        )

    def test_cached_reference_field_update_all(self):
        class Person(Document):
            TYPES = (
                ('pf', "PF"),
                ('pj', "PJ")
            )
            name = StringField()
            tp = StringField(
                choices=TYPES
            )

            father = CachedReferenceField('self', fields=('tp',))

        Person.drop_collection()

        a1 = Person(name="Wilson Father", tp="pj")
        a1.save()

        a2 = Person(name='Wilson Junior', tp='pf', father=a1)
        a2.save()

        self.assertEqual(dict(a2.to_mongo()), {
            "_id": a2.pk,
            "name": u"Wilson Junior",
            "tp": u"pf",
            "father": {
                "_id": a1.pk,
                "tp": u"pj"
            }
        })

        self.assertEqual(Person.objects(father=a1)._query, {
            'father._id': a1.pk
        })
        self.assertEqual(Person.objects(father=a1).count(), 1)

        Person.objects.update(set__tp="pf")
        Person.father.sync_all()

        a2.reload()
        self.assertEqual(dict(a2.to_mongo()), {
            "_id": a2.pk,
            "name": u"Wilson Junior",
            "tp": u"pf",
            "father": {
                "_id": a1.pk,
                "tp": u"pf"
            }
        })

    def test_cached_reference_fields_on_embedded_documents(self):
        with self.assertRaises(InvalidDocumentError):
            class Test(Document):
                name = StringField()

            type('WrongEmbeddedDocument', (
                EmbeddedDocument,), {
                    'test': CachedReferenceField(Test)
            })

    def test_cached_reference_auto_sync(self):
        class Person(Document):
            TYPES = (
                ('pf', "PF"),
                ('pj', "PJ")
            )
            name = StringField()
            tp = StringField(
                choices=TYPES
            )

            father = CachedReferenceField('self', fields=('tp',))

        Person.drop_collection()

        a1 = Person(name="Wilson Father", tp="pj")
        a1.save()

        a2 = Person(name='Wilson Junior', tp='pf', father=a1)
        a2.save()

        a1.tp = 'pf'
        a1.save()

        a2.reload()
        self.assertEqual(dict(a2.to_mongo()), {
            '_id': a2.pk,
            'name': 'Wilson Junior',
            'tp': 'pf',
            'father': {
                '_id': a1.pk,
                'tp': 'pf'
            }
        })

    def test_cached_reference_auto_sync_disabled(self):
        class Persone(Document):
            TYPES = (
                ('pf', "PF"),
                ('pj', "PJ")
            )
            name = StringField()
            tp = StringField(
                choices=TYPES
            )

            father = CachedReferenceField(
                'self', fields=('tp',), auto_sync=False)

        Persone.drop_collection()

        a1 = Persone(name="Wilson Father", tp="pj")
        a1.save()

        a2 = Persone(name='Wilson Junior', tp='pf', father=a1)
        a2.save()

        a1.tp = 'pf'
        a1.save()

        self.assertEqual(Persone.objects._collection.find_one({'_id': a2.pk}), {
            '_id': a2.pk,
            'name': 'Wilson Junior',
            'tp': 'pf',
            'father': {
                '_id': a1.pk,
                'tp': 'pj'
            }
        })

    def test_cached_reference_embedded_fields(self):
        class Owner(EmbeddedDocument):
            TPS = (
                ('n', "Normal"),
                ('u', "Urgent")
            )
            name = StringField()
            tp = StringField(
                verbose_name="Type",
                db_field="t",
                choices=TPS)

        class Animal(Document):
            name = StringField()
            tag = StringField()

            owner = EmbeddedDocumentField(Owner)

        class Ocorrence(Document):
            person = StringField()
            animal = CachedReferenceField(
                Animal, fields=['tag', 'owner.tp'])

        Animal.drop_collection()
        Ocorrence.drop_collection()

        a = Animal(name="Leopard", tag="heavy",
                   owner=Owner(tp='u', name="Wilson Júnior")
                   )
        a.save()

        o = Ocorrence(person="teste", animal=a)
        o.save()
        self.assertEqual(dict(a.to_mongo(fields=['tag', 'owner.tp'])), {
            '_id': a.pk,
            'tag': 'heavy',
            'owner': {
                't': 'u'
            }
        })
        self.assertEqual(o.to_mongo()['animal']['tag'], 'heavy')
        self.assertEqual(o.to_mongo()['animal']['owner']['t'], 'u')

        # counts
        Ocorrence(person="teste 2").save()
        Ocorrence(person="teste 3").save()

        count = Ocorrence.objects(
            animal__tag='heavy', animal__owner__tp='u').count()
        self.assertEqual(count, 1)

        ocorrence = Ocorrence.objects(
            animal__tag='heavy',
            animal__owner__tp='u').first()
        self.assertEqual(ocorrence.person, "teste")
        self.assertIsInstance(ocorrence.animal, Animal)

    def test_cached_reference_embedded_list_fields(self):
        class Owner(EmbeddedDocument):
            name = StringField()
            tags = ListField(StringField())

        class Animal(Document):
            name = StringField()
            tag = StringField()

            owner = EmbeddedDocumentField(Owner)

        class Ocorrence(Document):
            person = StringField()
            animal = CachedReferenceField(
                Animal, fields=['tag', 'owner.tags'])

        Animal.drop_collection()
        Ocorrence.drop_collection()

        a = Animal(name="Leopard", tag="heavy",
                   owner=Owner(tags=['cool', 'funny'],
                               name="Wilson Júnior")
                   )
        a.save()

        o = Ocorrence(person="teste 2", animal=a)
        o.save()
        self.assertEqual(dict(a.to_mongo(fields=['tag', 'owner.tags'])), {
            '_id': a.pk,
            'tag': 'heavy',
            'owner': {
                'tags': ['cool', 'funny']
            }
        })

        self.assertEqual(o.to_mongo()['animal']['tag'], 'heavy')
        self.assertEqual(o.to_mongo()['animal']['owner']['tags'],
                         ['cool', 'funny'])

        # counts
        Ocorrence(person="teste 2").save()
        Ocorrence(person="teste 3").save()

        query = Ocorrence.objects(
            animal__tag='heavy', animal__owner__tags='cool')._query
        self.assertEqual(
            query, {'animal.owner.tags': 'cool', 'animal.tag': 'heavy'})

        ocorrence = Ocorrence.objects(
            animal__tag='heavy',
            animal__owner__tags='cool').first()
        self.assertEqual(ocorrence.person, "teste 2")
        self.assertIsInstance(ocorrence.animal, Animal)


if __name__ == '__main__':
    unittest.main()
