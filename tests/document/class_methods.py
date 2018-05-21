# -*- coding: utf-8 -*-
import unittest

from mongoengine import *

from mongoengine.queryset import NULLIFY, PULL
from mongoengine.connection import get_db
from tests.utils import needs_mongodb_v26

__all__ = ("ClassMethodsTest", )


class ClassMethodsTest(unittest.TestCase):

    def setUp(self):
        connect(db='mongoenginetest')
        self.db = get_db()

        class Person(Document):
            name = StringField()
            age = IntField()

            non_field = True

            meta = {"allow_inheritance": True}

        self.Person = Person

    def tearDown(self):
        for collection in self.db.collection_names():
            if 'system.' in collection:
                continue
            self.db.drop_collection(collection)

    def test_definition(self):
        """Ensure that document may be defined using fields.
        """
        self.assertEqual(['_cls', 'age', 'id', 'name'],
                         sorted(self.Person._fields.keys()))
        self.assertEqual(["IntField", "ObjectIdField", "StringField", "StringField"],
                        sorted([x.__class__.__name__ for x in
                                self.Person._fields.values()]))

    def test_get_db(self):
        """Ensure that get_db returns the expected db.
        """
        db = self.Person._get_db()
        self.assertEqual(self.db, db)

    def test_get_collection_name(self):
        """Ensure that get_collection_name returns the expected collection
        name.
        """
        collection_name = 'person'
        self.assertEqual(collection_name, self.Person._get_collection_name())

    def test_get_collection(self):
        """Ensure that get_collection returns the expected collection.
        """
        collection_name = 'person'
        collection = self.Person._get_collection()
        self.assertEqual(self.db[collection_name], collection)

    def test_drop_collection(self):
        """Ensure that the collection may be dropped from the database.
        """
        collection_name = 'person'
        self.Person(name='Test').save()
        self.assertTrue(collection_name in self.db.collection_names())

        self.Person.drop_collection()
        self.assertFalse(collection_name in self.db.collection_names())

    def test_register_delete_rule(self):
        """Ensure that register delete rule adds a delete rule to the document
        meta.
        """
        class Job(Document):
            employee = ReferenceField(self.Person)

        self.assertEqual(self.Person._meta.get('delete_rules'), None)

        self.Person.register_delete_rule(Job, 'employee', NULLIFY)
        self.assertEqual(self.Person._meta['delete_rules'],
                         {(Job, 'employee'): NULLIFY})

    def test_compare_indexes(self):
        """ Ensure that the indexes are properly created and that
        compare_indexes identifies the missing/extra indexes
        """

        class BlogPost(Document):
            author = StringField()
            title = StringField()
            description = StringField()
            tags = StringField()

            meta = {
                'indexes': [('author', 'title')]
            }

        BlogPost.drop_collection()

        BlogPost.ensure_indexes()
        self.assertEqual(BlogPost.compare_indexes(), { 'missing': [], 'extra': [] })

        BlogPost.ensure_index(['author', 'description'])
        self.assertEqual(BlogPost.compare_indexes(), { 'missing': [], 'extra': [[('author', 1), ('description', 1)]] })

        BlogPost._get_collection().drop_index('author_1_description_1')
        self.assertEqual(BlogPost.compare_indexes(), { 'missing': [], 'extra': [] })

        BlogPost._get_collection().drop_index('author_1_title_1')
        self.assertEqual(BlogPost.compare_indexes(), { 'missing': [[('author', 1), ('title', 1)]], 'extra': [] })

    def test_compare_indexes_inheritance(self):
        """ Ensure that the indexes are properly created and that
        compare_indexes identifies the missing/extra indexes for subclassed
        documents (_cls included)
        """

        class BlogPost(Document):
            author = StringField()
            title = StringField()
            description = StringField()

            meta = {
                'allow_inheritance': True
            }

        class BlogPostWithTags(BlogPost):
            tags = StringField()
            tag_list = ListField(StringField())

            meta = {
                'indexes': [('author', 'tags')]
            }

        BlogPost.drop_collection()

        BlogPost.ensure_indexes()
        BlogPostWithTags.ensure_indexes()
        self.assertEqual(BlogPost.compare_indexes(), { 'missing': [], 'extra': [] })

        BlogPostWithTags.ensure_index(['author', 'tag_list'])
        self.assertEqual(BlogPost.compare_indexes(), { 'missing': [], 'extra': [[('_cls', 1), ('author', 1), ('tag_list', 1)]] })

        BlogPostWithTags._get_collection().drop_index('_cls_1_author_1_tag_list_1')
        self.assertEqual(BlogPost.compare_indexes(), { 'missing': [], 'extra': [] })

        BlogPostWithTags._get_collection().drop_index('_cls_1_author_1_tags_1')
        self.assertEqual(BlogPost.compare_indexes(), { 'missing': [[('_cls', 1), ('author', 1), ('tags', 1)]], 'extra': [] })

    def test_compare_indexes_multiple_subclasses(self):
        """ Ensure that compare_indexes behaves correctly if called from a
        class, which base class has multiple subclasses
        """

        class BlogPost(Document):
            author = StringField()
            title = StringField()
            description = StringField()

            meta = {
                'allow_inheritance': True
            }

        class BlogPostWithTags(BlogPost):
            tags = StringField()
            tag_list = ListField(StringField())

            meta = {
                'indexes': [('author', 'tags')]
            }

        class BlogPostWithCustomField(BlogPost):
            custom = DictField()

            meta = {
                'indexes': [('author', 'custom')]
            }

        BlogPost.ensure_indexes()
        BlogPostWithTags.ensure_indexes()
        BlogPostWithCustomField.ensure_indexes()

        self.assertEqual(BlogPost.compare_indexes(), { 'missing': [], 'extra': [] })
        self.assertEqual(BlogPostWithTags.compare_indexes(), { 'missing': [], 'extra': [] })
        self.assertEqual(BlogPostWithCustomField.compare_indexes(), { 'missing': [], 'extra': [] })

    @needs_mongodb_v26
    def test_compare_indexes_for_text_indexes(self):
        """ Ensure that compare_indexes behaves correctly for text indexes """

        class Doc(Document):
            a = StringField()
            b = StringField()
            meta = {'indexes': [
                {'fields': ['$a', "$b"],
                 'default_language': 'english',
                 'weights': {'a': 10, 'b': 2}
                }
            ]}

        Doc.drop_collection()
        Doc.ensure_indexes()
        actual = Doc.compare_indexes()
        expected = {'missing': [], 'extra': []}
        self.assertEqual(actual, expected)

    def test_list_indexes_inheritance(self):
        """ ensure that all of the indexes are listed regardless of the super-
        or sub-class that we call it from
        """

        class BlogPost(Document):
            author = StringField()
            title = StringField()
            description = StringField()

            meta = {
                'allow_inheritance': True
            }

        class BlogPostWithTags(BlogPost):
            tags = StringField()

            meta = {
                'indexes': [('author', 'tags')]
            }

        class BlogPostWithTagsAndExtraText(BlogPostWithTags):
            extra_text = StringField()

            meta = {
                'indexes': [('author', 'tags', 'extra_text')]
            }

        BlogPost.drop_collection()

        BlogPost.ensure_indexes()
        BlogPostWithTags.ensure_indexes()
        BlogPostWithTagsAndExtraText.ensure_indexes()

        self.assertEqual(BlogPost.list_indexes(),
                         BlogPostWithTags.list_indexes())
        self.assertEqual(BlogPost.list_indexes(),
                         BlogPostWithTagsAndExtraText.list_indexes())
        self.assertEqual(BlogPost.list_indexes(),
                         [[('_cls', 1), ('author', 1), ('tags', 1)],
                         [('_cls', 1), ('author', 1), ('tags', 1), ('extra_text', 1)],
                         [(u'_id', 1)], [('_cls', 1)]])

    def test_register_delete_rule_inherited(self):

        class Vaccine(Document):
            name = StringField(required=True)

            meta = {"indexes": ["name"]}

        class Animal(Document):
            family = StringField(required=True)
            vaccine_made = ListField(ReferenceField("Vaccine", reverse_delete_rule=PULL))

            meta = {"allow_inheritance": True, "indexes": ["family"]}

        class Cat(Animal):
            name = StringField(required=True)

        self.assertEqual(Vaccine._meta['delete_rules'][(Animal, 'vaccine_made')], PULL)
        self.assertEqual(Vaccine._meta['delete_rules'][(Cat, 'vaccine_made')], PULL)

    def test_collection_naming(self):
        """Ensure that a collection with a specified name may be used.
        """

        class DefaultNamingTest(Document):
            pass
        self.assertEqual('default_naming_test',
                         DefaultNamingTest._get_collection_name())

        class CustomNamingTest(Document):
            meta = {'collection': 'pimp_my_collection'}

        self.assertEqual('pimp_my_collection',
                         CustomNamingTest._get_collection_name())

        class DynamicNamingTest(Document):
            meta = {'collection': lambda c: "DYNAMO"}
        self.assertEqual('DYNAMO', DynamicNamingTest._get_collection_name())

        # Use Abstract class to handle backwards compatibility
        class BaseDocument(Document):
            meta = {
                'abstract': True,
                'collection': lambda c: c.__name__.lower()
            }

        class OldNamingConvention(BaseDocument):
            pass
        self.assertEqual('oldnamingconvention',
                         OldNamingConvention._get_collection_name())

        class InheritedAbstractNamingTest(BaseDocument):
            meta = {'collection': 'wibble'}
        self.assertEqual('wibble',
                         InheritedAbstractNamingTest._get_collection_name())

        # Mixin tests
        class BaseMixin(object):
            meta = {
                'collection': lambda c: c.__name__.lower()
            }

        class OldMixinNamingConvention(Document, BaseMixin):
            pass
        self.assertEqual('oldmixinnamingconvention',
                          OldMixinNamingConvention._get_collection_name())

        class BaseMixin(object):
            meta = {
                'collection': lambda c: c.__name__.lower()
            }

        class BaseDocument(Document, BaseMixin):
            meta = {'allow_inheritance': True}

        class MyDocument(BaseDocument):
            pass

        self.assertEqual('basedocument', MyDocument._get_collection_name())

    def test_custom_collection_name_operations(self):
        """Ensure that a collection with a specified name is used as expected.
        """
        collection_name = 'personCollTest'

        class Person(Document):
            name = StringField()
            meta = {'collection': collection_name}

        Person(name="Test User").save()
        self.assertTrue(collection_name in self.db.collection_names())

        user_obj = self.db[collection_name].find_one()
        self.assertEqual(user_obj['name'], "Test User")

        user_obj = Person.objects[0]
        self.assertEqual(user_obj.name, "Test User")

        Person.drop_collection()
        self.assertFalse(collection_name in self.db.collection_names())

    def test_collection_name_and_primary(self):
        """Ensure that a collection with a specified name may be used.
        """

        class Person(Document):
            name = StringField(primary_key=True)
            meta = {'collection': 'app'}

        Person(name="Test User").save()

        user_obj = Person.objects.first()
        self.assertEqual(user_obj.name, "Test User")

        Person.drop_collection()


if __name__ == '__main__':
    unittest.main()
