# -*- coding: utf-8 -*-
from mongoengine import *
from mongoengine.base import BaseDict

from tests.utils import MongoDBTestCase, get_as_pymongo


class TestDictField(MongoDBTestCase):

    def test_storage(self):
        class BlogPost(Document):
            info = DictField()

        BlogPost.drop_collection()

        info = {'testkey': 'testvalue'}
        post = BlogPost(info=info).save()
        self.assertEqual(
            get_as_pymongo(post),
            {
                '_id': post.id,
                'info': info
            }
        )

    def test_general_things(self):
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
        self.assertIsInstance(doc.field, dict)
        self.assertEqual(doc.field, {'_id': 2, 'recursive': {'_id': 1, 'recursive': {}}})
        # Same thing with a Document with a _cls field
        to_embed_recursive = ToEmbedChild(id=1).save()
        to_embed_child = ToEmbedChild(
            id=2, recursive=to_embed_recursive.to_mongo().to_dict()).save()
        doc = Doc(field=to_embed_child.to_mongo().to_dict())
        doc.save()
        self.assertIsInstance(doc.field, dict)
        expected = {
            '_id': 2, '_cls': 'ToEmbedParent.ToEmbedChild',
            'recursive': {'_id': 1, '_cls': 'ToEmbedParent.ToEmbedChild', 'recursive': {}}
        }
        self.assertEqual(doc.field, expected)

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

    def test_push_dict(self):
        class MyModel(Document):
            events = ListField(DictField())

        doc = MyModel(events=[{'a': 1}]).save()
        raw_doc = get_as_pymongo(doc)
        expected_raw_doc = {
            '_id': doc.id,
            'events': [{'a': 1}]
        }
        self.assertEqual(raw_doc, expected_raw_doc)

        MyModel.objects(id=doc.id).update(push__events={})
        raw_doc = get_as_pymongo(doc)
        expected_raw_doc = {
            '_id': doc.id,
            'events': [{'a': 1}, {}]
        }
        self.assertEqual(raw_doc, expected_raw_doc)

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
