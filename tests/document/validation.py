# -*- coding: utf-8 -*-
import sys
sys.path[0:0] = [""]

import unittest

from mongoengine import *

__all__ = ("ValidatorErrorTest",)


class ValidatorErrorTest(unittest.TestCase):

    def test_to_dict(self):
        """Ensure a ValidationError handles error to_dict correctly.
        """
        error = ValidationError('root')
        self.assertEqual(error.to_dict(), {})

        # 1st level error schema
        error.errors = {'1st': ValidationError('bad 1st'), }
        self.assertTrue('1st' in error.to_dict())
        self.assertEqual(error.to_dict()['1st'], 'bad 1st')

        # 2nd level error schema
        error.errors = {'1st': ValidationError('bad 1st', errors={
            '2nd': ValidationError('bad 2nd'),
        })}
        self.assertTrue('1st' in error.to_dict())
        self.assertTrue(isinstance(error.to_dict()['1st'], dict))
        self.assertTrue('2nd' in error.to_dict()['1st'])
        self.assertEqual(error.to_dict()['1st']['2nd'], 'bad 2nd')

        # moar levels
        error.errors = {'1st': ValidationError('bad 1st', errors={
            '2nd': ValidationError('bad 2nd', errors={
                '3rd': ValidationError('bad 3rd', errors={
                    '4th': ValidationError('Inception'),
                }),
            }),
        })}
        self.assertTrue('1st' in error.to_dict())
        self.assertTrue('2nd' in error.to_dict()['1st'])
        self.assertTrue('3rd' in error.to_dict()['1st']['2nd'])
        self.assertTrue('4th' in error.to_dict()['1st']['2nd']['3rd'])
        self.assertEqual(error.to_dict()['1st']['2nd']['3rd']['4th'],
                         'Inception')

        self.assertEqual(error.message, "root(2nd.3rd.4th.Inception: ['1st'])")

    def test_model_validation(self):

        class User(Document):
            username = StringField(primary_key=True)
            name = StringField(required=True)

        try:
            User().validate()
        except ValidationError, e:
            self.assertEqual(e.to_dict(), {
                'username': 'Field is required',
                'name': 'Field is required'})

    def test_spaces_in_keys(self):

        class Embedded(DynamicEmbeddedDocument):
            pass

        class Doc(DynamicDocument):
            pass

        Doc.drop_collection()
        doc = Doc()
        setattr(doc, 'hello world', 1)
        doc.save()

        one = Doc.objects.filter(**{'hello world': 1}).count()
        self.assertEqual(1, one)

    def test_fields_rewrite(self):
        class BasePerson(Document):
            name = StringField()
            age = IntField()
            meta = {'abstract': True}

        class Person(BasePerson):
            name = StringField(required=True)

        p = Person(age=15)
        self.assertRaises(ValidationError, p.validate)

    def test_cascaded_save_wrong_reference(self):

        class ADocument(Document):
            val = IntField()

        class BDocument(Document):
            a = ReferenceField(ADocument)

        ADocument.drop_collection()
        BDocument.drop_collection()

        a = ADocument()
        a.val = 15
        a.save()

        b = BDocument()
        b.a = a
        b.save()

        a.delete()

        b = BDocument.objects.first()
        b.save(cascade=True)

    def test_shard_key(self):
        class LogEntry(Document):
            machine = StringField()
            log = StringField()

            meta = {
                'shard_key': ('machine',)
            }

        LogEntry.drop_collection()

        log = LogEntry()
        log.machine = "Localhost"
        log.save()

        log.log = "Saving"
        log.save()

        def change_shard_key():
            log.machine = "127.0.0.1"

        self.assertRaises(OperationError, change_shard_key)

    def test_shard_key_primary(self):
        class LogEntry(Document):
            machine = StringField(primary_key=True)
            log = StringField()

            meta = {
                'shard_key': ('machine',)
            }

        LogEntry.drop_collection()

        log = LogEntry()
        log.machine = "Localhost"
        log.save()

        log.log = "Saving"
        log.save()

        def change_shard_key():
            log.machine = "127.0.0.1"

        self.assertRaises(OperationError, change_shard_key)

    def test_kwargs_simple(self):

        class Embedded(EmbeddedDocument):
            name = StringField()

        class Doc(Document):
            doc_name = StringField()
            doc = EmbeddedDocumentField(Embedded)

        classic_doc = Doc(doc_name="my doc", doc=Embedded(name="embedded doc"))
        dict_doc = Doc(**{"doc_name": "my doc",
                          "doc": {"name": "embedded doc"}})

        self.assertEqual(classic_doc, dict_doc)
        self.assertEqual(classic_doc._data, dict_doc._data)

    def test_kwargs_complex(self):

        class Embedded(EmbeddedDocument):
            name = StringField()

        class Doc(Document):
            doc_name = StringField()
            docs = ListField(EmbeddedDocumentField(Embedded))

        classic_doc = Doc(doc_name="my doc", docs=[
                            Embedded(name="embedded doc1"),
                            Embedded(name="embedded doc2")])
        dict_doc = Doc(**{"doc_name": "my doc",
                          "docs": [{"name": "embedded doc1"},
                                   {"name": "embedded doc2"}]})

        self.assertEqual(classic_doc, dict_doc)
        self.assertEqual(classic_doc._data, dict_doc._data)
