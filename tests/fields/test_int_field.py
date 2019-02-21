# -*- coding: utf-8 -*-
from mongoengine import *

from tests.utils import MongoDBTestCase


class TestIntField(MongoDBTestCase):

    def test_int_validation(self):
        """Ensure that invalid values cannot be assigned to int fields.
        """
        class Person(Document):
            age = IntField(min_value=0, max_value=110)

        person = Person()
        person.age = 0
        person.validate()

        person.age = 50
        person.validate()

        person.age = 110
        person.validate()

        person.age = -1
        self.assertRaises(ValidationError, person.validate)
        person.age = 120
        self.assertRaises(ValidationError, person.validate)
        person.age = 'ten'
        self.assertRaises(ValidationError, person.validate)

    def test_ne_operator(self):
        class TestDocument(Document):
            int_fld = IntField()

        TestDocument.drop_collection()

        TestDocument(int_fld=None).save()
        TestDocument(int_fld=1).save()

        self.assertEqual(1, TestDocument.objects(int_fld__ne=None).count())
        self.assertEqual(1, TestDocument.objects(int_fld__ne=1).count())
