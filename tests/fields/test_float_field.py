# -*- coding: utf-8 -*-
import six

from mongoengine import *

from tests.utils import MongoDBTestCase


class TestFloatField(MongoDBTestCase):

    def test_float_ne_operator(self):
        class TestDocument(Document):
            float_fld = FloatField()

        TestDocument.drop_collection()

        TestDocument(float_fld=None).save()
        TestDocument(float_fld=1).save()

        self.assertEqual(1, TestDocument.objects(float_fld__ne=None).count())
        self.assertEqual(1, TestDocument.objects(float_fld__ne=1).count())

    def test_validation(self):
        """Ensure that invalid values cannot be assigned to float fields.
        """
        class Person(Document):
            height = FloatField(min_value=0.1, max_value=3.5)

        class BigPerson(Document):
            height = FloatField()

        person = Person()
        person.height = 1.89
        person.validate()

        person.height = '2.0'
        self.assertRaises(ValidationError, person.validate)

        person.height = 0.01
        self.assertRaises(ValidationError, person.validate)

        person.height = 4.0
        self.assertRaises(ValidationError, person.validate)

        person_2 = Person(height='something invalid')
        self.assertRaises(ValidationError, person_2.validate)

        big_person = BigPerson()

        for value, value_type in enumerate(six.integer_types):
            big_person.height = value_type(value)
            big_person.validate()

        big_person.height = 2 ** 500
        big_person.validate()

        big_person.height = 2 ** 100000  # Too big for a float value
        self.assertRaises(ValidationError, big_person.validate)
