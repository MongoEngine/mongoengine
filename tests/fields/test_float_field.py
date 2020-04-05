# -*- coding: utf-8 -*-
import pytest
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

        assert 1 == TestDocument.objects(float_fld__ne=None).count()
        assert 1 == TestDocument.objects(float_fld__ne=1).count()

    def test_list_of_floats_can_be_none(self):
        class TestFloatListNone(Document):
            list_of_float_fld = ListField(FloatField())

        TestFloatListNone.drop_collection()

        # Verifies that validate works with None
        TestFloatListNone(list_of_float_fld=[None]).save()

    def test_list_of_floats_can_update_none(self):
        class TestFloatListUpdateNone(Document):
            list_of_float_fld = ListField(FloatField())

        TestFloatListUpdateNone.drop_collection()

        doc = TestFloatListUpdateNone()
        doc.list_of_float_fld = [1.2]
        doc.save()

        # Verifies that to_python works with None
        res = TestFloatListUpdateNone.objects(id=doc.id).update(
            set__list_of_float_fld=[None],
        )
        assert res == 1

        # Retrive data from db and verify it.
        ret = TestFloatListUpdateNone.objects.all()[0]
        assert ret.list_of_float_fld == [None]

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

        person.height = "2.0"
        with pytest.raises(ValidationError):
            person.validate()

        person.height = 0.01
        with pytest.raises(ValidationError):
            person.validate()

        person.height = 4.0
        with pytest.raises(ValidationError):
            person.validate()

        person_2 = Person(height="something invalid")
        with pytest.raises(ValidationError):
            person_2.validate()

        big_person = BigPerson()

        for value, value_type in enumerate(six.integer_types):
            big_person.height = value_type(value)
            big_person.validate()

        big_person.height = 2 ** 500
        big_person.validate()

        big_person.height = 2 ** 100000  # Too big for a float value
        with pytest.raises(ValidationError):
            big_person.validate()
