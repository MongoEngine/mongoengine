import pytest
from bson.decimal128 import Decimal128

from mongoengine import *
from tests.utils import MongoDBTestCase


def generate_test_cls() -> Document:
    class TestDocument(Document):
        dec128_fld = Decimal128Field()

    TestDocument.drop_collection()
    TestDocument(dec128_fld=None).save()
    TestDocument(dec128_fld=Decimal128("1")).save()
    return TestDocument


class TestDecimal128Field(MongoDBTestCase):
    def test_decimal128_validation(self):
        """Ensure that invalid values cannot be assigned to int fields."""

        class Person(Document):
            age = Decimal128Field(min_value=0, max_value=110)

        person = Person()
        person.age = Decimal128("0")
        person.validate()

        person.age = Decimal128("50")
        person.validate()

        person.age = Decimal128("110")
        person.validate()

        person.age = Decimal128("-1")
        with pytest.raises(ValidationError):
            person.validate()
        person.age = Decimal128("120")
        with pytest.raises(ValidationError):
            person.validate()
        person.age = "ten"
        with pytest.raises(ValidationError):
            person.validate()

    def test_eq_operator(self):
        cls = generate_test_cls()
        assert 1 == cls.objects(dec128_fld=1.0).count()
        assert 0 == cls.objects(dec128_fld=2.0).count()

    def test_ne_operator(self):
        cls = generate_test_cls()
        assert 1 == cls.objects(dec128_fld__ne=None).count()
        assert 1 == cls.objects(dec128_fld__ne=1).count()
        assert 1 == cls.objects(dec128_fld__ne=1.0).count()

    def test_gt_operator(self):
        cls = generate_test_cls()
        assert 1 == cls.objects(dec128_fld__gt=0.5).count()

    def test_lt_operator(self):
        cls = generate_test_cls()
        assert 1 == cls.objects(dec128_fld__lt=1.5).count()
