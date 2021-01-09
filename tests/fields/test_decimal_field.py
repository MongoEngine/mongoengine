from decimal import Decimal

import pytest

from mongoengine import DecimalField, Document, ValidationError
from tests.utils import MongoDBTestCase


class TestDecimalField(MongoDBTestCase):
    def test_storage(self):
        class Person(Document):
            float_value = DecimalField(precision=4)
            string_value = DecimalField(precision=4, force_string=True)

        Person.drop_collection()
        values_to_store = [
            10,
            10.1,
            10.11,
            "10.111",
            Decimal("10.1111"),
            Decimal("10.11111"),
        ]
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
            {"float_value": 10.0, "string_value": "10.0000"},
            {"float_value": 10.1, "string_value": "10.1000"},
            {"float_value": 10.11, "string_value": "10.1100"},
            {"float_value": 10.111, "string_value": "10.1110"},
            {"float_value": 10.1111, "string_value": "10.1111"},
            {"float_value": 10.1111, "string_value": "10.1111"},
        ]
        expected.extend(expected)
        actual = list(Person.objects.exclude("id").as_pymongo())
        assert expected == actual

        # How it comes out locally
        expected = [
            Decimal("10.0000"),
            Decimal("10.1000"),
            Decimal("10.1100"),
            Decimal("10.1110"),
            Decimal("10.1111"),
            Decimal("10.1111"),
        ]
        expected.extend(expected)
        for field_name in ["float_value", "string_value"]:
            actual = list(Person.objects().scalar(field_name))
            assert expected == actual

    def test_save_none(self):
        class Person(Document):
            value = DecimalField()

        Person.drop_collection()

        person = Person(value=None)
        assert person.value is None
        person.save()
        fetched_person = Person.objects.first()
        fetched_person.value is None

    def test_validation(self):
        """Ensure that invalid values cannot be assigned to decimal fields."""

        class Person(Document):
            height = DecimalField(min_value=Decimal("0.1"), max_value=Decimal("3.5"))

        Person.drop_collection()

        Person(height=Decimal("1.89")).save()
        person = Person.objects.first()
        assert person.height == Decimal("1.89")

        person.height = "2.0"
        person.save()
        person.height = 0.01
        with pytest.raises(ValidationError):
            person.validate()
        person.height = Decimal("0.01")
        with pytest.raises(ValidationError):
            person.validate()
        person.height = Decimal("4.0")
        with pytest.raises(ValidationError):
            person.validate()
        person.height = "something invalid"
        with pytest.raises(ValidationError):
            person.validate()

        person_2 = Person(height="something invalid")
        with pytest.raises(ValidationError):
            person_2.validate()

    def test_comparison(self):
        class Person(Document):
            money = DecimalField()

        Person.drop_collection()

        Person(money=6).save()
        Person(money=7).save()
        Person(money=8).save()
        Person(money=10).save()

        assert 2 == Person.objects(money__gt=Decimal("7")).count()
        assert 2 == Person.objects(money__gt=7).count()
        assert 2 == Person.objects(money__gt="7").count()

        assert 3 == Person.objects(money__gte="7").count()
