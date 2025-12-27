from decimal import Decimal

import pytest

from mongoengine import DecimalField, Document, ValidationError
from tests.asynchronous.utils import MongoDBAsyncTestCase


class TestDecimalField(MongoDBAsyncTestCase):
    async def test_storage(self):
        class Person(Document):
            float_value = DecimalField(precision=4)
            string_value = DecimalField(precision=4, force_string=True)

        await Person.adrop_collection()
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
                    await Person(float_value=value, string_value=value).asave()
                else:
                    person = await Person.aobjects.create()
                    person.float_value = value
                    person.string_value = value
                    await person.asave()

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
        actual = await Person.aobjects.exclude("id").as_pymongo().to_list()
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
            actual = await Person.aobjects().scalar(field_name).to_list()
            assert expected == actual

    async def test_save_none(self):
        class Person(Document):
            value = DecimalField()

        await Person.adrop_collection()

        person = Person(value=None)
        assert person.value is None
        await person.asave()
        fetched_person = await Person.aobjects.first()
        fetched_person.value is None

        assert await Person.aobjects(value=None).first() is not None

    async def test_validation(self):
        """Ensure that invalid values cannot be assigned to decimal fields."""

        class Person(Document):
            height = DecimalField(min_value=Decimal("0.1"), max_value=Decimal("3.5"))

        await Person.adrop_collection()

        await Person(height=Decimal("1.89")).asave()
        person = await Person.aobjects.first()
        assert person.height == Decimal("1.89")

        person.height = "2.0"
        await person.asave()
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

    async def test_comparison(self):
        class Person(Document):
            money = DecimalField()

        await Person.adrop_collection()

        await Person(money=6).asave()
        await Person(money=7).asave()
        await Person(money=8).asave()
        await Person(money=10).asave()

        assert 2 == await Person.aobjects(money__gt=Decimal("7")).count()
        assert 2 == await Person.aobjects(money__gt=7).count()
        assert 2 == await Person.aobjects(money__gt="7").count()

        assert 3 == await Person.aobjects(money__gte="7").count()

    async def test_precision_0(self):
        """prevent regression of a bug that was raising an exception when using precision=0"""

        class TestDoc(Document):
            d = DecimalField(precision=0)

        await TestDoc.adrop_collection()

        td = TestDoc(d=Decimal("12.00032678131263"))
        assert td.d == Decimal("12")

    async def test_precision_negative_raise(self):
        """prevent regression of a bug that was raising an exception when using precision=0"""
        with pytest.raises(
            ValidationError, match="precision must be a positive integer"
        ):

            class TestDoc(Document):
                dneg = DecimalField(precision=-1)
