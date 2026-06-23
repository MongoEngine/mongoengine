import json
import random
from decimal import Decimal

import pytest
from bson.decimal128 import Decimal128

from mongoengine import Decimal128Field, Document, ValidationError
from tests.asynchronous.utils import MongoDBAsyncTestCase, async_get_as_pymongo


class Decimal128Document(Document):
    dec128_fld = Decimal128Field()
    dec128_min_0 = Decimal128Field(min_value=0)
    dec128_max_100 = Decimal128Field(max_value=100)


async def generate_test_cls() -> Document:
    await Decimal128Document.adrop_collection()
    await Decimal128Document(dec128_fld=None).asave()
    await Decimal128Document(dec128_fld=Decimal(1)).asave()
    return Decimal128Document


class TestDecimal128Field(MongoDBAsyncTestCase):
    async def test_decimal128_validation_good(self):
        doc = Decimal128Document()

        doc.dec128_fld = Decimal(0)
        doc.validate()

        doc.dec128_fld = Decimal(50)
        doc.validate()

        doc.dec128_fld = Decimal(110)
        doc.validate()

        doc.dec128_fld = Decimal("110")
        doc.validate()

    async def test_decimal128_validation_invalid(self):
        """Ensure that invalid values cannot be assigned."""

        doc = Decimal128Document()

        doc.dec128_fld = "ten"

        with pytest.raises(ValidationError):
            doc.validate()

    async def test_decimal128_validation_min(self):
        """Ensure that out of bounds values cannot be assigned."""

        doc = Decimal128Document()

        doc.dec128_min_0 = Decimal(50)
        doc.validate()

        doc.dec128_min_0 = Decimal(-1)
        with pytest.raises(ValidationError):
            doc.validate()

    async def test_decimal128_validation_max(self):
        """Ensure that out of bounds values cannot be assigned."""

        doc = Decimal128Document()

        doc.dec128_max_100 = Decimal(50)
        doc.validate()

        doc.dec128_max_100 = Decimal(101)
        with pytest.raises(ValidationError):
            doc.validate()

    async def test_eq_operator(self):
        cls = await generate_test_cls()
        assert await cls.aobjects(dec128_fld=1.0).count() == 1
        assert await cls.aobjects(dec128_fld=2.0).count() == 0

    async def test_ne_operator(self):
        cls = await generate_test_cls()
        assert await cls.aobjects(dec128_fld__ne=None).count() == 1
        assert await cls.aobjects(dec128_fld__ne=1).count() == 1
        assert await cls.aobjects(dec128_fld__ne=1.0).count() == 1

    async def test_gt_operator(self):
        cls = await generate_test_cls()
        assert await cls.aobjects(dec128_fld__gt=0.5).count() == 1

    async def test_lt_operator(self):
        cls = await generate_test_cls()
        assert await cls.aobjects(dec128_fld__lt=1.5).count() == 1

    async def test_field_exposed_as_python_Decimal(self):
        # from int
        model = await Decimal128Document(dec128_fld=100).asave()
        assert isinstance(model.dec128_fld, Decimal)
        model = await Decimal128Document.aobjects.get(id=model.id)
        assert isinstance(model.dec128_fld, Decimal)
        assert model.dec128_fld == Decimal("100")

    async def test_storage(self):
        # from int
        model = await Decimal128Document(dec128_fld=100).asave()
        assert await async_get_as_pymongo(model) == {
            "_id": model.id,
            "dec128_fld": Decimal128("100"),
        }

        # from str
        model = await Decimal128Document(dec128_fld="100.0").asave()
        assert await async_get_as_pymongo(model) == {
            "_id": model.id,
            "dec128_fld": Decimal128("100.0"),
        }

        # from float
        model = await Decimal128Document(dec128_fld=100.0).asave()
        assert await async_get_as_pymongo(model) == {
            "_id": model.id,
            "dec128_fld": Decimal128("100"),
        }

        # from Decimal
        model = await Decimal128Document(dec128_fld=Decimal(100)).asave()
        assert await async_get_as_pymongo(model) == {
            "_id": model.id,
            "dec128_fld": Decimal128("100"),
        }
        model = await Decimal128Document(dec128_fld=Decimal("100.0")).asave()
        assert await async_get_as_pymongo(model) == {
            "_id": model.id,
            "dec128_fld": Decimal128("100.0"),
        }

        # from Decimal128
        model = await Decimal128Document(dec128_fld=Decimal128("100")).asave()
        assert await async_get_as_pymongo(model) == {
            "_id": model.id,
            "dec128_fld": Decimal128("100"),
        }

    async def test_json(self):
        await Decimal128Document.adrop_collection()
        f = str(random.random())
        await Decimal128Document(dec128_fld=f).asave()
        json_str = await Decimal128Document.aobjects.to_json()
        array = json.loads(json_str)
        assert array[0]["dec128_fld"] == {"$numberDecimal": str(f)}
