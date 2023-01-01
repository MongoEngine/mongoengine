import json
import random
from decimal import Decimal

import pytest
from bson.decimal128 import Decimal128

from mongoengine import Decimal128Field, Document, ValidationError
from tests.utils import MongoDBTestCase, get_as_pymongo


class Decimal128Document(Document):
    dec128_fld = Decimal128Field()
    dec128_min_0 = Decimal128Field(min_value=0)
    dec128_max_100 = Decimal128Field(max_value=100)


def generate_test_cls() -> Document:
    Decimal128Document.drop_collection()
    Decimal128Document(dec128_fld=None).save()
    Decimal128Document(dec128_fld=Decimal(1)).save()
    return Decimal128Document


class TestDecimal128Field(MongoDBTestCase):
    def test_decimal128_validation_good(self):
        doc = Decimal128Document()

        doc.dec128_fld = Decimal(0)
        doc.validate()

        doc.dec128_fld = Decimal(50)
        doc.validate()

        doc.dec128_fld = Decimal(110)
        doc.validate()

        doc.dec128_fld = Decimal("110")
        doc.validate()

    def test_decimal128_validation_invalid(self):
        """Ensure that invalid values cannot be assigned."""

        doc = Decimal128Document()

        doc.dec128_fld = "ten"

        with pytest.raises(ValidationError):
            doc.validate()

    def test_decimal128_validation_min(self):
        """Ensure that out of bounds values cannot be assigned."""

        doc = Decimal128Document()

        doc.dec128_min_0 = Decimal(50)
        doc.validate()

        doc.dec128_min_0 = Decimal(-1)
        with pytest.raises(ValidationError):
            doc.validate()

    def test_decimal128_validation_max(self):
        """Ensure that out of bounds values cannot be assigned."""

        doc = Decimal128Document()

        doc.dec128_max_100 = Decimal(50)
        doc.validate()

        doc.dec128_max_100 = Decimal(101)
        with pytest.raises(ValidationError):
            doc.validate()

    def test_eq_operator(self):
        cls = generate_test_cls()
        assert cls.objects(dec128_fld=1.0).count() == 1
        assert cls.objects(dec128_fld=2.0).count() == 0

    def test_ne_operator(self):
        cls = generate_test_cls()
        assert cls.objects(dec128_fld__ne=None).count() == 1
        assert cls.objects(dec128_fld__ne=1).count() == 1
        assert cls.objects(dec128_fld__ne=1.0).count() == 1

    def test_gt_operator(self):
        cls = generate_test_cls()
        assert cls.objects(dec128_fld__gt=0.5).count() == 1

    def test_lt_operator(self):
        cls = generate_test_cls()
        assert cls.objects(dec128_fld__lt=1.5).count() == 1

    def test_field_exposed_as_python_Decimal(self):
        # from int
        model = Decimal128Document(dec128_fld=100).save()
        assert isinstance(model.dec128_fld, Decimal)
        model = Decimal128Document.objects.get(id=model.id)
        assert isinstance(model.dec128_fld, Decimal)
        assert model.dec128_fld == Decimal("100")

    def test_storage(self):
        # from int
        model = Decimal128Document(dec128_fld=100).save()
        assert get_as_pymongo(model) == {
            "_id": model.id,
            "dec128_fld": Decimal128("100"),
        }

        # from str
        model = Decimal128Document(dec128_fld="100.0").save()
        assert get_as_pymongo(model) == {
            "_id": model.id,
            "dec128_fld": Decimal128("100.0"),
        }

        # from float
        model = Decimal128Document(dec128_fld=100.0).save()
        assert get_as_pymongo(model) == {
            "_id": model.id,
            "dec128_fld": Decimal128("100"),
        }

        # from Decimal
        model = Decimal128Document(dec128_fld=Decimal(100)).save()
        assert get_as_pymongo(model) == {
            "_id": model.id,
            "dec128_fld": Decimal128("100"),
        }
        model = Decimal128Document(dec128_fld=Decimal("100.0")).save()
        assert get_as_pymongo(model) == {
            "_id": model.id,
            "dec128_fld": Decimal128("100.0"),
        }

        # from Decimal128
        model = Decimal128Document(dec128_fld=Decimal128("100")).save()
        assert get_as_pymongo(model) == {
            "_id": model.id,
            "dec128_fld": Decimal128("100"),
        }

    def test_json(self):
        Decimal128Document.drop_collection()
        f = str(random.random())
        Decimal128Document(dec128_fld=f).save()
        json_str = Decimal128Document.objects.to_json()
        array = json.loads(json_str)
        assert array[0]["dec128_fld"] == {"$numberDecimal": str(f)}
