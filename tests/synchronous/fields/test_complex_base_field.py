import pytest

from mongoengine.base import ComplexBaseField
from tests.synchronous.utils import MongoDBTestCase


class TestComplexBaseField(MongoDBTestCase):
    def test_field_validation(self):
        with pytest.raises(TypeError, match="field argument must be a Field instance"):
            ComplexBaseField("test")
