# -*- coding: utf-8 -*-
import unittest
from enum import Enum
from mongoengine import Document, EnumField
from tests.utils import MongoDBTestCase


class TestField(MongoDBTestCase):

    def test_enum_field(self):
        class Letters(Enum):
            A = 'a'
            B = 'b'
            C = 'c'

        class Model(Document):
            letter_required = EnumField(Letters, required=True)
            letter_optional = EnumField(Letters)

        m = Model(letter_required='a')
        assert m.letter_required is Letters.A
        assert m.letter_optional is None
        assert m.validate() is None

        m.letter_required = Letters.C
        m.letter_optional = Letters.B
        assert m.letter_required is Letters.C
        assert m.letter_optional is Letters.B
        assert m.validate() is None

        m.letter_required = 'd'
        validation = None
        try:
            m.validate()
        except Exception as error:
            validation = error
        assert validation is not None

        m.letter_required = 'a'
        m.letter_optional = 'd'
        validation = None
        try:
            m.validate()
        except Exception as error:
            validation = error
        assert validation is not None


if __name__ == "__main__":
    unittest.main()
