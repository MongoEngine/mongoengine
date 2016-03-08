# -*- coding: utf-8 -*-
import sys
sys.path[0:0] = [""]
import unittest
from mongoengine import *

__all__ = ("DbFieldParameterTest", )


class DbFieldParameterTest(unittest.TestCase):

    def test_document_db_field_validation(self):

        # https://github.com/MongoEngine/mongoengine/issues/904

        data = {'b': {'c': [{'x': 1.0, 'y': 2.0}]}}

        class C(EmbeddedDocument):
            x = FloatField(db_field='fx')
            y = FloatField(db_field='fy')

        class B(EmbeddedDocument):
            c = ListField(EmbeddedDocumentField(C), db_field='fc')

        class A(Document):
            b = EmbeddedDocumentField(B, db_field='fb')

        a = A(**data)

        try:
            a.validate()
        except ValidationError as e:
            self.fail("ValidationError raised: %s" % e.message)


if __name__ == '__main__':
    unittest.main()
