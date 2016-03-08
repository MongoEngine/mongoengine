# -*- coding: utf-8 -*-
import sys
sys.path[0:0] = [""]
import unittest

from bson import SON
from mongoengine import *

__all__ = ("DbFieldParameterTest", )


class DbFieldParameterTest(unittest.TestCase):

    def setUp(self):
        # testcase from https://github.com/MongoEngine/mongoengine/issues/904
        self.data = {'b': {'c': [{'x': 1.0, 'y': 2.0}]}}
        self.reference_son = SON([('fb', SON([('fc', [SON([('fx', 1.0), ('fy', 2.0)])])]))])

        class C(EmbeddedDocument):
            x = FloatField(db_field='fx')
            y = FloatField(db_field='fy')

        class B(EmbeddedDocument):
            c = ListField(EmbeddedDocumentField(C), db_field='fc')

        class A(Document):
            b = EmbeddedDocumentField(B, db_field='fb')

        self.A = A

    def test_document_db_field_validation(self):
        a = self.A(**self.data)
        try:
            a.validate()
        except ValidationError as e:
            self.fail("ValidationError raised: %s" % e.message)

    def test_document_fieldname_to_db_field_name(self):
        a = self.A(**self.data)
        self.assertEqual(a.to_mongo(), self.reference_son)

    def test_document_db_fieldname_to_fieldname(self):
        a = self.A._from_son(self.reference_son)
        self.assertEqual(a.b.c[0].x, 1.0)
        self.assertEqual(a.b.c[0].y, 2.0)


if __name__ == '__main__':
    unittest.main()
