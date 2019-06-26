import unittest

from mongoengine.common import _import_class
from mongoengine import Document


class TestCommon(unittest.TestCase):
    def test__import_class(self):
        doc_cls = _import_class("Document")
        self.assertIs(doc_cls, Document)

    def test__import_class_raise_if_not_known(self):
        with self.assertRaises(ValueError):
            _import_class("UnknownClass")
