# -*- coding: utf-8 -*-
import re

from bson import Regex, SON
from mongoengine import Document, RegexField, ValidationError
from tests.utils import MongoDBTestCase


class Rule(Document):
    """Fake Document using a RegexField"""
    regex = RegexField()


class TestValidate(MongoDBTestCase):
    def test_raises_exception_if_invalid_type(self):
        """RegexField should raise an error if value is not a regex"""
        doc = Rule(regex=1)
        self.assertRaises(ValidationError, doc.save)

    def test_do_not_raise_exception_if_regex(self):
        """RegexField should not raise error if value is a regex"""
        regex = re.compile('mongo', re.U)
        doc = Rule(regex=regex)
        doc.save()


class TestToPython(MongoDBTestCase):
    def test_convert_to_python_regex_if_bson_regex(self):
        """RegexField should convert bson regex to python regex"""
        regex = re.compile('mongo', re.U)
        doc = Rule._from_son(SON([
            ('regex', Regex('mongo', 32))
        ]))

        self.assertEqual(doc.regex, regex)

    def test_keep_original_value_if_invalid_type(self):
        """RegexField should keep the original value if not a regex object"""
        doc = Rule._from_son(SON([
            ('regex', 'str')
        ]))

        self.assertEqual(doc.regex, 'str')


class TestToMongo(MongoDBTestCase):
    def test_convert_to_bson_regex_if_valid_regex(self):
        """RegexField should convert python regex to bson regex"""
        regex = re.compile('mongo', re.U)
        doc = Rule(regex=regex)
        mongo_representation = SON([
            ('regex', Regex('mongo', 32))
        ])
        self.assertEqual(doc.to_mongo(), mongo_representation)

    def test_keep_value_if_invalid_regex(self):
        """RegexField should keep original value if invalid regex"""
        doc = Rule(regex='mongo')
        mongo_representation = SON([
            ('regex', 'mongo')
        ])
        self.assertEqual(doc.to_mongo(), mongo_representation)
