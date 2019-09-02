import datetime
import unittest
import uuid
from decimal import Decimal

from bson import DBRef, ObjectId
import pymongo
from pymongo.read_preferences import ReadPreference
from pymongo.results import UpdateResult
import six
from six import iteritems

from mongoengine import *
from mongoengine.connection import get_connection, get_db
from mongoengine.context_managers import query_counter, switch_db
from mongoengine.errors import InvalidQueryError
from mongoengine.mongodb_support import MONGODB_36, get_mongodb_version
from mongoengine.queryset import (
    DoesNotExist,
    MultipleObjectsReturned,
    QuerySet,
    QuerySetManager,
    queryset_manager,
)

Document._meta['check_fields_retrieved'] = True


class TestException(unittest.TestCase):
    def setUp(self):
            connect(db="mongoenginetest")

    def test_exception(self):
        import random

        class Person(EmbeddedDocument):
            name = StringField()
            age = DecimalField()

        class Class(EmbeddedDocument):
            number = DecimalField()
            literal = StringField()

        class School(EmbeddedDocument):
            name = StringField()
            classes = ListField(EmbeddedDocumentField(Class))
            director = EmbeddedDocumentField(Person)

        class City(Document):
            schools = ListField(EmbeddedDocumentField(School))
            name = StringField()
            postal_code = StringField()

        City.drop_collection()

        literals = 'ABCDEF'

        schools = [
            {
                'name': 'School number 1', 
                'director': Person(name='Director 1'),
                'classes': [
                    {
                        'number': 1,
                        'literal': random.choice(literals),
                    }
                ]
            }
        ]

        City(schools=schools, name='Moscow', postal_code='000000').save()

        city1 = City.objects.only('postal_code', 'schools.classes.number', 'schools.director.name').first()
        city2 = City.objects.exclude('name', 'schools.name', 'schools.classes.literal', 'schools.director.age').first()

        for city in [city1, city2]:
            excpected_exceptions = [
                (city, 'name'),
                (city.schools[0], 'name'),
                (city.schools[0].director, 'age'),
                (city.schools[0].classes[0], 'literal'),
            ]
            
            no_exceptions = [
                (city, 'postal_code', '000000'),
                (city.schools[0].director, 'name', 'Director 1'),
                (city.schools[0].classes[0], 'number', 1),
            ]
            for doc, field in excpected_exceptions:
                with self.assertRaises(FieldIsNotRetrieved):
                    getattr(doc, field)

            for doc, field, value in no_exceptions:
                assert getattr(doc, field) == value
            

if __name__ == "__main__":
    unittest.main()
