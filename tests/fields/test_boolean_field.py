# -*- coding: utf-8 -*-
from mongoengine import *

from tests.utils import MongoDBTestCase, get_as_pymongo


class TestBooleanField(MongoDBTestCase):
    def test_storage(self):
        class Person(Document):
            admin = BooleanField()

        person = Person(admin=True)
        person.save()
        self.assertEqual(
            get_as_pymongo(person),
            {'_id': person.id,
             'admin': True})

    def test_validation(self):
        """Ensure that invalid values cannot be assigned to boolean
        fields.
        """
        class Person(Document):
            admin = BooleanField()

        person = Person()
        person.admin = True
        person.validate()

        person.admin = 2
        self.assertRaises(ValidationError, person.validate)
        person.admin = 'Yes'
        self.assertRaises(ValidationError, person.validate)
        person.admin = 'False'
        self.assertRaises(ValidationError, person.validate)

    def test_weirdness_constructor(self):
        """When attribute is set in contructor, it gets cast into a bool
        which causes some weird behavior. We dont necessarily want to maintain this behavior
        but its a known issue
        """
        class Person(Document):
            admin = BooleanField()

        new_person = Person(admin='False')
        self.assertTrue(new_person.admin)

        new_person = Person(admin='0')
        self.assertTrue(new_person.admin)
