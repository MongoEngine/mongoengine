import pytest

from mongoengine import *
from tests.utils import MongoDBTestCase, get_as_pymongo


class TestBooleanField(MongoDBTestCase):
    def test_storage(self):
        class Person(Document):
            admin = BooleanField()

        person = Person(admin=True)
        person.save()
        assert get_as_pymongo(person) == {"_id": person.id, "admin": True}

    def test_construction_does_not_fail_uncastable_value(self):
        class BoolFail:
            def __bool__(self):
                return "bogus"

        class Person(Document):
            admin = BooleanField()

        person = Person(admin=BoolFail())
        person.admin == "bogus"

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
        with pytest.raises(ValidationError):
            person.validate()
        person.admin = "Yes"
        with pytest.raises(ValidationError):
            person.validate()
        person.admin = "False"
        with pytest.raises(ValidationError):
            person.validate()

    def test_weirdness_constructor(self):
        """When attribute is set in contructor, it gets cast into a bool
        which causes some weird behavior. We dont necessarily want to maintain this behavior
        but its a known issue
        """

        class Person(Document):
            admin = BooleanField()

        new_person = Person(admin="False")
        assert new_person.admin

        new_person = Person(admin="0")
        assert new_person.admin
