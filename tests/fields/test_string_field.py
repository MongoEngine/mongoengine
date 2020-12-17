import pytest

from mongoengine import *
from tests.utils import MongoDBTestCase, get_as_pymongo


class TestStringField(MongoDBTestCase):
    def test_storage(self):
        class Person(Document):
            name = StringField()

        Person.drop_collection()
        person = Person(name="test123")
        person.save()
        assert get_as_pymongo(person) == {"_id": person.id, "name": "test123"}

    def test_validation(self):
        class Person(Document):
            name = StringField(max_length=20, min_length=2)
            userid = StringField(r"[0-9a-z_]+$")

        with pytest.raises(ValidationError, match="only accepts string values"):
            Person(name=34).validate()

        with pytest.raises(ValidationError, match="value is too short"):
            Person(name="s").validate()

        # Test regex validation on userid
        person = Person(userid="test.User")
        with pytest.raises(ValidationError):
            person.validate()

        person.userid = "test_user"
        assert person.userid == "test_user"
        person.validate()

        # Test max length validation on name
        person = Person(name="Name that is more than twenty characters")
        with pytest.raises(ValidationError):
            person.validate()

        person = Person(name="a friendl name", userid="7a757668sqjdkqlsdkq")
        person.validate()
