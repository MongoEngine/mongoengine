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

    def test_string_operators_use_an_index(self):
        class Person(Document):
            name = StringField()
            meta = {"indexes": ["name"]}

        Person.drop_collection()
        Person.ensure_indexes()
        Person.objects.insert([Person(name=f"person{i}") for i in range(100)])

        stats = Person.objects(name__startswith="person7").explain()["executionStats"]

        assert stats["nReturned"] == 11
        assert stats["totalKeysExamined"] <= 12

    def test_string_operators_keep_case_insensitive_flag(self):
        class Person(Document):
            name = StringField()

        Person.drop_collection()
        Person(name="Guido").save()

        assert Person.objects(name__istartswith="gui").count() == 1
        assert Person.objects(name__startswith="gui").count() == 0
        assert Person.objects(name__not__istartswith="gui").count() == 0
