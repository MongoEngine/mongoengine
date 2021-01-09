import copy
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

    def test_deepcopy(self):
        regex_field = StringField(regex=r"(^ABC\d\d\d\d$)")
        no_regex_field = StringField()

        # Copy copied field object
        copy.deepcopy(copy.deepcopy(regex_field))
        copy.deepcopy(copy.deepcopy(no_regex_field))
        # Copy same field object multiple times to make sure we restore __deepcopy__ correctly
        copy.deepcopy(regex_field)
        copy.deepcopy(regex_field)
        copy.deepcopy(no_regex_field)
        copy.deepcopy(no_regex_field)

    def test_deepcopy_with_reference_itself(self):
        class User(Document):
            name = StringField(regex=r"(.*)")
            other_user = ReferenceField("self")

        user1 = User(name="John").save()
        User(name="Bob", other_user=user1).save()

        user1.other_user = user1
        user1.save()

        for u in User.objects:
            copied_u = copy.deepcopy(u)
            assert copied_u is not u
            assert copied_u._fields["name"] is not u._fields["name"]
            assert (
                copied_u._fields["name"].regex is u._fields["name"].regex
            )  # Compiled regex objects are atomic
