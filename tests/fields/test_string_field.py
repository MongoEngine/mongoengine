import copy
from mongoengine import *

from tests.utils import MongoDBTestCase


class TestStringField(MongoDBTestCase):
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
