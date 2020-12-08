import unittest
from datetime import datetime

import pytest

from mongoengine import *
from tests.utils import MongoDBTestCase


class TestValidatorError(MongoDBTestCase):
    def test_to_dict(self):
        """Ensure a ValidationError handles error to_dict correctly."""
        error = ValidationError("root")
        assert error.to_dict() == {}

        # 1st level error schema
        error.errors = {"1st": ValidationError("bad 1st")}
        assert "1st" in error.to_dict()
        assert error.to_dict()["1st"] == "bad 1st"

        # 2nd level error schema
        error.errors = {
            "1st": ValidationError(
                "bad 1st", errors={"2nd": ValidationError("bad 2nd")}
            )
        }
        assert "1st" in error.to_dict()
        assert isinstance(error.to_dict()["1st"], dict)
        assert "2nd" in error.to_dict()["1st"]
        assert error.to_dict()["1st"]["2nd"] == "bad 2nd"

        # moar levels
        error.errors = {
            "1st": ValidationError(
                "bad 1st",
                errors={
                    "2nd": ValidationError(
                        "bad 2nd",
                        errors={
                            "3rd": ValidationError(
                                "bad 3rd", errors={"4th": ValidationError("Inception")}
                            )
                        },
                    )
                },
            )
        }
        assert "1st" in error.to_dict()
        assert "2nd" in error.to_dict()["1st"]
        assert "3rd" in error.to_dict()["1st"]["2nd"]
        assert "4th" in error.to_dict()["1st"]["2nd"]["3rd"]
        assert error.to_dict()["1st"]["2nd"]["3rd"]["4th"] == "Inception"

        assert error.message == "root(2nd.3rd.4th.Inception: ['1st'])"

    def test_model_validation(self):
        class User(Document):
            username = StringField(primary_key=True)
            name = StringField(required=True)

        try:
            User().validate()
        except ValidationError as e:
            assert "User:None" in e.message
            assert e.to_dict() == {
                "username": "Field is required",
                "name": "Field is required",
            }

        user = User(username="RossC0", name="Ross").save()
        user.name = None
        try:
            user.save()
        except ValidationError as e:
            assert "User:RossC0" in e.message
            assert e.to_dict() == {"name": "Field is required"}

    def test_fields_rewrite(self):
        class BasePerson(Document):
            name = StringField()
            age = IntField()
            meta = {"abstract": True}

        class Person(BasePerson):
            name = StringField(required=True)

        p = Person(age=15)
        with pytest.raises(ValidationError):
            p.validate()

    def test_embedded_document_validation(self):
        """Ensure that embedded documents may be validated."""

        class Comment(EmbeddedDocument):
            date = DateTimeField()
            content = StringField(required=True)

        comment = Comment()
        with pytest.raises(ValidationError):
            comment.validate()

        comment.content = "test"
        comment.validate()

        comment.date = 4
        with pytest.raises(ValidationError):
            comment.validate()

        comment.date = datetime.now()
        comment.validate()
        assert comment._instance is None

    def test_embedded_db_field_validate(self):
        class SubDoc(EmbeddedDocument):
            val = IntField(required=True)

        class Doc(Document):
            id = StringField(primary_key=True)
            e = EmbeddedDocumentField(SubDoc, db_field="eb")

        try:
            Doc(id="bad").validate()
        except ValidationError as e:
            assert "SubDoc:None" in e.message
            assert e.to_dict() == {"e": {"val": "OK could not be converted to int"}}

        Doc.drop_collection()

        Doc(id="test", e=SubDoc(val=15)).save()

        doc = Doc.objects.first()
        keys = doc._data.keys()
        assert 2 == len(keys)
        assert "e" in keys
        assert "id" in keys

        doc.e.val = "OK"
        try:
            doc.save()
        except ValidationError as e:
            assert "Doc:test" in e.message
            assert e.to_dict() == {"e": {"val": "OK could not be converted to int"}}

    def test_embedded_weakref(self):
        class SubDoc(EmbeddedDocument):
            val = IntField(required=True)

        class Doc(Document):
            e = EmbeddedDocumentField(SubDoc, db_field="eb")

        Doc.drop_collection()

        d1 = Doc()
        d2 = Doc()

        s = SubDoc()

        with pytest.raises(ValidationError):
            s.validate()

        d1.e = s
        d2.e = s

        del d1

        with pytest.raises(ValidationError):
            d2.validate()

    def test_parent_reference_in_child_document(self):
        """
        Test to ensure a ReferenceField can store a reference to a parent
        class when inherited. Issue #954.
        """

        class Parent(Document):
            meta = {"allow_inheritance": True}
            reference = ReferenceField("self")

        class Child(Parent):
            pass

        parent = Parent()
        parent.save()

        child = Child(reference=parent)

        # Saving child should not raise a ValidationError
        try:
            child.save()
        except ValidationError as e:
            self.fail("ValidationError raised: %s" % e.message)

    def test_parent_reference_set_as_attribute_in_child_document(self):
        """
        Test to ensure a ReferenceField can store a reference to a parent
        class when inherited and when set via attribute. Issue #954.
        """

        class Parent(Document):
            meta = {"allow_inheritance": True}
            reference = ReferenceField("self")

        class Child(Parent):
            pass

        parent = Parent()
        parent.save()

        child = Child()
        child.reference = parent

        # Saving the child should not raise a ValidationError
        child.save()


if __name__ == "__main__":
    unittest.main()
