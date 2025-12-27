from datetime import datetime

import pytest

from mongoengine import *
from tests.asynchronous.utils import MongoDBAsyncTestCase


class TestValidatorError(MongoDBAsyncTestCase):
    async def test_to_dict(self):
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

    async def test_model_validation(self):
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

        user = await User(username="RossC0", name="Ross").asave()
        user.name = None
        try:
            await user.asave()
        except ValidationError as e:
            assert "User:RossC0" in e.message
            assert e.to_dict() == {"name": "Field is required"}

    async def test_fields_rewrite(self):
        class BasePerson(Document):
            name = StringField()
            age = IntField()
            meta = {"abstract": True}

        class Person(BasePerson):
            name = StringField(required=True)

        p = Person(age=15)
        with pytest.raises(ValidationError):
            p.validate()

    async def test_embedded_document_validation(self):
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

    async def test_embedded_db_field_validate(self):
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

        await Doc.adrop_collection()

        await Doc(id="test", e=SubDoc(val=15)).asave()

        doc = await Doc.aobjects.first()
        keys = doc._data.keys()
        assert 2 == len(keys)
        assert "e" in keys
        assert "id" in keys

        doc.e.val = "OK"
        try:
            await doc.asave()
        except ValidationError as e:
            assert "Doc:test" in e.message
            assert e.to_dict() == {"e": {"val": "OK could not be converted to int"}}

    async def test_embedded_weakref(self):
        class SubDoc(EmbeddedDocument):
            val = IntField(required=True)

        class Doc(Document):
            e = EmbeddedDocumentField(SubDoc, db_field="eb")

        await Doc.adrop_collection()

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

    async def test_parent_reference_in_child_document(self):
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
        await parent.asave()

        child = Child(reference=parent)

        # Saving child should not raise a ValidationError
        try:
            await child.asave()
        except ValidationError as e:
            self.fail("ValidationError raised: %s" % e.message)

    async def test_parent_reference_set_as_attribute_in_child_document(self):
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
        await parent.asave()

        child = Child()
        child.reference = parent

        # Saving the child should not raise a ValidationError
        await child.asave()


