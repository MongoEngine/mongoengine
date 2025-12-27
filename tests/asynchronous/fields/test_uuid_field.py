import uuid

import pytest

from mongoengine import *
from tests.asynchronous.utils import MongoDBAsyncTestCase, async_get_as_pymongo


class Person(Document):
    api_key = UUIDField(binary=False)


class TestUUIDField(MongoDBAsyncTestCase):


    async def test_storage(self):
        uid = uuid.uuid4()
        await Person.adrop_collection()
        person = await Person(api_key=uid).asave()
        assert await async_get_as_pymongo(person) == {"_id": person.id, "api_key": str(uid)}

    async def test_field_string(self):
        """Test UUID fields storing as String"""
        await Person.adrop_collection()

        uu = uuid.uuid4()
        await Person(api_key=uu).asave()
        assert 1 == await Person.aobjects(api_key=uu).count()
        assert uu == (await Person.aobjects.first()).api_key

        person = Person()
        valid = (uuid.uuid4(), uuid.uuid1())
        for api_key in valid:
            person.api_key = api_key
            person.validate()

        invalid = (
            "9d159858-549b-4975-9f98-dd2f987c113g",
            "9d159858-549b-4975-9f98-dd2f987c113",
        )
        for api_key in invalid:
            person.api_key = api_key
            with pytest.raises(ValidationError):
                person.validate()

    async def test_field_binary(self):
        """Test UUID fields storing as Binary object."""
        await Person.adrop_collection()

        uu = uuid.uuid4()
        await Person(api_key=uu).asave()
        assert 1 == await Person.aobjects(api_key=uu).count()
        assert uu == (await Person.aobjects.first()).api_key

        person = Person()
        valid = (uuid.uuid4(), uuid.uuid1())
        for api_key in valid:
            person.api_key = api_key
            person.validate()

        invalid = (
            "9d159858-549b-4975-9f98-dd2f987c113g",
            "9d159858-549b-4975-9f98-dd2f987c113",
        )
        for api_key in invalid:
            person.api_key = api_key
            with pytest.raises(ValidationError):
                person.validate()
