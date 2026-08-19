import uuid

import pytest
from bson.binary import UuidRepresentation

from mongoengine import *
from mongoengine.connection import disconnect, get_connection, get_db
from tests.utils import MONGO_TEST_DB, MongoDBTestCase, get_as_pymongo

LEGACY_ALIAS = "uuid-python-legacy"
STANDARD_ALIAS = "uuid-standard"
UNSPECIFIED_ALIAS = "uuid-unspecified"
UUID_COLLECTION = "uuid_representation"
UUID_PRIMARY_KEY_COLLECTION = "uuid_primary_key_representation"


class LegacyUUIDDocument(Document):
    identifier = UUIDField()

    meta = {"collection": UUID_COLLECTION, "db_alias": LEGACY_ALIAS}


class StandardUUIDDocument(Document):
    identifier = UUIDField()

    meta = {"collection": UUID_COLLECTION, "db_alias": STANDARD_ALIAS}


class UnspecifiedUUIDDocument(Document):
    identifier = UUIDField()

    meta = {"collection": UUID_COLLECTION, "db_alias": UNSPECIFIED_ALIAS}


class UnspecifiedStringUUIDDocument(Document):
    identifier = UUIDField(binary=False)

    meta = {"collection": "uuid_string_representation", "db_alias": UNSPECIFIED_ALIAS}


class LegacyUUIDPrimaryKeyDocument(Document):
    id = UUIDField(primary_key=True)

    meta = {"collection": UUID_PRIMARY_KEY_COLLECTION, "db_alias": LEGACY_ALIAS}


class UnspecifiedUUIDPrimaryKeyDocument(Document):
    id = UUIDField(primary_key=True)

    meta = {
        "collection": UUID_PRIMARY_KEY_COLLECTION,
        "db_alias": UNSPECIFIED_ALIAS,
    }


class Person(Document):
    api_key = UUIDField(binary=False)


class TestUUIDField(MongoDBTestCase):
    def test_storage(self):
        uid = uuid.uuid4()
        person = Person(api_key=uid).save()
        assert get_as_pymongo(person) == {"_id": person.id, "api_key": str(uid)}

    def test_field_string(self):
        """Test UUID fields storing as String"""
        Person.drop_collection()

        uu = uuid.uuid4()
        Person(api_key=uu).save()
        assert 1 == Person.objects(api_key=uu).count()
        assert uu == Person.objects.first().api_key

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

    def test_field_binary(self):
        """Test UUID fields storing as Binary object."""
        Person.drop_collection()

        uu = uuid.uuid4()
        Person(api_key=uu).save()
        assert 1 == Person.objects(api_key=uu).count()
        assert uu == Person.objects.first().api_key

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


class TestUUIDRepresentation(MongoDBTestCase):
    def setUp(self):
        connect(
            db=MONGO_TEST_DB,
            alias=LEGACY_ALIAS,
            uuidRepresentation="pythonLegacy",
        )
        connect(
            db=MONGO_TEST_DB,
            alias=STANDARD_ALIAS,
            uuidRepresentation="standard",
        )
        connect(db=MONGO_TEST_DB, alias=UNSPECIFIED_ALIAS)

        LegacyUUIDDocument.drop_collection()
        LegacyUUIDPrimaryKeyDocument.drop_collection()
        UnspecifiedStringUUIDDocument.drop_collection()

        self.identifier = uuid.uuid4()
        self.document = LegacyUUIDDocument(identifier=self.identifier).save()

    def tearDown(self):
        LegacyUUIDDocument.drop_collection()
        LegacyUUIDPrimaryKeyDocument.drop_collection()
        UnspecifiedStringUUIDDocument.drop_collection()
        disconnect(LEGACY_ALIAS)
        disconnect(STANDARD_ALIAS)
        disconnect(UNSPECIFIED_ALIAS)

    def test_unspecified__legacy_uuid_exists__fails_on_read_write_and_query(self):
        connection = get_connection(UNSPECIFIED_ALIAS)
        assert (
            connection.options.codec_options.uuid_representation
            == UuidRepresentation.UNSPECIFIED
        )

        with pytest.raises(ValidationError, match="BSON UUID"):
            UnspecifiedUUIDDocument.objects.first()

        with pytest.raises(ValueError, match="cannot encode native uuid.UUID"):
            UnspecifiedUUIDDocument(identifier=uuid.uuid4()).save()

        with pytest.raises(ValueError, match="cannot encode native uuid.UUID"):
            UnspecifiedUUIDDocument.objects(identifier=self.identifier).first()

    def test_python_legacy__legacy_uuid_exists__reads_uuid(self):
        document = LegacyUUIDDocument.objects.get(id=self.document.id)

        assert document.identifier == self.identifier
        assert isinstance(document.identifier, uuid.UUID)

    def test_standard__legacy_uuid_is_migrated__reads_uuid(self):
        with pytest.raises(ValidationError, match="BSON UUID"):
            StandardUUIDDocument.objects.first()

        legacy_collection = get_db(LEGACY_ALIAS)[UUID_COLLECTION]
        standard_collection = get_db(STANDARD_ALIAS)[UUID_COLLECTION]
        legacy_document = legacy_collection.find_one({"_id": self.document.id})
        standard_collection.update_one(
            {"_id": self.document.id},
            {"$set": {"identifier": legacy_document["identifier"]}},
        )

        migrated_document = StandardUUIDDocument.objects.get(id=self.document.id)
        assert migrated_document.identifier == self.identifier
        assert isinstance(migrated_document.identifier, uuid.UUID)

    def test_unspecified__uuid_is_stored_as_string__reads_and_writes_uuid(self):
        document = UnspecifiedStringUUIDDocument(identifier=self.identifier).save()

        assert (
            UnspecifiedStringUUIDDocument.objects.get(id=document.id).identifier
            == self.identifier
        )

    def test_unspecified__legacy_uuid_primary_key_exists__fails_on_read(self):
        LegacyUUIDPrimaryKeyDocument(id=self.identifier).save()

        with pytest.raises(ValidationError, match="BSON UUID"):
            UnspecifiedUUIDPrimaryKeyDocument.objects.first()

        with pytest.raises(ValueError, match="cannot encode native uuid.UUID"):
            UnspecifiedUUIDPrimaryKeyDocument.objects.get(id=self.identifier)
