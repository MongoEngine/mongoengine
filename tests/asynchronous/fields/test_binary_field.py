import uuid

import pytest
from bson import Binary

from mongoengine import *
from tests.asynchronous.utils import MongoDBAsyncTestCase

BIN_VALUE = "\xa9\xf3\x8d(\xd7\x03\x84\xb4k[\x0f\xe3\xa2\x19\x85p[J\xa3\xd2>\xde\xe6\x87\xb1\x7f\xc6\xe6\xd9r\x18\xf5".encode(
    "latin-1"
)


class TestBinaryField(MongoDBAsyncTestCase):
    async def test_binary_fields(self):
        """Ensure that binary fields can be stored and retrieved."""

        class Attachment(Document):
            content_type = StringField()
            blob = BinaryField()

        BLOB = b"\xe6\x00\xc4\xff\x07"
        MIME_TYPE = "application/octet-stream"

        await Attachment.adrop_collection()

        attachment = Attachment(content_type=MIME_TYPE, blob=BLOB)
        await attachment.asave()

        attachment_1 = await Attachment.aobjects().first()
        assert MIME_TYPE == attachment_1.content_type
        assert BLOB == bytes(attachment_1.blob)

    async def test_bytearray_conversion_to_bytes(self):
        class Dummy(Document):
            blob = BinaryField()

        byte_arr = bytearray(b"\x00\x00\x00\x00\x00")
        dummy = Dummy(blob=byte_arr)
        assert isinstance(dummy.blob, bytes)

    async def test_validation_succeeds(self):
        """Ensure that valid values can be assigned to binary fields."""

        class AttachmentRequired(Document):
            blob = BinaryField(required=True)

        class AttachmentSizeLimit(Document):
            blob = BinaryField(max_bytes=4)

        attachment_required = AttachmentRequired()
        with pytest.raises(ValidationError):
            attachment_required.validate()
        attachment_required.blob = Binary(b"\xe6\x00\xc4\xff\x07")
        attachment_required.validate()

        _5_BYTES = b"\xe6\x00\xc4\xff\x07"
        _4_BYTES = b"\xe6\x00\xc4\xff"
        with pytest.raises(ValidationError):
            AttachmentSizeLimit(blob=_5_BYTES).validate()
        AttachmentSizeLimit(blob=_4_BYTES).validate()

    async def test_validation_fails(self):
        """Ensure that invalid values cannot be assigned to binary fields."""

        class Attachment(Document):
            blob = BinaryField()

        for invalid_data in (2, "Im_a_unicode", ["some_str"]):
            with pytest.raises(ValidationError):
                Attachment(blob=invalid_data).validate()

    async def test__primary(self):
        class Attachment(Document):
            id = BinaryField(primary_key=True)

        await Attachment.adrop_collection()
        binary_id = uuid.uuid4().bytes
        att = await Attachment(id=binary_id).asave()
        assert 1 == await Attachment.aobjects.count()
        assert 1 == await Attachment.aobjects.filter(id=att.id).count()
        await att.adelete()
        assert 0 == await Attachment.aobjects.count()

    async def test_primary_filter_by_binary_pk_as_str(self):
        class Attachment(Document):
            id = BinaryField(primary_key=True)

        await Attachment.adrop_collection()
        binary_id = uuid.uuid4().bytes
        att = await Attachment(id=binary_id).asave()
        assert 1 == await Attachment.aobjects.filter(id=binary_id).count()
        await att.adelete()
        assert 0 == await Attachment.aobjects.count()

    async def test_match_querying_with_bytes(self):
        class MyDocument(Document):
            bin_field = BinaryField()

        await MyDocument.adrop_collection()

        doc = await MyDocument(bin_field=BIN_VALUE).asave()
        matched_doc = await MyDocument.aobjects(bin_field=BIN_VALUE).first()
        assert matched_doc.id == doc.id

    async def test_match_querying_with_binary(self):
        class MyDocument(Document):
            bin_field = BinaryField()

        await MyDocument.adrop_collection()

        doc = await MyDocument(bin_field=BIN_VALUE).asave()

        matched_doc = await MyDocument.aobjects(bin_field=Binary(BIN_VALUE)).first()
        assert matched_doc.id == doc.id

    async def test_modify_operation__set(self):
        """Ensures no regression of bug #1127"""

        class MyDocument(Document):
            some_field = StringField()
            bin_field = BinaryField()

        await MyDocument.adrop_collection()

        doc = await MyDocument.aobjects(some_field="test").modify(
            upsert=True, new=True, set__bin_field=BIN_VALUE
        )
        assert doc.some_field == "test"
        assert doc.bin_field == BIN_VALUE

    async def test_update_one(self):
        """Ensures no regression of bug #1127"""

        class MyDocument(Document):
            bin_field = BinaryField()

        await MyDocument.adrop_collection()

        bin_data = b"\xe6\x00\xc4\xff\x07"
        doc = await MyDocument(bin_field=bin_data).asave()

        n_updated = await MyDocument.aobjects(bin_field=bin_data).update_one(
            bin_field=BIN_VALUE
        )
        assert n_updated == 1
        fetched = await MyDocument.aobjects.with_id(doc.id)
        assert fetched.bin_field == BIN_VALUE
