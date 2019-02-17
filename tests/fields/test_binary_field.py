# -*- coding: utf-8 -*-
import datetime
import unittest
import uuid
import math
import itertools
import re
import sys

from nose.plugins.skip import SkipTest
import six

try:
    import dateutil
except ImportError:
    dateutil = None

from bson import Binary

try:
    from bson.int64 import Int64
except ImportError:
    Int64 = long

from mongoengine import *
from tests.utils import MongoDBTestCase


class TestBinaryField(MongoDBTestCase):
    def test_binary_fields(self):
        """Ensure that binary fields can be stored and retrieved.
        """
        class Attachment(Document):
            content_type = StringField()
            blob = BinaryField()

        BLOB = six.b('\xe6\x00\xc4\xff\x07')
        MIME_TYPE = 'application/octet-stream'

        Attachment.drop_collection()

        attachment = Attachment(content_type=MIME_TYPE, blob=BLOB)
        attachment.save()

        attachment_1 = Attachment.objects().first()
        self.assertEqual(MIME_TYPE, attachment_1.content_type)
        self.assertEqual(BLOB, six.binary_type(attachment_1.blob))

    def test_binary_validation_succeeds(self):
        """Ensure that valid values can be assigned to binary fields.
        """
        class AttachmentRequired(Document):
            blob = BinaryField(required=True)

        class AttachmentSizeLimit(Document):
            blob = BinaryField(max_bytes=4)

        attachment_required = AttachmentRequired()
        self.assertRaises(ValidationError, attachment_required.validate)
        attachment_required.blob = Binary(six.b('\xe6\x00\xc4\xff\x07'))
        attachment_required.validate()

        _5_BYTES = six.b('\xe6\x00\xc4\xff\x07')
        _4_BYTES = six.b('\xe6\x00\xc4\xff')
        self.assertRaises(ValidationError, AttachmentSizeLimit(blob=_5_BYTES).validate)
        AttachmentSizeLimit(blob=_4_BYTES).validate()

    def test_binary_validation_fails(self):
        """Ensure that invalid values cannot be assigned to binary fields."""

        class Attachment(Document):
            blob = BinaryField()

        for invalid_data in (2, u'Im_a_unicode', ['some_str']):
            self.assertRaises(ValidationError, Attachment(blob=invalid_data).validate)

    def test_binary_field_primary(self):
        class Attachment(Document):
            id = BinaryField(primary_key=True)

        Attachment.drop_collection()
        binary_id = uuid.uuid4().bytes
        att = Attachment(id=binary_id).save()
        self.assertEqual(1, Attachment.objects.count())
        self.assertEqual(1, Attachment.objects.filter(id=att.id).count())
        att.delete()
        self.assertEqual(0, Attachment.objects.count())

    def test_binary_field_primary_filter_by_binary_pk_as_str(self):
        raise SkipTest("Querying by id as string is not currently supported")

        class Attachment(Document):
            id = BinaryField(primary_key=True)

        Attachment.drop_collection()
        binary_id = uuid.uuid4().bytes
        att = Attachment(id=binary_id).save()
        self.assertEqual(1, Attachment.objects.filter(id=binary_id).count())
        att.delete()
        self.assertEqual(0, Attachment.objects.count())
