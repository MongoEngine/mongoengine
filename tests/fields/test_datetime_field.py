# -*- coding: utf-8 -*-
import datetime
import six

try:
    import dateutil
except ImportError:
    dateutil = None

from mongoengine import *
from mongoengine import connection

from tests.utils import MongoDBTestCase


class TestDateTimeField(MongoDBTestCase):
    def test_datetime_from_empty_string(self):
        """
        Ensure an exception is raised when trying to
        cast an empty string to datetime.
        """
        class MyDoc(Document):
            dt = DateTimeField()

        md = MyDoc(dt='')
        self.assertRaises(ValidationError, md.save)

    def test_datetime_from_whitespace_string(self):
        """
        Ensure an exception is raised when trying to
        cast a whitespace-only string to datetime.
        """
        class MyDoc(Document):
            dt = DateTimeField()

        md = MyDoc(dt='   ')
        self.assertRaises(ValidationError, md.save)

    def test_default_value_utcnow(self):
        """Ensure that default field values are used when creating
        a document.
        """
        class Person(Document):
            created = DateTimeField(default=datetime.datetime.utcnow)

        utcnow = datetime.datetime.utcnow()
        person = Person()
        person.validate()
        person_created_t0 = person.created
        self.assertLess(person.created - utcnow, datetime.timedelta(seconds=1))
        self.assertEqual(person_created_t0, person.created)  # make sure it does not change
        self.assertEqual(person._data['created'], person.created)

    def test_handling_microseconds(self):
        """Tests showing pymongo datetime fields handling of microseconds.
        Microseconds are rounded to the nearest millisecond and pre UTC
        handling is wonky.

        See: http://api.mongodb.org/python/current/api/bson/son.html#dt
        """
        class LogEntry(Document):
            date = DateTimeField()

        LogEntry.drop_collection()

        # Test can save dates
        log = LogEntry()
        log.date = datetime.date.today()
        log.save()
        log.reload()
        self.assertEqual(log.date.date(), datetime.date.today())

        # Post UTC - microseconds are rounded (down) nearest millisecond and
        # dropped
        d1 = datetime.datetime(1970, 1, 1, 0, 0, 1, 999)
        d2 = datetime.datetime(1970, 1, 1, 0, 0, 1)
        log = LogEntry()
        log.date = d1
        log.save()
        log.reload()
        self.assertNotEqual(log.date, d1)
        self.assertEqual(log.date, d2)

        # Post UTC - microseconds are rounded (down) nearest millisecond
        d1 = datetime.datetime(1970, 1, 1, 0, 0, 1, 9999)
        d2 = datetime.datetime(1970, 1, 1, 0, 0, 1, 9000)
        log.date = d1
        log.save()
        log.reload()
        self.assertNotEqual(log.date, d1)
        self.assertEqual(log.date, d2)

        if not six.PY3:
            # Pre UTC dates microseconds below 1000 are dropped
            # This does not seem to be true in PY3
            d1 = datetime.datetime(1969, 12, 31, 23, 59, 59, 999)
            d2 = datetime.datetime(1969, 12, 31, 23, 59, 59)
            log.date = d1
            log.save()
            log.reload()
            self.assertNotEqual(log.date, d1)
            self.assertEqual(log.date, d2)

    def test_regular_usage(self):
        """Tests for regular datetime fields"""
        class LogEntry(Document):
            date = DateTimeField()

        LogEntry.drop_collection()

        d1 = datetime.datetime(1970, 1, 1, 0, 0, 1)
        log = LogEntry()
        log.date = d1
        log.validate()
        log.save()

        for query in (d1, d1.isoformat(' ')):
            log1 = LogEntry.objects.get(date=query)
            self.assertEqual(log, log1)

        if dateutil:
            log1 = LogEntry.objects.get(date=d1.isoformat('T'))
            self.assertEqual(log, log1)

        # create additional 19 log entries for a total of 20
        for i in range(1971, 1990):
            d = datetime.datetime(i, 1, 1, 0, 0, 1)
            LogEntry(date=d).save()

        self.assertEqual(LogEntry.objects.count(), 20)

        # Test ordering
        logs = LogEntry.objects.order_by("date")
        i = 0
        while i < 19:
            self.assertTrue(logs[i].date <= logs[i + 1].date)
            i += 1

        logs = LogEntry.objects.order_by("-date")
        i = 0
        while i < 19:
            self.assertTrue(logs[i].date >= logs[i + 1].date)
            i += 1

        # Test searching
        logs = LogEntry.objects.filter(date__gte=datetime.datetime(1980, 1, 1))
        self.assertEqual(logs.count(), 10)

        logs = LogEntry.objects.filter(date__lte=datetime.datetime(1980, 1, 1))
        self.assertEqual(logs.count(), 10)

        logs = LogEntry.objects.filter(
            date__lte=datetime.datetime(1980, 1, 1),
            date__gte=datetime.datetime(1975, 1, 1),
        )
        self.assertEqual(logs.count(), 5)

    def test_datetime_validation(self):
        """Ensure that invalid values cannot be assigned to datetime
        fields.
        """
        class LogEntry(Document):
            time = DateTimeField()

        log = LogEntry()
        log.time = datetime.datetime.now()
        log.validate()

        log.time = datetime.date.today()
        log.validate()

        log.time = datetime.datetime.now().isoformat(' ')
        log.validate()

        if dateutil:
            log.time = datetime.datetime.now().isoformat('T')
            log.validate()

        log.time = -1
        self.assertRaises(ValidationError, log.validate)
        log.time = 'ABC'
        self.assertRaises(ValidationError, log.validate)


class TestDateTimeTzAware(MongoDBTestCase):
    def test_datetime_tz_aware_mark_as_changed(self):
        # Reset the connections
        connection._connection_settings = {}
        connection._connections = {}
        connection._dbs = {}

        connect(db='mongoenginetest', tz_aware=True)

        class LogEntry(Document):
            time = DateTimeField()

        LogEntry.drop_collection()

        LogEntry(time=datetime.datetime(2013, 1, 1, 0, 0, 0)).save()

        log = LogEntry.objects.first()
        log.time = datetime.datetime(2013, 1, 1, 0, 0, 0)
        self.assertEqual(['time'], log._changed_fields)
