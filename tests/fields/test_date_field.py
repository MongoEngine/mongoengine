# -*- coding: utf-8 -*-
import datetime
import six

try:
    import dateutil
except ImportError:
    dateutil = None

from mongoengine import *

from tests.utils import MongoDBTestCase


class TestDateField(MongoDBTestCase):
    def test_date_from_empty_string(self):
        """
        Ensure an exception is raised when trying to
        cast an empty string to datetime.
        """
        class MyDoc(Document):
            dt = DateField()

        md = MyDoc(dt='')
        self.assertRaises(ValidationError, md.save)

    def test_date_from_whitespace_string(self):
        """
        Ensure an exception is raised when trying to
        cast a whitespace-only string to datetime.
        """
        class MyDoc(Document):
            dt = DateField()

        md = MyDoc(dt='   ')
        self.assertRaises(ValidationError, md.save)

    def test_default_values_today(self):
        """Ensure that default field values are used when creating
        a document.
        """
        class Person(Document):
            day = DateField(default=datetime.date.today)

        person = Person()
        person.validate()
        self.assertEqual(person.day, person.day)
        self.assertEqual(person.day, datetime.date.today())
        self.assertEqual(person._data['day'], person.day)

    def test_date(self):
        """Tests showing pymongo date fields

        See: http://api.mongodb.org/python/current/api/bson/son.html#dt
        """
        class LogEntry(Document):
            date = DateField()

        LogEntry.drop_collection()

        # Test can save dates
        log = LogEntry()
        log.date = datetime.date.today()
        log.save()
        log.reload()
        self.assertEqual(log.date, datetime.date.today())

        d1 = datetime.datetime(1970, 1, 1, 0, 0, 1, 999)
        d2 = datetime.datetime(1970, 1, 1, 0, 0, 1)
        log = LogEntry()
        log.date = d1
        log.save()
        log.reload()
        self.assertEqual(log.date, d1.date())
        self.assertEqual(log.date, d2.date())

        d1 = datetime.datetime(1970, 1, 1, 0, 0, 1, 9999)
        d2 = datetime.datetime(1970, 1, 1, 0, 0, 1, 9000)
        log.date = d1
        log.save()
        log.reload()
        self.assertEqual(log.date, d1.date())
        self.assertEqual(log.date, d2.date())

        if not six.PY3:
            # Pre UTC dates microseconds below 1000 are dropped
            # This does not seem to be true in PY3
            d1 = datetime.datetime(1969, 12, 31, 23, 59, 59, 999)
            d2 = datetime.datetime(1969, 12, 31, 23, 59, 59)
            log.date = d1
            log.save()
            log.reload()
            self.assertEqual(log.date, d1.date())
            self.assertEqual(log.date, d2.date())

    def test_regular_usage(self):
        """Tests for regular datetime fields"""
        class LogEntry(Document):
            date = DateField()

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

    def test_validation(self):
        """Ensure that invalid values cannot be assigned to datetime
        fields.
        """
        class LogEntry(Document):
            time = DateField()

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
