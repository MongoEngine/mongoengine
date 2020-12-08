import datetime
import itertools
import math
import re

import pytest

from mongoengine import *

from tests.utils import MongoDBTestCase


class ComplexDateTimeFieldTest(MongoDBTestCase):
    def test_complexdatetime_storage(self):
        """Tests for complex datetime fields - which can handle
        microseconds without rounding.
        """

        class LogEntry(Document):
            date = ComplexDateTimeField()
            date_with_dots = ComplexDateTimeField(separator=".")

        LogEntry.drop_collection()

        # Post UTC - microseconds are rounded (down) nearest millisecond and
        # dropped - with default datetimefields
        d1 = datetime.datetime(1970, 1, 1, 0, 0, 1, 999)
        log = LogEntry()
        log.date = d1
        log.save()
        log.reload()
        assert log.date == d1

        # Post UTC - microseconds are rounded (down) nearest millisecond - with
        # default datetimefields
        d1 = datetime.datetime(1970, 1, 1, 0, 0, 1, 9999)
        log.date = d1
        log.save()
        log.reload()
        assert log.date == d1

        # Pre UTC dates microseconds below 1000 are dropped - with default
        # datetimefields
        d1 = datetime.datetime(1969, 12, 31, 23, 59, 59, 999)
        log.date = d1
        log.save()
        log.reload()
        assert log.date == d1

        # Pre UTC microseconds above 1000 is wonky - with default datetimefields
        # log.date has an invalid microsecond value so I can't construct
        # a date to compare.
        for i in range(1001, 3113, 33):
            d1 = datetime.datetime(1969, 12, 31, 23, 59, 59, i)
            log.date = d1
            log.save()
            log.reload()
            assert log.date == d1
            log1 = LogEntry.objects.get(date=d1)
            assert log == log1

        # Test string padding
        microsecond = map(int, (math.pow(10, x) for x in range(6)))
        mm = dd = hh = ii = ss = [1, 10]

        for values in itertools.product([2014], mm, dd, hh, ii, ss, microsecond):
            stored = LogEntry(date=datetime.datetime(*values)).to_mongo()["date"]
            assert (
                re.match(r"^\d{4},\d{2},\d{2},\d{2},\d{2},\d{2},\d{6}$", stored)
                is not None
            )

        # Test separator
        stored = LogEntry(date_with_dots=datetime.datetime(2014, 1, 1)).to_mongo()[
            "date_with_dots"
        ]
        assert (
            re.match(r"^\d{4}.\d{2}.\d{2}.\d{2}.\d{2}.\d{2}.\d{6}$", stored) is not None
        )

    def test_complexdatetime_usage(self):
        """Tests for complex datetime fields - which can handle
        microseconds without rounding.
        """

        class LogEntry(Document):
            date = ComplexDateTimeField()

        LogEntry.drop_collection()

        d1 = datetime.datetime(1950, 1, 1, 0, 0, 1, 999)
        log = LogEntry()
        log.date = d1
        log.save()

        log1 = LogEntry.objects.get(date=d1)
        assert log == log1

        # create extra 59 log entries for a total of 60
        for i in range(1951, 2010):
            d = datetime.datetime(i, 1, 1, 0, 0, 1, 999)
            LogEntry(date=d).save()

        assert LogEntry.objects.count() == 60

        # Test ordering
        logs = LogEntry.objects.order_by("date")
        i = 0
        while i < 59:
            assert logs[i].date <= logs[i + 1].date
            i += 1

        logs = LogEntry.objects.order_by("-date")
        i = 0
        while i < 59:
            assert logs[i].date >= logs[i + 1].date
            i += 1

        # Test searching
        logs = LogEntry.objects.filter(date__gte=datetime.datetime(1980, 1, 1))
        assert logs.count() == 30

        logs = LogEntry.objects.filter(date__lte=datetime.datetime(1980, 1, 1))
        assert logs.count() == 30

        logs = LogEntry.objects.filter(
            date__lte=datetime.datetime(2011, 1, 1),
            date__gte=datetime.datetime(2000, 1, 1),
        )
        assert logs.count() == 10

        LogEntry.drop_collection()

        # Test microsecond-level ordering/filtering
        for microsecond in (99, 999, 9999, 10000):
            LogEntry(date=datetime.datetime(2015, 1, 1, 0, 0, 0, microsecond)).save()

        logs = list(LogEntry.objects.order_by("date"))
        for next_idx, log in enumerate(logs[:-1], start=1):
            next_log = logs[next_idx]
            assert log.date < next_log.date

        logs = list(LogEntry.objects.order_by("-date"))
        for next_idx, log in enumerate(logs[:-1], start=1):
            next_log = logs[next_idx]
            assert log.date > next_log.date

        logs = LogEntry.objects.filter(
            date__lte=datetime.datetime(2015, 1, 1, 0, 0, 0, 10000)
        )
        assert logs.count() == 4

    def test_no_default_value(self):
        class Log(Document):
            timestamp = ComplexDateTimeField()

        Log.drop_collection()

        log = Log()
        assert log.timestamp is None
        log.save()

        fetched_log = Log.objects.with_id(log.id)
        assert fetched_log.timestamp is None

    def test_default_static_value(self):
        NOW = datetime.datetime.utcnow()

        class Log(Document):
            timestamp = ComplexDateTimeField(default=NOW)

        Log.drop_collection()

        log = Log()
        assert log.timestamp == NOW
        log.save()

        fetched_log = Log.objects.with_id(log.id)
        assert fetched_log.timestamp == NOW

    def test_default_callable(self):
        NOW = datetime.datetime.utcnow()

        class Log(Document):
            timestamp = ComplexDateTimeField(default=datetime.datetime.utcnow)

        Log.drop_collection()

        log = Log()
        assert log.timestamp >= NOW
        log.save()

        fetched_log = Log.objects.with_id(log.id)
        assert fetched_log.timestamp >= NOW

    def test_setting_bad_value_does_not_raise_unless_validate_is_called(self):
        # test regression of #2253

        class Log(Document):
            timestamp = ComplexDateTimeField()

        Log.drop_collection()

        log = Log(timestamp="garbage")
        with pytest.raises(ValidationError):
            log.validate()

        with pytest.raises(ValidationError):
            log.save()
