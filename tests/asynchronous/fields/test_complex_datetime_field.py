import datetime
import itertools
import math
import re

import pytest

from mongoengine import *
from tests.asynchronous.utils import MongoDBAsyncTestCase

try:
    # Python 3.11+
    from datetime import UTC
except ImportError:
    # Python ≤ 3.10
    from datetime import timezone
    UTC = timezone.utc


class ComplexDateTimeFieldTest(MongoDBAsyncTestCase):
    async def test_complexdatetime_storage(self):
        """Tests for complex datetime fields - which can handle
        microseconds without rounding.
        """

        class LogEntry(Document):
            date = ComplexDateTimeField()
            date_with_dots = ComplexDateTimeField(separator=".")

        await LogEntry.adrop_collection()

        # Post UTC - microseconds are rounded (down) nearest millisecond and
        # dropped - with default datetime fields
        d1 = datetime.datetime(1970, 1, 1, 0, 0, 1, 999,tzinfo=UTC)
        log = LogEntry()
        log.date = d1
        await log.asave()
        await log.areload()
        assert log.date == d1

        # Post UTC - microseconds are rounded (down) nearest millisecond - with
        # default datetime fields
        d1 = datetime.datetime(1970, 1, 1, 0, 0, 1, 9999,tzinfo=UTC)
        log.date = d1
        await log.asave()
        await log.areload()
        assert log.date == d1

        # Pre UTC dates microseconds below 1000 are dropped - with default
        # datetime fields
        d1 = datetime.datetime(1969, 12, 31, 23, 59, 59, 999,tzinfo=UTC)
        log.date = d1
        await log.asave()
        await log.areload()
        assert log.date == d1

        # Pre UTC microseconds above 1000 are wonky - with default datetime fields
        # log.date has an invalid microsecond value, so I can't construct
        # a date to compare.
        for i in range(1001, 3113, 33):
            d1 = datetime.datetime(1969, 12, 31, 23, 59, 59, i,tzinfo=UTC)
            log = LogEntry(
                date=d1
            )
            log.date = d1
            await log.asave()
            await log.areload()
            assert log.date == d1
            log1 = await LogEntry.aobjects.get(date=d1)
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

    async def test_complexdatetime_usage(self):
        """Tests for complex datetime fields - which can handle microseconds without rounding."""

        class LogEntry(Document):
            date = ComplexDateTimeField()

        await LogEntry.adrop_collection()

        d1 = datetime.datetime(1950, 1, 1, 0, 0, 1, 999)
        log = LogEntry()
        log.date = d1
        await log.asave()

        log1 = await LogEntry.aobjects.get(date=d1)
        assert log == log1

        # create extra 59 log entries for a total of 60
        for i in range(1951, 2010):
            d = datetime.datetime(i, 1, 1, 0, 0, 1, 999)
            await LogEntry(date=d).asave()

        assert await LogEntry.aobjects.count() == 60

        # Test ordering
        logs = await LogEntry.aobjects.order_by("date").to_list()
        i = 0
        while i < 59:
            assert logs[i].date <= logs[i + 1].date
            i += 1

        logs = await LogEntry.aobjects.order_by("-date").to_list()
        i = 0
        while i < 59:
            assert logs[i].date >= logs[i + 1].date
            i += 1

        # Test searching
        logs = LogEntry.aobjects.filter(date__gte=datetime.datetime(1980, 1, 1))
        assert await logs.count() == 30

        logs = LogEntry.aobjects.filter(date__lte=datetime.datetime(1980, 1, 1))
        assert await logs.count() == 30

        logs = LogEntry.aobjects.filter(
            date__lte=datetime.datetime(2011, 1, 1),
            date__gte=datetime.datetime(2000, 1, 1),
        )
        assert await logs.count() == 10

        await LogEntry.adrop_collection()

        # Test microsecond-level ordering/filtering
        for microsecond in (99, 999, 9999, 10000):
            await LogEntry(date=datetime.datetime(2015, 1, 1, 0, 0, 0, microsecond)).asave()

        logs = await LogEntry.aobjects.order_by("date").to_list()
        for next_idx, log in enumerate(logs[:-1], start=1):
            next_log = logs[next_idx]
            assert log.date < next_log.date

        logs = await LogEntry.aobjects.order_by("-date").to_list()
        for next_idx, log in enumerate(logs[:-1], start=1):
            next_log = logs[next_idx]
            assert log.date > next_log.date

        logs = LogEntry.aobjects.filter(
            date__lte=datetime.datetime(2015, 1, 1, 0, 0, 0, 10000)
        )
        assert await logs.count() == 4

    async def test_no_default_value(self):
        class Log(Document):
            timestamp = ComplexDateTimeField()

        await Log.adrop_collection()

        log = Log()
        assert log.timestamp is None
        await log.asave()

        fetched_log = await Log.aobjects.with_id(log.id)
        assert fetched_log.timestamp is None

    async def test_default_static_value(self):
        NOW = datetime.datetime.now(UTC)

        class Log(Document):
            timestamp = ComplexDateTimeField(default=NOW)

        await Log.adrop_collection()

        log = Log()
        assert log.timestamp == NOW
        await log.asave()

        fetched_log = await Log.aobjects.with_id(log.id)
        assert fetched_log.timestamp == NOW

    async def test_default_callable(self):
        NOW = datetime.datetime.now(UTC)

        class Log(Document):
            timestamp = ComplexDateTimeField(default=NOW)

        await Log.adrop_collection()

        log = Log()
        assert log.timestamp == NOW
        await log.asave()

        fetched_log = await Log.aobjects.with_id(log.id)
        assert fetched_log.timestamp >= NOW

    async def test_setting_bad_value_does_not_raise_unless_validate_is_called(self):
        # test regression of #2253

        class Log(Document):
            timestamp = ComplexDateTimeField()

        await Log.adrop_collection()

        log = Log(timestamp="garbage")
        with pytest.raises(ValidationError):
            log.validate()

        with pytest.raises(ValidationError):
            await log.asave()

    async def test_query_none_value_dont_raise(self):
        class Log(Document):
            timestamp = ComplexDateTimeField()

        _ = await Log.aobjects(timestamp=None).to_list()
