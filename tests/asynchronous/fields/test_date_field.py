import datetime

import pytest

try:
    import dateutil
except ImportError:
    dateutil = None

from mongoengine import *
from tests.asynchronous.utils import MongoDBAsyncTestCase


class TestDateField(MongoDBAsyncTestCase):
    async def test_date_from_empty_string(self):
        """
        Ensure an exception is raised when trying to
        cast an empty string to datetime.
        """

        class MyDoc(Document):
            dt = DateField()

        md = MyDoc(dt="")
        with pytest.raises(ValidationError):
            await md.asave()

    async def test_date_from_whitespace_string(self):
        """
        Ensure an exception is raised when trying to
        cast a whitespace-only string to datetime.
        """

        class MyDoc(Document):
            dt = DateField()

        md = MyDoc(dt="   ")
        with pytest.raises(ValidationError):
            await md.asave()

    async def test_default_values_today(self):
        """Ensure that default field values are used when creating
        a document.
        """

        class Person(Document):
            day = DateField(default=datetime.date.today)

        person = Person()
        person.validate()
        assert person.day == person.day
        assert person.day == datetime.date.today()
        assert person._data["day"] == person.day

    async def test_date(self):
        """Tests showing pymongo date fields

        See: http://api.mongodb.org/python/current/api/bson/son.html#dt
        """

        class LogEntry(Document):
            date = DateField()

        await LogEntry.adrop_collection()

        # Test can save dates
        log = LogEntry()
        log.date = datetime.date.today()
        await log.asave()
        await log.areload()
        assert log.date == datetime.date.today()

        d1 = datetime.datetime(1970, 1, 1, 0, 0, 1, 999)
        d2 = datetime.datetime(1970, 1, 1, 0, 0, 1)
        log = LogEntry()
        log.date = d1
        await log.asave()
        await log.areload()
        assert log.date == d1.date()
        assert log.date == d2.date()

        d1 = datetime.datetime(1970, 1, 1, 0, 0, 1, 9999)
        d2 = datetime.datetime(1970, 1, 1, 0, 0, 1, 9000)
        log.date = d1
        await log.asave()
        await log.areload()
        assert log.date == d1.date()
        assert log.date == d2.date()

    async def test_regular_usage(self):
        """Tests for regular datetime fields"""

        class LogEntry(Document):
            date = DateField()

        await LogEntry.adrop_collection()

        d1 = datetime.datetime(1970, 1, 1, 0, 0, 1)
        log = LogEntry()
        log.date = d1
        log.validate()
        await log.asave()

        for query in (d1, d1.isoformat(" ")):
            log1 = await LogEntry.aobjects.get(date=query)
            assert log == log1

        if dateutil:
            log1 = await LogEntry.aobjects.get(date=d1.isoformat("T"))
            assert log == log1

        # create additional 19 log entries for a total of 20
        for i in range(1971, 1990):
            d = datetime.datetime(i, 1, 1, 0, 0, 1)
            await LogEntry(date=d).asave()

        assert await LogEntry.aobjects.count() == 20

        # Test ordering
        logs = await LogEntry.aobjects.order_by("date").to_list()
        i = 0
        while i < 19:
            assert logs[i].date <= logs[i + 1].date
            i += 1

        logs = await LogEntry.aobjects.order_by("-date").to_list()
        i = 0
        while i < 19:
            assert logs[i].date >= logs[i + 1].date
            i += 1

        # Test searching
        logs = LogEntry.aobjects.filter(date__gte=datetime.datetime(1980, 1, 1))
        assert await logs.count() == 10

    async def test_validation(self):
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

        log.time = datetime.datetime.now().isoformat(" ")
        log.validate()

        if dateutil:
            log.time = datetime.datetime.now().isoformat("T")
            log.validate()

        log.time = -1
        with pytest.raises(ValidationError):
            log.validate()
        log.time = "ABC"
        with pytest.raises(ValidationError):
            log.validate()
