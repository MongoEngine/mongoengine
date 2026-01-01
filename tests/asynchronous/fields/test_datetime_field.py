import datetime
import datetime as dt

import pytest

from mongoengine import *
from mongoengine.asynchronous import async_connect, connection
from tests.asynchronous.utils import MongoDBAsyncTestCase, async_get_as_pymongo
from tests.utils import MONGO_TEST_DB

try:
    import dateutil
except ImportError:
    dateutil = None


class TestDateTimeField(MongoDBAsyncTestCase):
    async def test_datetime_from_empty_string(self):
        """
        Ensure an exception is raised when trying to
        cast an empty string to datetime.
        """

        class MyDoc(Document):
            dt = DateTimeField()

        md = MyDoc(dt="")
        with pytest.raises(ValidationError):
            await md.asave()

    async def test_datetime_from_whitespace_string(self):
        """
        Ensure an exception is raised when trying to
        cast a whitespace-only string to datetime.
        """

        class MyDoc(Document):
            dt = DateTimeField()

        md = MyDoc(dt="   ")
        with pytest.raises(ValidationError):
            await md.asave()

    async def test_default_value_utcnow(self):
        """Ensure that default field values are used when creating
        a document.
        """

        class Person(Document):
            created = DateTimeField(default=dt.datetime.now(datetime.UTC))

        utcnow = dt.datetime.now(datetime.UTC)
        person = Person()
        person.validate()
        person_created_t0 = person.created
        assert person.created - utcnow < dt.timedelta(seconds=1)
        assert person_created_t0 == person.created  # make sure it does not change
        assert person._data["created"] == person.created

    async def test_set_using_callable(self):
        # Weird feature but it's there for a while so let's make sure we don't break it
        class Person(Document):
            created = DateTimeField()

        await Person.adrop_collection()

        person = Person()
        frozen_dt = dt.datetime(2020, 7, 25, 9, 56, 1)
        person.created = lambda: frozen_dt
        await person.asave()

        assert callable(person.created)
        assert await async_get_as_pymongo(person) == {"_id": person.id, "created": frozen_dt}

    async def test_handling_microseconds(self):
        """Tests showing pymongo datetime fields handling of microseconds.
        Microseconds are rounded to the nearest millisecond and pre UTC
        handling is wonky.

        See: http://api.mongodb.org/python/current/api/bson/son.html#dt
        """

        class LogEntry(Document):
            date = DateTimeField()

        await LogEntry.adrop_collection()

        # Test can save dates
        log = LogEntry()
        log.date = dt.date.today()
        await log.asave()
        await log.areload()
        assert log.date.date() == dt.date.today()

        # Post UTC - microseconds are rounded (down) nearest millisecond and
        # dropped
        d1 = dt.datetime(1970, 1, 1, 0, 0, 1, 999)
        d2 = dt.datetime(1970, 1, 1, 0, 0, 1)
        log = LogEntry()
        log.date = d1
        await log.asave()
        await log.areload()
        assert log.date != d1
        assert log.date == d2

        # Post UTC - microseconds are rounded (down) nearest millisecond
        d1 = dt.datetime(1970, 1, 1, 0, 0, 1, 9999)
        d2 = dt.datetime(1970, 1, 1, 0, 0, 1, 9000)
        log.date = d1
        await log.asave()
        await log.areload()
        assert log.date != d1
        assert log.date == d2

    async def test_regular_usage(self):
        """Tests for regular datetime fields"""

        class LogEntry(Document):
            date = DateTimeField()

        await LogEntry.adrop_collection()

        d1 = dt.datetime(1970, 1, 1, 0, 0, 1)
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
            d = dt.datetime(i, 1, 1, 0, 0, 1)
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
        logs = LogEntry.aobjects.filter(date__gte=dt.datetime(1980, 1, 1))
        assert await logs.count() == 10

        logs = LogEntry.aobjects.filter(date__lte=dt.datetime(1980, 1, 1))
        assert await logs.count() == 10

        logs = LogEntry.aobjects.filter(
            date__lte=dt.datetime(1980, 1, 1), date__gte=dt.datetime(1975, 1, 1)
        )
        assert await logs.count() == 5

    async def test_datetime_validation(self):
        """Ensure that invalid values cannot be assigned to datetime
        fields.
        """

        class LogEntry(Document):
            time = DateTimeField()

        log = LogEntry()
        log.time = dt.datetime.now()
        log.validate()

        log.time = dt.date.today()
        log.validate()

        log.time = dt.datetime.now().isoformat(" ")
        log.validate()

        log.time = "2019-05-16 21:42:57.897847"
        log.validate()

        if dateutil:
            log.time = dt.datetime.now().isoformat("T")
            log.validate()

        log.time = -1
        with pytest.raises(ValidationError):
            log.validate()
        log.time = "ABC"
        with pytest.raises(ValidationError):
            log.validate()
        log.time = "2019-05-16 21:GARBAGE:12"
        with pytest.raises(ValidationError):
            log.validate()
        log.time = "2019-05-16 21:42:57.GARBAGE"
        with pytest.raises(ValidationError):
            log.validate()
        log.time = "2019-05-16 21:42:57.123.456"
        with pytest.raises(ValidationError):
            log.validate()

    async def test_parse_datetime_as_str(self):
        class DTDoc(Document):
            date = DateTimeField()

        date_str = "2019-03-02 22:26:01"

        # make sure that passing a parsable datetime works
        dtd = DTDoc()
        dtd.date = date_str
        assert isinstance(dtd.date, str)
        await dtd.asave()
        await dtd.areload()

        assert isinstance(dtd.date, dt.datetime)
        assert str(dtd.date) == date_str

        dtd.date = "January 1st, 9999999999"
        with pytest.raises(ValidationError):
            dtd.validate()


class TestDateTimeTzAware(MongoDBAsyncTestCase):
    async def test_datetime_tz_aware_mark_as_changed(self):
        # Reset the connections
        connection._connection_settings = {}
        connection._connections = {}
        connection._dbs = {}

        await async_connect(db=MONGO_TEST_DB, tz_aware=True)

        class LogEntry(Document):
            time = DateTimeField()

        await LogEntry.adrop_collection()

        await LogEntry(time=dt.datetime(2013, 1, 1, 0, 0, 0)).asave()

        log = await LogEntry.aobjects.first()
        log.time = dt.datetime(2013, 1, 1, 0, 0, 0)
        assert ["time"] == log._changed_fields
