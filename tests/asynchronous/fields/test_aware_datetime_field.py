import datetime

try:
    from zoneinfo import ZoneInfo
except ImportError:
    import pytz

    ZoneInfo = pytz.timezone

try:
    from datetime import UTC
except ImportError:
    from datetime import timezone

    UTC = timezone.utc

import pytest

from mongoengine import *
from tests.asynchronous.utils import MongoDBAsyncTestCase, async_get_as_pymongo


class TestAwareDateTimeField(MongoDBAsyncTestCase):
    async def test_basic_storage_and_retrieval(self):
        """Test that timezone-aware datetimes are stored and retrieved correctly."""

        class Event(Document):
            start_time = AwareDateTimeField(required=True)

        await Event.adrop_collection()

        # Create event with Asia/Kolkata timezone
        kolkata_time = datetime.datetime.now().astimezone(ZoneInfo("Asia/Kolkata"))
        event = Event(start_time=kolkata_time)
        await event.asave()

        # Verify storage format in MongoDB
        raw = await async_get_as_pymongo(event)
        assert "start_time" in raw
        assert "utc" in raw["start_time"]
        assert "iso" in raw["start_time"]
        assert "tz" in raw["start_time"]
        assert raw["start_time"]["tz"] == "Asia/Kolkata"

        # Retrieve and verify timezone is preserved
        retrieved = await Event.aobjects.first()
        assert retrieved.start_time.tzinfo is not None
        assert str(retrieved.start_time.tzinfo) == "Asia/Kolkata"
        assert retrieved.start_time == kolkata_time

    async def test_timezone_preservation(self):
        """Test that different timezones are preserved correctly."""

        class Event(Document):
            name = StringField()
            start_time = AwareDateTimeField(required=True)

        await Event.adrop_collection()

        # Create events in different timezones
        now = datetime.datetime.now(UTC)
        timezones = [
            (tz, now.astimezone(ZoneInfo(tz)))
            for tz in ["Asia/Kolkata", "America/New_York", "Europe/London", "UTC"]
        ]
        for tz_name, dt in timezones:
            await Event(name=tz_name, start_time=dt).asave()

        # Verify all timezones are preserved
        for tz_name, expected_dt in timezones:
            event = await Event.aobjects.get(name=tz_name)
            assert str(event.start_time.tzinfo) == tz_name
            assert event.start_time == expected_dt

    async def test_dst_handling(self):
        """Test that DST (Daylight Saving Time) is handled correctly."""

        class Event(Document):
            name = StringField()
            start_time = AwareDateTimeField(required=True)

        await Event.adrop_collection()

        # Winter time (EST = UTC-5)
        winter = Event(
            name="Winter",
            start_time=datetime.datetime(
                2024, 1, 15, 10, 0, 30, 500000, tzinfo=ZoneInfo("America/New_York")
            ),
        )
        await winter.asave()

        # Summer time (EDT = UTC-4)
        summer = Event(
            name="Summer",
            start_time=datetime.datetime(
                2024, 7, 15, 10, 0, 30, 500000, tzinfo=ZoneInfo("America/New_York")
            ),
        )
        await summer.asave()

        # Retrieve and verify both have same timezone name but different offsets
        winter_event = await Event.aobjects.get(name="Winter")
        summer_event = await Event.aobjects.get(name="Summer")

        assert str(winter_event.start_time.tzinfo) == "America/New_York"
        assert str(summer_event.start_time.tzinfo) == "America/New_York"

        # Verify offsets are different (DST)
        winter_offset = winter_event.start_time.utcoffset().total_seconds()
        summer_offset = summer_event.start_time.utcoffset().total_seconds()
        assert winter_offset == -5 * 3600  # EST is UTC-5
        assert summer_offset == -4 * 3600  # EDT is UTC-4

    async def test_query_by_utc(self):
        """Test querying by UTC time using the utc subfield."""

        class Event(Document):
            name = StringField()
            start_time = AwareDateTimeField(required=True)

        await Event.adrop_collection()

        # Create events at different times
        # Early: 8:00 Asia/Kolkata (UTC+5:30) = 2:30 UTC
        await Event(
            name="Early",
            start_time=datetime.datetime(
                2024, 6, 15, 8, 0, 30, 500000, tzinfo=ZoneInfo("Asia/Kolkata")
            ),
        ).asave()
        # Late: 18:00 Asia/Kolkata (UTC+5:30) = 12:30 UTC
        await Event(
            name="Late",
            start_time=datetime.datetime(
                2024, 6, 15, 18, 0, 30, 500000, tzinfo=ZoneInfo("Asia/Kolkata")
            ),
        ).asave()

        # Query by UTC time - should find only the Late event
        utc_noon = datetime.datetime(2024, 6, 15, 12, 0, 30, 500000, tzinfo=UTC)
        events_after_noon = Event.aobjects(start_time__utc__gte=utc_noon)

        assert await events_after_noon.count() == 1
        first_event = await events_after_noon.first()
        assert first_event.name == "Late"

    async def test_query_by_timezone(self):
        """Test querying by timezone name using the tz subfield."""

        class Event(Document):
            start_time = AwareDateTimeField(required=True)

        await Event.adrop_collection()

        # Create events in different timezones
        await Event(
            start_time=datetime.datetime(
                2024, 6, 15, 14, 30, 30, 500000, tzinfo=ZoneInfo("Asia/Kolkata")
            )
        ).asave()
        await Event(
            start_time=datetime.datetime(
                2024, 6, 15, 9, 0, 30, 500000, tzinfo=ZoneInfo("America/New_York")
            )
        ).asave()

        # Query by timezone
        kolkata_events = Event.aobjects(start_time__tz="Asia/Kolkata")
        assert await kolkata_events.count() == 1
        first_event = await kolkata_events.first()
        assert str(first_event.start_time.tzinfo) == "Asia/Kolkata"

    async def test_ordering(self):
        """Test ordering by UTC time."""

        class Event(Document):
            name = StringField()
            start_time = AwareDateTimeField(required=True)

        await Event.adrop_collection()

        # Create events at different UTC times (but in different timezones)
        await Event(
            name="First",
            start_time=datetime.datetime(
                2024, 6, 15, 10, 0, 30, 500000, tzinfo=ZoneInfo("Asia/Kolkata")
            ),  # 04:30 UTC
        ).asave()
        await Event(
            name="Second",
            start_time=datetime.datetime(
                2024, 6, 15, 9, 0, 30, 500000, tzinfo=ZoneInfo("America/New_York")
            ),  # 13:00 UTC
        ).asave()

        # Order by start_time (should use UTC for comparison)
        events = await Event.aobjects.order_by("start_time").to_list()
        assert events[0].name == "First"
        assert events[1].name == "Second"

        # Reverse order
        events = await Event.aobjects.order_by("-start_time").to_list()
        assert events[0].name == "Second"
        assert events[1].name == "First"

    async def test_indexing(self):
        """Test that indexes are created correctly on the UTC subfield."""

        class Event(Document):
            start_time = AwareDateTimeField(required=True)
            meta = {"indexes": ["start_time"]}

        await Event.adrop_collection()
        await Event.aensure_indexes()

        # Get index information from MongoDB
        from mongoengine import async_get_db

        db = await async_get_db()
        indexes = await db[Event._get_collection_name()].index_information()

        # Verify that start_time.utc index was created
        index_names = list(indexes.keys())
        assert any("start_time.utc" in name for name in index_names)

    async def test_validation_requires_timezone(self):
        """Test that naive datetimes are rejected."""

        class Event(Document):
            start_time = AwareDateTimeField(required=True)

        await Event.adrop_collection()

        # Naive datetime should raise validation error
        naive_dt = datetime.datetime(2024, 6, 15, 14, 30, 30, 500000)
        event = Event(start_time=naive_dt)

        with pytest.raises(ValidationError):
            await event.asave()

    async def test_validation_requires_datetime(self):
        """Test that non-datetime values are rejected."""

        class Event(Document):
            start_time = AwareDateTimeField(required=True)

        event = Event(start_time="not a datetime")

        with pytest.raises(ValidationError):
            await event.asave()

    async def test_none_value(self):
        """Test that None values are handled correctly."""

        class Event(Document):
            start_time = AwareDateTimeField()

        await Event.adrop_collection()

        event = Event()
        assert event.start_time is None
        await event.asave()

        retrieved = await Event.aobjects.first()
        assert retrieved.start_time is None

    async def test_default_value(self):
        """Test default values work correctly."""

        def get_default_time():
            return datetime.datetime(2024, 1, 1, 0, 0, 30, 500000, tzinfo=UTC)

        class Event(Document):
            start_time = AwareDateTimeField(default=get_default_time)

        await Event.adrop_collection()

        event = Event()
        assert event.start_time == get_default_time()
        await event.asave()

        retrieved = await Event.aobjects.first()
        assert retrieved.start_time == get_default_time()

    async def test_utc_conversion(self):
        """Test that UTC conversion works correctly."""

        class Event(Document):
            start_time = AwareDateTimeField(required=True)

        await Event.adrop_collection()

        # Create event in Kolkata timezone
        kolkata_time = datetime.datetime(
            2024, 6, 15, 14, 30, 30, 500000, tzinfo=ZoneInfo("Asia/Kolkata")
        )
        event = Event(start_time=kolkata_time)
        await event.asave()

        # Verify UTC conversion
        retrieved = await Event.aobjects.first()
        utc_time = retrieved.start_time.astimezone(UTC)

        # Kolkata is UTC+5:30, so 14:30 Kolkata = 09:00 UTC
        assert utc_time.hour == 9
        assert utc_time.minute == 0

    async def test_compound_index(self):
        """Test compound indexes work correctly."""

        class Event(Document):
            name = StringField()
            start_time = AwareDateTimeField(required=True)
            meta = {"indexes": [[("start_time", 1), ("name", 1)]]}

        await Event.adrop_collection()
        await Event.aensure_indexes()

        from mongoengine import async_get_db

        db = await async_get_db()
        indexes = await db[Event._get_collection_name()].index_information()

        # Verify compound index was created with start_time.utc
        compound_idx = None
        for idx_name, idx_info in indexes.items():
            if len(idx_info["key"]) == 2:
                compound_idx = idx_info
                break

        assert compound_idx is not None
        assert compound_idx["key"][0] == ("start_time.utc", 1)
        assert compound_idx["key"][1] == ("name", 1)

    async def test_descending_index(self):
        """Test descending indexes work correctly."""

        class Event(Document):
            start_time = AwareDateTimeField(required=True)
            meta = {"indexes": ["-start_time"]}

        await Event.adrop_collection()
        await Event.aensure_indexes()

        from mongoengine import async_get_db

        db = await async_get_db()
        indexes = await db[Event._get_collection_name()].index_information()

        # Verify descending index was created
        desc_idx = None
        for idx_name, idx_info in indexes.items():
            if "start_time.utc" in str(idx_info["key"]):
                desc_idx = idx_info
                break

        assert desc_idx is not None
        assert desc_idx["key"][0] == ("start_time.utc", -1)

    async def test_iso_field_stored_in_mongodb(self):
        """Test that the iso field is stored alongside utc and tz."""

        class Event(Document):
            start_time = AwareDateTimeField(required=True)

        await Event.adrop_collection()

        dt = datetime.datetime(
            2024, 6, 15, 14, 30, 30, 500000, tzinfo=ZoneInfo("Asia/Kolkata")
        )
        event = await Event(start_time=dt).asave()

        raw = await async_get_as_pymongo(event)
        assert "iso" in raw["start_time"]
        assert isinstance(raw["start_time"]["iso"], str)
        # ISO string must round-trip back to the original datetime
        assert datetime.datetime.fromisoformat(raw["start_time"]["iso"]) == dt

    async def test_microsecond_precision_preserved_via_iso(self):
        """Test that microseconds survive the MongoDB round-trip via the iso field."""

        class Event(Document):
            start_time = AwareDateTimeField(required=True)

        await Event.adrop_collection()

        dt = datetime.datetime(
            2024, 3, 10, 8, 45, 17, 987654, tzinfo=ZoneInfo("Europe/London")
        )
        await Event(start_time=dt).asave()

        retrieved = await Event.aobjects.first()
        assert retrieved.start_time.second == 17
        assert retrieved.start_time.microsecond == 987654
        assert retrieved.start_time == dt

    async def test_iso_field_queryable(self):
        """Test that start_time__iso can be used in queries."""

        class Event(Document):
            name = StringField()
            start_time = AwareDateTimeField(required=True)

        await Event.adrop_collection()

        dt = datetime.datetime(
            2024, 6, 15, 14, 30, 30, 500000, tzinfo=ZoneInfo("Asia/Kolkata")
        )
        await Event(name="kolkata", start_time=dt).asave()

        iso_str = dt.isoformat()
        result = await Event.aobjects(start_time__iso=iso_str).first()
        assert result is not None
        assert result.name == "kolkata"

    async def test_iso_field_contains_timezone_offset(self):
        """Test that the stored iso string includes the UTC offset."""

        class Event(Document):
            start_time = AwareDateTimeField(required=True)

        await Event.adrop_collection()

        # Kolkata is UTC+5:30
        dt = datetime.datetime(
            2024, 6, 15, 14, 30, 30, 500000, tzinfo=ZoneInfo("Asia/Kolkata")
        )
        event = await Event(start_time=dt).asave()

        raw = await async_get_as_pymongo(event)
        iso_str = raw["start_time"]["iso"]
        # ISO string must encode the +05:30 offset
        assert "+05:30" in iso_str or "05:30" in iso_str

    async def test_half_hour_offset_precision(self):
        """Test that UTC+5:30 (Kolkata) microseconds convert correctly to/from UTC."""

        class Event(Document):
            start_time = AwareDateTimeField(required=True)

        await Event.adrop_collection()

        # 14:30:30.500000 Kolkata = 09:00:30.500000 UTC
        dt = datetime.datetime(
            2024, 6, 15, 14, 30, 30, 500000, tzinfo=ZoneInfo("Asia/Kolkata")
        )
        await Event(start_time=dt).asave()

        retrieved = await Event.aobjects.first()
        utc_dt = retrieved.start_time.astimezone(UTC)

        assert utc_dt.hour == 9
        assert utc_dt.minute == 0
        assert utc_dt.second == 30
        assert utc_dt.microsecond == 500000
