import datetime

from mongoengine.base import ComplexBaseField

try:
    # Python 3.9+
    from zoneinfo import ZoneInfo, available_timezones
except ImportError:
    # Fallback to pytz for older Python
    try:
        import pytz

        ZoneInfo = pytz.timezone
        available_timezones = lambda: pytz.all_timezones_set
    except ImportError:
        ZoneInfo = None
        available_timezones = lambda: set()

try:
    # Python 3.11+
    from datetime import UTC
except ImportError:
    # Python ≤ 3.10
    from datetime import timezone

    UTC = timezone.utc


class AwareDateTimeField(ComplexBaseField):
    """DateTime field that preserves timezone information.

    Stores datetime as a dictionary with:
    - 'utc': UTC datetime (for database queries/comparisons)
    - 'tz': Timezone name (e.g., 'Asia/Kolkata', 'America/New_York')

    This allows:
    1. Accurate UTC-based comparisons in queries
    2. Preserving the original timezone for the frontend display
    3. Handling DST changes correctly

    Example:
        class Event (Document):
            start_time = AwareDateTimeField()
            meta = {
                'indexes': [
                    'start_time', # Automatically indexed as 'start_time.utc'
                ]
            }

        # Set with timezone-aware datetime
        from datetime import datetime
        from zoneinfo import ZoneInfo
        event = Event()
        event.start_time = datetime(2024, 6, 15, 14, 30, tzinfo=ZoneInfo('Asia/Kolkata'))
        event.save()

        # Retrieve preserves timezone
        event = Event.objects.first()
        print(event.start_time) # 2024-06-15 14:30:00+05:30
        print(event.start_time.tzinfo) # Asia/Kolkata

        # Query by UTC time
        Event.objects(start_time__utc__gte=some_datetime)

        # Query by timezone
        Event.objects(start_time__tz='Asia/Kolkata')

    Indexing:
        Simply add the field name to your indexes - it will automatically index
        the UTC subfield for efficient time-based queries:

            meta = {'indexes': ['start_time']} # Creates index on 'start_time.utc'

        For descending (newest first) or other options:
            meta = {'indexes': ['-start_time']} # Descending on 'start_time.utc'
            meta = {'indexes': [
                [('start_time', 1), ('other_field', 1)] # Compound index
            ]}

        To also index the timezone name:
            meta = {'indexes': ['start_time.tz']} # Explicit nested field
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize AwareDateTimeField.

        When indexing this field, you can use the field name directly in meta.indexes:
            meta = {'indexes': ['start_time']}

        This will automatically create an index on 'start_time.utc' for efficient
        time-based queries.
        """
        if ZoneInfo is None:
            raise ImportError(
                "AwareDateTimeField requires zoneinfo (Python 3.9+) or pytz. "
                "Install pytz: pip install pytz"
            )
        super().__init__(*args, **kwargs)

    def _set_owner_document(self, owner_document):
        """Called when a field is attached to a document class.

        Expands any index specifications that reference this field to use the
        nested UTC subfield instead.
        """
        super()._set_owner_document(owner_document)

        if owner_document is None:
            return

        # Get the document's meta-indexes
        meta_indexes = owner_document._meta.get("indexes", [])
        if not meta_indexes:
            return

        # Process indexes to expand this field name to nested field
        expanded_indexes = []
        field_name = self.name or self.db_field

        for idx_spec in meta_indexes:
            expanded = self._expand_index_spec(idx_spec, field_name)
            if expanded != idx_spec:
                # This index referenced our field, use expanded version
                expanded_indexes.append(expanded)
            else:
                # Keep original
                expanded_indexes.append(idx_spec)

        # Update the meta-indexes
        owner_document._meta["indexes"] = expanded_indexes

    def _expand_index_spec(self, spec, field_name):
        """Expand the index spec if it references this field.

        Converts:
            'start_time' -> 'start_time.utc'
            [('start_time', 1)] -> [('start_time.utc', 1)]
            {'fields': ['start_time']} -> {'fields': ['start_time.utc']}
        """
        if isinstance(spec, str):
            # Simple string index: 'start_time' or '-start_time'
            prefix = ""
            clean_spec = spec
            if spec and spec[0] in "+-*$#()":
                prefix = spec[0]
                clean_spec = spec[1:]

            if clean_spec == field_name:
                return f"{prefix}{field_name}.utc"
            return spec

        elif isinstance(spec, (list, tuple)):
            # List/tuple of fields: [('start_time', 1), ('other_field', -1)]
            expanded_fields = []
            for field_spec in spec:
                if isinstance(field_spec, tuple) and len(field_spec) == 2:
                    fname, direction = field_spec
                    if fname == field_name:
                        expanded_fields.append((f"{field_name}.utc", direction))
                    else:
                        expanded_fields.append(field_spec)
                elif isinstance(field_spec, str):
                    expanded_fields.append(
                        self._expand_index_spec(field_spec, field_name)
                    )
                else:
                    expanded_fields.append(field_spec)
            return type(spec)(expanded_fields)

        elif isinstance(spec, dict):
            # Dict spec: {'fields': ['start_time'], 'expireAfterSeconds': 3600}
            if "fields" in spec:
                expanded_spec = spec.copy()
                expanded_fields = []
                for field_spec in spec["fields"]:
                    expanded_fields.append(
                        self._expand_index_spec(field_spec, field_name)
                    )
                expanded_spec["fields"] = expanded_fields
                return expanded_spec

        return spec

    def to_python(self, value):
        """Convert MongoDB storage format to Python datetime."""
        if value is None:
            return None

        if isinstance(value, datetime.datetime):
            # Already a datetime object
            return value

        if isinstance(value, dict) and "utc" in value and "tz" in value:
            # Stored format: {"utc": datetime, "tz": "Asia/Kolkata"}
            utc_dt = value["utc"]
            tz_name = value["tz"]

            if not isinstance(utc_dt, datetime.datetime):
                return None

            # Ensure UTC datetime is timezone-aware
            if utc_dt.tzinfo is None:
                utc_dt = utc_dt.replace(tzinfo=UTC)

            # Convert from UTC to original timezone
            try:
                tz = ZoneInfo(tz_name)
                return utc_dt.astimezone(tz)
            except Exception:
                # If timezone is invalid, return UTC
                return utc_dt

        return None

    def to_mongo(self, value):
        """Convert Python datetime to MongoDB storage format."""
        if value is None:
            return None

        # Callable default handling
        if callable(value):
            value = value()

        if not isinstance(value, datetime.datetime):
            self.error(
                f"AwareDateTimeField only accepts datetime objects, got {type(value)}"
            )

        # Ensure timezone-aware
        if value.tzinfo is None:
            self.error(
                "AwareDateTimeField requires timezone-aware datetime. "
                "Use datetime.now(ZoneInfo('Asia/Kolkata')) or similar."
            )

        # Get timezone name
        tz_name = None
        if hasattr(value.tzinfo, "key"):
            # pytz timezone
            tz_name = value.tzinfo.key
        elif hasattr(value.tzinfo, "tzname"):
            # Could be ZoneInfo or other
            tz_name_str = value.tzinfo.tzname(value)
            # For ZoneInfo, try to get the actual zone name
            if hasattr(value.tzinfo, "__str__"):
                # ZoneInfo's __str__ returns the zone name
                zone_str = str(value.tzinfo)
                # ZoneInfo zones are in available_timezones
                if zone_str in available_timezones():
                    tz_name = zone_str
                else:
                    tz_name = tz_name_str
            else:
                tz_name = tz_name_str

        if not tz_name:
            self.error(
                "Could not determine timezone name. "
                "Use ZoneInfo('Asia/Kolkata') or pytz.timezone('Asia/Kolkata')"
            )

        # Convert to UTC for storage
        utc_dt = value.astimezone(UTC)

        return {
            "utc": utc_dt,
            "tz": tz_name,
        }

    def prepare_query_value(self, op, value):
        """Prepare query value - use UTC for comparisons.

        Note: Queries will compare against the 'utc' subfield.
        Use queries like: Event.objects(start_time__gte=some_datetime)
        This will be translated to: {"start_time.utc": {"$gte": utc_datetime}}
        """
        if value is None:
            return None

        # For query operations, we want to compare against the UTC value
        mongo_value = self.to_mongo(value)
        if isinstance(mongo_value, dict) and "utc" in mongo_value:
            # Return just the UTC datetime for comparison
            return mongo_value["utc"]

        return mongo_value

    def lookup_member(self, member_name):
        """Allow querying nested fields like start_time__utc."""
        if member_name == "utc":
            from mongoengine.fields.datetime import DateTimeField

            # Return field type for nested UTC datetime
            field = DateTimeField()
            field.db_field = "utc"
            field.name = member_name
            return field
        elif member_name == "tz":
            from mongoengine.fields.string import StringField

            # Return field type for nested timezone string
            field = StringField()
            field.db_field = "tz"
            field.name = member_name
            return field
        return None

    def validate(self, value, clean=True):
        """Validate the datetime value."""
        if value is None:
            return

        if not isinstance(value, datetime.datetime):
            self.error(
                f"AwareDateTimeField only accepts datetime objects, got {type(value)}"
            )

        if value.tzinfo is None:
            self.error(
                "AwareDateTimeField requires timezone-aware datetime. "
                f"Got naive datetime: {value}"
            )


__all__ = ("AwareDateTimeField",)
