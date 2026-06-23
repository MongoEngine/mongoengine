import datetime
import time

from mongoengine.base import BaseField

try:
    import dateutil
except ImportError:
    dateutil = None
else:
    import dateutil.parser

try:
    # Python 3.11+
    from datetime import UTC
except ImportError:
    # Python ≤ 3.10
    from datetime import timezone

    UTC = timezone.utc


class DateTimeField(BaseField):
    """Datetime field.

    Uses the python-dateutil library if available alternatively use time.strptime
    to parse the dates.  Note: python-dateutil's parser is fully featured and when
    installed you can utilise it to convert varying types of date formats into valid
    python datetime objects.

    Note: To default the field to the current datetime, use: DateTimeField(default=datetime.utcnow)

    Note: Microseconds are rounded to the nearest millisecond.
      Pre UTC microsecond support is effectively broken.
      Use :class:`~mongoengine.fields.ComplexDateTimeField` if you
      need accurate microsecond support.
    """

    def validate(self, value, clean=True):
        new_value = self.to_mongo(value)
        if not isinstance(new_value, (datetime.datetime, datetime.date)):
            self.error('cannot parse date "%s"' % value)

    def to_mongo(self, value):
        if value is None:
            return value

        # Callable default handling (must be first!)
        if callable(value):
            value = value()

        # Already a datetime
        if isinstance(value, datetime.datetime):
            # If naive: assume UTC
            if value.tzinfo is None:
                value = value.replace(tzinfo=UTC)
            else:
                # Normalize to UTC
                value = value.astimezone(UTC)
            return value

        # A date without time
        if isinstance(value, datetime.date):
            value = datetime.datetime(value.year, value.month, value.day, tzinfo=UTC)
            return value

        # Strings
        if isinstance(value, str):
            parsed = self._parse_datetime(value)
            if parsed is None:
                return None
            # Force to UTC
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=UTC)
            else:
                parsed = parsed.astimezone(UTC)
            return parsed

        return None

    @staticmethod
    def _parse_datetime(value):
        # Attempt to parse a datetime from a string
        value = value.strip()
        if not value:
            return None

        if dateutil:
            try:
                return dateutil.parser.parse(value)
            except (TypeError, ValueError, OverflowError):
                return None

        # split usecs, because they are not recognized by strptime.
        if "." in value:
            try:
                value, usecs = value.split(".")
                usecs = int(usecs)
            except ValueError:
                return None
        else:
            usecs = 0
        kwargs = {"microsecond": usecs}
        try:  # Seconds are optional, so try converting seconds first.
            return datetime.datetime(
                *time.strptime(value, "%Y-%m-%d %H:%M:%S")[:6], **kwargs
            )
        except ValueError:
            try:  # Try without seconds.
                return datetime.datetime(
                    *time.strptime(value, "%Y-%m-%d %H:%M")[:5], **kwargs
                )
            except ValueError:  # Try without hour/minutes/seconds.
                try:
                    return datetime.datetime(
                        *time.strptime(value, "%Y-%m-%d")[:3], **kwargs
                    )
                except ValueError:
                    return None

    def prepare_query_value(self, op, value):
        return super().prepare_query_value(op, self.to_mongo(value))


__all__ = ("DateTimeField",)
