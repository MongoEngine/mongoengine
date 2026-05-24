import datetime

from mongoengine.base import BaseField

try:
    # Python 3.11+
    from datetime import UTC
except ImportError:
    # Python ≤ 3.10
    from datetime import timezone

    UTC = timezone.utc


class ComplexDateTimeField(BaseField):
    """
    ComplexDateTimeField handles microseconds exactly instead of rounding
    like DateTimeField does.

    Derives from a StringField so you can do `gte` and `lte` filtering by
    using lexicographical comparison when filtering / sorting strings.

    The stored string has the following format:

        YYYY,MM,DD,HH,MM,SS,NNNNNN

    Where NNNNNN is the number of microseconds of the represented `datetime`.
    The `,` as the separator can be easily modified by passing the `separator`
    keyword when initializing the field.

    Note: To default the field to the current datetime, use: DateTimeField(default=datetime.utcnow)
    """

    def __init__(self, separator=",", **kwargs):
        """
        :param separator: Allows to customize the separator used for storage (default ``,``)
        :param kwargs: Keyword arguments passed into the parent :class:`~mongoengine.StringField`
        """
        self.separator = separator
        self.format = separator.join(["%Y", "%m", "%d", "%H", "%M", "%S", "%f"])
        super().__init__(**kwargs)

    def _convert_from_datetime(self, val):
        """
        Convert a `datetime` object to a string representation (which will be
        stored in MongoDB). This is the reverse function of
        `_convert_from_string`.

        >>> a = datetime.datetime(2011, 6, 8, 20, 26, 24, 92284)
        >>> ComplexDateTimeField()._convert_from_datetime(a)
        '2011,06,08,20,26,24,092284'
        """
        if val.tzinfo is None:
            val = val.replace(tzinfo=UTC)
        else:
            val = val.astimezone(UTC)
        return val.strftime(self.format)

    def _convert_from_string(self, data):
        """
        Convert a string representation to a `datetime` object (the object you
        will manipulate). This is the reverse function of
        `_convert_from_datetime`.

        >>> a = '2011,06,08,20,26,24,092284'
        >>> ComplexDateTimeField()._convert_from_string(a)
        datetime.datetime(2011, 6, 8, 20, 26, 24, 92284)
        """
        values = [int(d) for d in data.split(self.separator)]
        return datetime.datetime(*values, tzinfo=UTC)

    def __get__(self, instance, owner):
        if instance is None:
            return self

        data = super().__get__(instance, owner)

        if isinstance(data, datetime.datetime) or data is None:
            return data
        return self._convert_from_string(data)

    def __set__(self, instance, value):
        super().__set__(instance, value)
        value = instance._data[self.name]
        if value is not None:
            if isinstance(value, datetime.datetime):
                instance._data[self.name] = self._convert_from_datetime(value)
            else:
                instance._data[self.name] = value

    def validate(self, value, clean=True):
        value = self.to_python(value)
        if not isinstance(value, datetime.datetime):
            self.error("Only datetime objects may used in a ComplexDateTimeField")

    def to_python(self, value):
        original_value = value
        try:
            return self._convert_from_string(value)
        except Exception:
            return original_value

    def to_mongo(self, value):
        value = self.to_python(value)
        return self._convert_from_datetime(value)

    def prepare_query_value(self, op, value):
        if value is None:
            return value
        return super().prepare_query_value(op, self._convert_from_datetime(value))


__all__ = ("ComplexDateTimeField",)
