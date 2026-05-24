import datetime

from .datetime_field import DateTimeField


class DateField(DateTimeField):
    def to_mongo(self, value):
        value = super().to_mongo(value)
        # drop hours, minutes, seconds
        if isinstance(value, datetime.datetime):
            value = datetime.datetime(value.year, value.month, value.day)
        return value

    def to_python(self, value):
        value = super().to_python(value)
        # convert datetime to date
        if isinstance(value, datetime.datetime):
            value = datetime.date(value.year, value.month, value.day)
        return value


__all__ = ("DateField",)
