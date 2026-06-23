from bson import Binary

from mongoengine.base import BaseField


class BinaryField(BaseField):
    """A binary data field."""

    def __init__(self, max_bytes=None, **kwargs):
        self.max_bytes = max_bytes
        super().__init__(**kwargs)

    def __set__(self, instance, value):
        """Handle bytearrays in python 3.1"""
        if isinstance(value, bytearray):
            value = bytes(value)
        return super().__set__(instance, value)

    def to_mongo(self, value):
        return Binary(value)

    def validate(self, value, clean=True):
        if not isinstance(value, (bytes, Binary)):
            self.error(
                "BinaryField only accepts instances of "
                "(%s, %s, Binary)" % (bytes.__name__, Binary.__name__)
            )

        if self.max_bytes is not None and len(value) > self.max_bytes:
            self.error("Binary value is too long")

    def prepare_query_value(self, op, value):
        if value is None:
            return value
        return super().prepare_query_value(op, self.to_mongo(value))


__all__ = ("BinaryField",)
