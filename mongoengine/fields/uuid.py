import uuid

from bson import Binary, UUID_SUBTYPE

from mongoengine.base import BaseField


class UUIDField(BaseField):
    """A UUID field."""

    _binary = None

    def __init__(self, binary=True, **kwargs):
        """
        Store UUID data in the database

        :param binary: if False store as a string.
        """
        self._binary = binary
        super().__init__(**kwargs)

    def to_python(self, value):
        # 1) BSON Binary subtype=4 → decode safely
        if isinstance(value, Binary) and value.subtype == UUID_SUBTYPE:
            try:
                return value.as_uuid()  # <-- FIX: use as_uuid()
            except Exception:
                return value

        # 2) String → UUID
        if isinstance(value, str):
            try:
                return uuid.UUID(value)
            except Exception:
                return value

        # 3) Already UUID
        if isinstance(value, uuid.UUID):
            return value

        # 4) Leave raw BSON if storing binary
        if self._binary:
            return value

        # 5) Fallback coercion
        try:
            return uuid.UUID(str(value))
        except Exception:
            return value

    def to_mongo(self, value):
        if value is None:
            return None

        # Not storing binary → store as string
        if not self._binary:
            return str(value)

        # String → UUID → Binary
        if isinstance(value, str):
            value = uuid.UUID(value)

        # UUID → Binary
        if isinstance(value, uuid.UUID):
            return Binary.from_uuid(value)  # <-- FIX: required for PyMongo 4

        return value

    def prepare_query_value(self, op, value):
        if value is None:
            return None
        return self.to_mongo(value)

    def validate(self, value, clean=True):
        if value is None:
            return

        try:
            if isinstance(value, uuid.UUID):
                return
            uuid.UUID(str(value))
        except (ValueError, TypeError, AttributeError) as exc:
            self.error("Could not convert to UUID: %s" % exc)


__all__ = ("UUIDField",)
