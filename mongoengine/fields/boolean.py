from mongoengine.base import BaseField


class BooleanField(BaseField):
    """Boolean field type."""

    def to_python(self, value):
        try:
            value = bool(value)
        except (ValueError, TypeError):
            pass
        return value

    def validate(self, value, clean=True):
        if not isinstance(value, bool):
            self.error("BooleanField only accepts boolean values")


__all__ = ("BooleanField",)
