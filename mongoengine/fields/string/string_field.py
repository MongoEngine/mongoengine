import re

from mongoengine.base import BaseField
from mongoengine.base.queryset.transform import STRING_OPERATORS


class StringField(BaseField):
    """A unicode string field."""

    def __init__(self, regex=None, max_length=None, min_length=None, **kwargs):
        """
        :param regex: (optional) A string pattern that will be applied during validation
        :param max_length: (optional) A max length that will be applied during validation
        :param min_length: (optional) A min length that will be applied during validation
        :param kwargs: Keyword arguments passed into the parent :class:`~mongoengine.BaseField`
        """
        self.regex = re.compile(regex) if regex else None
        self.max_length = max_length
        self.min_length = min_length
        super().__init__(**kwargs)

    def to_python(self, value):
        if isinstance(value, str):
            return value
        try:
            value = value.decode("utf-8")
        except Exception:
            pass
        return value

    def validate(self, value, clean=True):
        if not isinstance(value, str):
            self.error("StringField only accepts string values")

        if self.max_length is not None and len(value) > self.max_length:
            self.error("String value is too long")

        if self.min_length is not None and len(value) < self.min_length:
            self.error("String value is too short")

        if self.regex is not None and self.regex.match(value) is None:
            self.error("String value did not match validation regex")

    def lookup_member(self, member_name):
        return None

    def prepare_query_value(self, op, value):
        if not isinstance(op, str):
            return value

        if op in STRING_OPERATORS:
            case_insensitive = op.startswith("i")
            op = op.lstrip("i")

            flags = re.IGNORECASE if case_insensitive else 0

            regex = r"%s"
            if op == "startswith":
                regex = r"^%s"
            elif op == "endswith":
                regex = r"%s$"
            elif op == "exact":
                regex = r"^%s$"
            elif op == "wholeword":
                regex = r"\b%s\b"
            elif op == "regex":
                regex = value

            if op == "regex":
                value = re.compile(regex, flags)
            else:
                # escape unsafe characters which could lead to a re.error
                value = re.escape(value)
                value = re.compile(regex % value, flags)
        return super().prepare_query_value(op, value)


__all__ = ("StringField",)
