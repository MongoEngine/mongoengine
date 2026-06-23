from mongoengine.base import ComplexBaseField
from mongoengine.base.queryset.transform import STRING_OPERATORS

from .helpers import key_not_string, key_starts_with_dollar


class DictField(ComplexBaseField):
    """A dictionary field that wraps a standard Python dictionary. This is
    similar to an embedded document, but the structure is not defined.

    .. note::
        Required means it cannot be empty - as the default for DictFields is {}
    """

    def __init__(self, field=None, *args, **kwargs):
        kwargs.setdefault("default", dict)
        super().__init__(*args, field=field, **kwargs)

    def validate(self, value, clean=True):
        """Make sure that a list of valid fields is being used."""
        from mongoengine.document import Document

        if isinstance(value, (Document,)):
            value = value.to_mongo().to_dict()
        if not isinstance(value, dict):
            self.error("Only dictionaries may be used in a DictField")

        if key_not_string(value):
            msg = "Invalid dictionary key - documents must have only string keys"
            self.error(msg)

        # Following condition applies to MongoDB >= 3.6
        # older Mongo has stricter constraints but
        # it will be rejected upon insertion anyway
        # Having a validation that depends on the MongoDB version
        # is not straightforward as the field isn't aware of the connected Mongo
        if key_starts_with_dollar(value):
            self.error(
                'Invalid dictionary key name - keys may not startswith "$" characters'
            )
        super().validate(value)

    def lookup_member(self, member_name):
        return DictField(db_field=member_name)

    def prepare_query_value(self, op, value):
        from mongoengine.fields.string import StringField

        match_operators = [*STRING_OPERATORS]

        if op in match_operators and isinstance(value, str):
            return StringField().prepare_query_value(op, value)

        if hasattr(
            self.field, "field"
        ):  # Used for instance when using DictField(ListField(IntField()))
            if op in ("set", "unset") and isinstance(value, dict):
                return {
                    k: self.field.prepare_query_value(op, v) for k, v in value.items()
                }
            return self.field.prepare_query_value(op, value)

        return super().prepare_query_value(op, value)


__all__ = ("DictField",)
