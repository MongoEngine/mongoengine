from mongoengine.base import (
    BaseDocument,
    ComplexBaseField,
    LazyReference,
    _DocumentRegistry,
)
from mongoengine.synchronous.queryset.base import BaseQuerySet


class ListField(ComplexBaseField):
    """A list field that wraps a standard field, allowing multiple instances
    of the field to be used as a list in the database.

    If using with ReferenceFields see: :ref:`many-to-many-with-listfields`

    .. note::
        Required means it cannot be empty - as the default for ListFields is []
    """

    def __init__(self, field=None, *, max_length=None, **kwargs):
        self.max_length = max_length
        kwargs.setdefault("default", list)
        super().__init__(field=field, **kwargs)

    def __get__(self, instance, owner):
        if instance is None:
            # Document class being used rather than a document object
            return self
        value = instance._data.get(self.name)
        if value:
            for index, val in enumerate(value):
                if isinstance(val, dict) and "_cls" in val and "_ref" in val:
                    if "missing_reference" in val:
                        value[index] = LazyReference(
                            document_type=_DocumentRegistry.get(val["_cls"]),
                            pk=val["_ref"].id,
                        )
        return super().__get__(instance, owner)

    def validate(self, value, clean=True):
        """Make sure that a list of valid fields is being used."""
        if not isinstance(value, (list, tuple, BaseQuerySet)):
            self.error("Only lists and tuples may be used in a list field")

        # Validate that max_length is not exceeded.
        # NOTE It's still possible to bypass this enforcement by using $push.
        # However, if the document is reloaded after $push and then re-saved,
        # the validation error will be raised.
        if self.max_length is not None and len(value) > self.max_length:
            self.error("List is too long")

        super().validate(value)

    def prepare_query_value(self, op, value):
        # Validate that the `set` operator doesn't contain more items than `max_length`.
        if op == "set" and self.max_length is not None and len(value) > self.max_length:
            self.error("List is too long")

        if self.field:
            # If the value is iterable and it's not a string nor a
            # BaseDocument, call prepare_query_value for each of its items.
            is_iter = hasattr(value, "__iter__")
            eligible_iter = is_iter and not isinstance(value, (str, BaseDocument))
            if (
                op in ("set", "unset", "gt", "gte", "lt", "lte", "ne", None)
                and eligible_iter
            ):
                return [self.field.prepare_query_value(op, v) for v in value]

            return self.field.prepare_query_value(op, value)

        return super().prepare_query_value(op, value)


class EmbeddedDocumentListField(ListField):
    """A :class:`~mongoengine.ListField` designed specially to hold a list of
    embedded documents to provide additional query helpers.

    .. note::
        The only valid list values are subclasses of
        :class:`~mongoengine.EmbeddedDocument`.
    """

    def __init__(self, document_type, **kwargs):
        """
        :param document_type: The type of
         :class:`~mongoengine.EmbeddedDocument` the list will hold.
        :param kwargs: Keyword arguments passed into the parent :class:`~mongoengine.ListField`
        """
        from mongoengine.fields.document import EmbeddedDocumentField

        super().__init__(field=EmbeddedDocumentField(document_type), **kwargs)


class SortedListField(ListField):
    """A ListField that sorts the contents of its list before writing to
    the database in order to ensure that a sorted list is always
    retrieved.

    .. warning::
        There is a potential race condition when handling lists.  If you set /
        save the whole list then other processes trying to save the whole list
        as well could overwrite changes.  The safest way to append to a list is
        to perform a push operation.
    """

    def __init__(self, field, **kwargs):
        from operator import itemgetter

        self._ordering = kwargs.pop("ordering", None)
        self._order_reverse = kwargs.pop("reverse", False)
        self._itemgetter = itemgetter
        super().__init__(field, **kwargs)

    def to_mongo(self, value, use_db_field=True, fields=None):
        value = super().to_mongo(value, use_db_field, fields)
        if self._ordering is not None:
            return sorted(
                value, key=self._itemgetter(self._ordering), reverse=self._order_reverse
            )
        return sorted(value, reverse=self._order_reverse)


__all__ = ("ListField", "EmbeddedDocumentListField", "SortedListField")
