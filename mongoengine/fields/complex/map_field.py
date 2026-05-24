from mongoengine.base import LazyReference, _DocumentRegistry

from .dict_field import DictField


class MapField(DictField):
    """A field that maps a name to a specified field type. Similar to
    a DictField, except the 'value' of each item must match the specified
    field type.
    """

    def __init__(self, field=None, *args, **kwargs):
        from mongoengine.base import BaseField

        # XXX ValidationError raised outside the "validate" method.
        if not isinstance(field, BaseField):
            self.error("Argument to MapField constructor must be a valid field")
        super().__init__(field=field, *args, **kwargs)

    def __get__(self, instance, owner):
        if instance is None:
            return self

        result = super().__get__(instance, owner)
        # Import here to avoid circular import
        from mongoengine.fields.reference import GenericReferenceField, ReferenceField

        if isinstance(self.field, GenericReferenceField) or isinstance(
            self.field, ReferenceField
        ):
            for k, v in result.items():
                if isinstance(v, dict) and "_cls" in v:
                    cls_ = _DocumentRegistry.get(v["_cls"])
                    result[k] = LazyReference(document_type=cls_, pk=v["_ref"].id)
            instance._data[self.name] = result
        return result


__all__ = ("MapField",)
