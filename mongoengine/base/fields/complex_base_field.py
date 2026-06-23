import operator

from bson import DBRef

from mongoengine.base.common import _DocumentRegistry
from mongoengine.base.datastructures import (
    BaseDict,
    BaseList,
    EmbeddedDocumentList,
)
from mongoengine.common import _import_class
from mongoengine.errors import ValidationError, NotRegistered

from .base_field import BaseField


class ComplexBaseField(BaseField):
    """Handles complex fields, such as lists / dictionaries.

    Allows for nesting of embedded documents inside complex types.
    Handles the lazy dereferencing of a queryset by lazily dereferencing all
    items in a list / dict rather than one at a time.
    """

    def __init__(self, field=None, **kwargs):
        if field is not None and not isinstance(field, BaseField):
            raise TypeError(
                f"field argument must be a Field instance (e.g {self.__class__.__name__}(StringField()))"
            )
        self.field = field
        super().__init__(**kwargs)

    def __set__(self, instance, value):
        # Some fields e.g EnumField are converted upon __set__
        # So it is fair to mimic the same behavior when using e.g ListField(EnumField)
        EnumField = _import_class("EnumField")
        if self.field and isinstance(self.field, EnumField):
            if isinstance(value, (list, tuple)):
                value = [self.field.to_python(sub_val) for sub_val in value]
            elif isinstance(value, dict):
                value = {key: self.field.to_python(sub) for key, sub in value.items()}

        return super().__set__(instance, value)

    def __get__(self, instance, owner):
        if instance is None:
            return self

        EmbeddedDocumentField = _import_class("EmbeddedDocumentField")

        result = super().__get__(instance, owner)

        # Wrap into BaseList / BaseDict
        if isinstance(result, (list, tuple)):
            if isinstance(self.field, EmbeddedDocumentField):
                result = EmbeddedDocumentList(result, instance, self.name)
                instance._data[self.name] = result
            elif not isinstance(result, BaseList):
                result = BaseList(result, instance, self.name)
                instance._data[self.name] = result
        elif isinstance(result, dict):
            if "_cls" in result:
                cls_ = _DocumentRegistry.get(result["_cls"].split(".")[-1])
                result = cls_._from_son(result)
                instance._data[self.name] = result
            elif not isinstance(result, BaseDict):
                result = BaseDict(result, instance, self.name)
                instance._data[self.name] = result

        return result

    def to_python(self, value):
        """Convert a MongoDB-compatible type to a Python type."""
        if isinstance(value, str):
            return value

        if hasattr(value, "to_python"):
            return value.to_python()

        BaseDocument = _import_class("BaseDocument")
        if isinstance(value, BaseDocument):
            # Something is wrong, return the value as it is
            return value

        is_list = False
        if not hasattr(value, "items"):
            try:
                is_list = True
                value = {idx: v for idx, v in enumerate(value)}
            except TypeError:  # Not iterable return the value
                return value

        if self.field:
            value_dict = {
                key: self.field.to_python(item) for key, item in value.items()
            }
        else:
            Document = _import_class("Document")
            value_dict = {}
            for k, v in value.items():
                if isinstance(v, Document):
                    # We need the id from the saved object to create the DBRef
                    if v.pk is None:
                        self.error(
                            "You can only reference documents once they"
                            " have been saved to the database"
                        )
                    collection = v._get_collection_name()
                    value_dict[k] = DBRef(collection, v.pk)
                elif hasattr(v, "to_python"):
                    value_dict[k] = v.to_python()
                elif isinstance(v, dict) and v.get("_cls") and not "_ref" in v:
                    try:
                        cls_ = _DocumentRegistry.get(v.get("_cls").split(".")[-1])
                        value_dict[k] = cls_._from_son(v)
                    except NotRegistered:
                        value_dict[k] = self.to_python(v)
                else:
                    value_dict[k] = self.to_python(v)

        if is_list:  # Convert back to a list
            return [
                v for _, v in sorted(value_dict.items(), key=operator.itemgetter(0))
            ]
        return value_dict

    def to_mongo(self, value, use_db_field=True, fields=None):
        """Convert a Python type to a MongoDB-compatible type."""
        Document = _import_class("Document")
        EmbeddedDocument = _import_class("EmbeddedDocument")
        GenericReferenceField = _import_class("GenericReferenceField")

        if isinstance(value, str):
            return value

        if hasattr(value, "to_mongo"):
            if isinstance(value, Document):
                return GenericReferenceField(choices=(type(value),)).to_mongo(value)
            cls = value.__class__
            val = value.to_mongo(use_db_field, fields)
            # If it's a document that is not inherited add _cls
            if isinstance(value, EmbeddedDocument):
                val["_cls"] = cls.__name__
            return val

        is_list = False
        if not hasattr(value, "items"):
            try:
                is_list = True
                value = {k: v for k, v in enumerate(value)}
            except TypeError:  # Not iterable return the value
                return value

        if self.field:
            value_dict = {
                key: self.field._to_mongo_safe_call(item, use_db_field, fields)
                for key, item in value.items()
            }
        else:
            value_dict = {}
            for k, v in value.items():
                if isinstance(v, Document):
                    # We need the id from the saved object to create the DBRef
                    if v.pk is None:
                        self.error(
                            "You can only reference documents once they"
                            " have been saved to the database"
                        )

                    # If it's a document that is not inheritable it won't have
                    # any _cls data so make it a generic reference allows
                    # us to dereference
                    meta = getattr(v, "_meta", {})
                    allow_inheritance = meta.get("allow_inheritance")
                    if not allow_inheritance:
                        value_dict[k] = GenericReferenceField(
                            choices=(type(v),)
                        ).to_mongo(v)
                    else:
                        collection = v._get_collection_name()
                        value_dict[k] = DBRef(collection, v.pk)
                elif hasattr(v, "to_mongo"):
                    cls = v.__class__
                    val = v.to_mongo(use_db_field, fields)
                    # If it's a document that is not inherited add _cls
                    if isinstance(v, (Document, EmbeddedDocument)):
                        val["_cls"] = cls.__name__
                    value_dict[k] = val
                else:
                    value_dict[k] = self.to_mongo(v, use_db_field, fields)

        if is_list:  # Convert back to a list
            return [
                v for _, v in sorted(value_dict.items(), key=operator.itemgetter(0))
            ]
        return value_dict

    def validate(self, value, clean=True):
        """If field is provided ensure the value is valid."""
        errors = {}
        if self.field:
            if hasattr(value, "items"):
                sequence = value.items()
            else:
                sequence = enumerate(value)
            for k, v in sequence:
                try:
                    self.field._validate(v)
                except ValidationError as error:
                    errors[k] = error.errors or error
                except (ValueError, AssertionError) as error:
                    errors[k] = error

            if errors:
                field_class = self.field.__class__.__name__
                self.error(f"Invalid {field_class} item ({value})", errors=errors)
        # Don't allow empty values if required
        if self.required and not value:
            self.error("Field is required and cannot be empty")

    def prepare_query_value(self, op, value):
        return self.to_mongo(value)

    def lookup_member(self, member_name):
        if self.field:
            return self.field.lookup_member(member_name)
        return None

    def _set_owner_document(self, owner_document):
        if self.field:
            self.field.owner_document = owner_document
        self._owner_document = owner_document


__all__ = ("ComplexBaseField",)
