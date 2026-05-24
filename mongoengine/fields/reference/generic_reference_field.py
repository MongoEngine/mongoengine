from bson import DBRef, ObjectId, SON

from mongoengine.base import BaseField, LazyReference, _DocumentRegistry
from mongoengine.document import Document
from mongoengine.errors import DoesNotExist

from .helpers import _unsaved_object_error


class GenericReferenceField(BaseField):
    """A reference to *any* Document subclass, stored as {"_cls": ..., "_ref": DBRef(...)}."""

    def __init__(self, choices, *args, **kwargs):
        """
        :param choices: The valid choices
        :param *args: (optional) Arguments passed to the BaseField constructor.
        :param **kwargs: (optional) Keyword Arguments passed to the BaseField constructor.
        """
        if choices is None:
            raise ValueError("GenericReferenceField requires a choices argument")
        super().__init__(*args, **kwargs)
        self.choices = []
        for choice in choices:
            if isinstance(choice, str):
                if choice.lower() == "self":
                    self.choices.append("self")
                else:
                    self.choices.append(choice)
            elif isinstance(choice, type) and issubclass(choice, Document):
                self.choices.append(choice)
            else:
                self.error(
                    "Invalid choices provided: must be a list of "
                    "Document subclasses and/or str"
                )

    def _validate_choices(self, value):
        if isinstance(value, dict):
            value = _DocumentRegistry.get(value.get("_cls"))(pk=value["_ref"].id)
        super()._validate_choices(value)

    def __get__(self, instance, owner):
        if instance is None:
            return self
        val = instance._data.get(self.name)
        if isinstance(val, dict) and val.get("_missing_reference", False):
            raise DoesNotExist(f"Trying to dereference unknown document {val}")
        elif isinstance(val, dict) and "_cls" in val:
            return LazyReference(
                document_type=_DocumentRegistry.get(val["_cls"]),
                pk=val["_ref"].id,
                passthrough=True,
            )
        return super().__get__(instance, owner)

    def validate(self, value, clean=True):
        if not isinstance(value, (Document, DBRef, dict, SON)):
            self.error("GenericReferences can only contain documents")

        if isinstance(value, (dict, SON)):
            if "_ref" not in value or "_cls" not in value:
                self.error("GenericReferences can only contain documents")

        elif isinstance(value, Document) and value.id is None:
            self.error(_unsaved_object_error(value.__class__.__name__))

    def to_mongo(self, document):
        if document is None:
            return None

        if isinstance(document, (dict, SON, ObjectId, DBRef)):
            return document

        id_field_name = document.__class__._meta["id_field"]
        id_field = document.__class__._fields[id_field_name]

        if isinstance(document, Document):
            id_ = document.id
            if id_ is None:
                self.error(_unsaved_object_error(document.__class__.__name__))
        else:
            id_ = document

        id_ = id_field.to_mongo(id_)
        collection = document._get_collection_name()
        ref = DBRef(collection, id_)
        return SON((("_cls", document._class_name), ("_ref", ref)))

    def prepare_query_value(self, op, value):
        if value is None:
            return None
        return self.to_mongo(value)

    def to_python(self, value):
        if isinstance(value, Document):
            return value
        elif isinstance(value, dict) and value.get("_missing_reference"):
            return value
        elif isinstance(value, dict) and ("_id" in value and "_cls" in value):
            document_type = _DocumentRegistry.get(value["_cls"])
            del value["_ref"]
            value = document_type._from_son(value)
        return value


__all__ = ("GenericReferenceField",)
