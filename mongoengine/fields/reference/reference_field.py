import inspect

from bson import DBRef, ObjectId

from mongoengine.base import BaseField, LazyReference, _DocumentRegistry
from mongoengine.document import Document, EmbeddedDocument
from mongoengine.errors import DoesNotExist
from mongoengine.base.queryset import DO_NOTHING

from .helpers import _unsaved_object_error


class ReferenceField(BaseField):
    """A reference to a document that will be automatically dereferenced on access (lazily)."""

    def __init__(
        self, document_type, dbref=False, reverse_delete_rule=DO_NOTHING, **kwargs
    ):
        if not (
            isinstance(document_type, str)
            or (inspect.isclass(document_type) and issubclass(document_type, Document))
        ):
            self.error(
                "Argument to ReferenceField constructor must be a "
                "document class or a string"
            )

        self.dbref = dbref
        self.document_type_obj = document_type
        self.reverse_delete_rule = reverse_delete_rule
        super().__init__(**kwargs)

    @property
    def document_type(self):
        if isinstance(self.document_type_obj, str):
            if self.document_type_obj == "self":
                self.document_type_obj = self.owner_document
            else:
                self.document_type_obj = _DocumentRegistry.get(self.document_type_obj)
        return self.document_type_obj

    def __get__(self, instance, owner):
        if instance is None:
            return self

        value = instance._data.get(self.name)
        if isinstance(value, dict) and value.get("_missing_reference", False):
            dbref = DBRef(
                collection=self.owner_document._get_collection_name(),
                id=value.get("_ref"),
            )
            raise DoesNotExist(f"Trying to dereference unknown document {dbref}")

        if isinstance(value, DBRef):
            return LazyReference(
                document_type=self.document_type, pk=value.id, passthrough=True
            )
        return super().__get__(instance, owner)

    def to_mongo(self, document):
        if isinstance(document, DBRef):
            if not self.dbref:
                return document.id
            return document

        if isinstance(document, Document):
            id_ = document.pk
            if id_ is None:
                self.error(_unsaved_object_error(document.__class__.__name__))
            cls = document
        else:
            id_ = document
            cls = self.document_type

        id_field_name = cls._meta["id_field"]
        id_field = cls._fields[id_field_name]

        id_ = id_field.to_mongo(id_)
        if self.document_type._meta.get("abstract"):
            collection = cls._get_collection_name()
            return DBRef(collection, id_, cls=cls._class_name)
        elif self.dbref:
            collection = cls._get_collection_name()
            return DBRef(collection, id_)

        return id_

    def to_python(self, value):
        if isinstance(value, dict) and value.get("_missing_reference"):
            pass
        elif isinstance(value, dict) and ("_id" in value or "_cls" in value):
            if "_ref" in value:
                document_type = _DocumentRegistry.get(value["_ref"].cls)
                del value["_ref"]
                value = document_type._from_son(value)
            else:
                value = self.document_type._from_son(value)
        elif not self.dbref and not isinstance(
            value, (DBRef, Document, EmbeddedDocument)
        ):
            value = LazyReference(document_type=self.document_type, pk=value)
        return value

    def prepare_query_value(self, op, value):
        if value is None:
            return None
        super().prepare_query_value(op, value)
        return self.to_mongo(value)

    def validate(self, value, clean=True):
        if not isinstance(value, (self.document_type, DBRef, ObjectId)):
            self.error("A ReferenceField only accepts DBRef, ObjectId or documents")

        if isinstance(value, Document) and value.id is None:
            self.error(_unsaved_object_error(value.__class__.__name__))

    def lookup_member(self, member_name):
        return self.document_type._fields.get(member_name)


__all__ = ("ReferenceField",)
