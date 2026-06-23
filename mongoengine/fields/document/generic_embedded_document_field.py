from bson import SON

from mongoengine.base import BaseField, _DocumentRegistry
from mongoengine.document import EmbeddedDocument


class GenericEmbeddedDocumentField(BaseField):
    """A generic embedded document field - allows any
    :class:`~mongoengine.EmbeddedDocument` to be stored.

    Only valid values are subclasses of :class:`~mongoengine.EmbeddedDocument`.

    .. note ::
        You can use the choices param to limit the acceptable
        EmbeddedDocument types
    """

    def prepare_query_value(self, op, value):
        return super().prepare_query_value(op, self.to_mongo(value))

    def to_python(self, value):
        if isinstance(value, dict):
            doc_cls = _DocumentRegistry.get(value["_cls"])
            value = doc_cls._from_son(value)

        return value

    def validate(self, value, clean=True):
        if self.choices and isinstance(value, SON):
            for choice in self.choices:
                if value["_cls"] == choice._class_name:
                    return True

        if not isinstance(value, EmbeddedDocument):
            self.error(
                "Invalid embedded document instance provided to an "
                "GenericEmbeddedDocumentField"
            )

        value.validate(clean=clean)

    def lookup_member(self, member_name):
        document_choices = self.choices or []
        for document_choice in document_choices:
            doc_and_subclasses = [document_choice] + document_choice.__subclasses__()
            for doc_type in doc_and_subclasses:
                field = doc_type._fields.get(member_name)
                if field:
                    return field

    def to_mongo(self, document, use_db_field=True, fields=None):
        if document is None:
            return None
        data = document.to_mongo(use_db_field, fields)
        if "_cls" not in data:
            data["_cls"] = document._class_name
        return data


__all__ = ("GenericEmbeddedDocumentField",)
