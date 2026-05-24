from operator import itemgetter

from mongoengine.base import BaseField, LazyReference, _DocumentRegistry
from mongoengine.document import Document, EmbeddedDocument


class DynamicField(BaseField):
    """A truly dynamic field type capable of handling different and varying
    types of data.

    Used by :class:`~mongoengine.DynamicDocument` to handle dynamic data"""

    def to_mongo(self, value, use_db_field=True, fields=None):
        """Convert a Python type to a MongoDB compatible type."""
        if isinstance(value, str):
            return value

        if hasattr(value, "to_mongo"):
            cls = value.__class__
            val = value.to_mongo(use_db_field, fields)
            # If we its a document thats not inherited add _cls
            if isinstance(value, Document):
                val = {"_ref": value.to_dbref(), "_cls": cls.__name__}
            if isinstance(value, EmbeddedDocument):
                val["_cls"] = cls.__name__
            return val

        if not isinstance(value, (dict, list, tuple)):
            return value

        is_list = False
        if not hasattr(value, "items"):
            is_list = True
            value = {k: v for k, v in enumerate(value)}

        data = {}
        for k, v in value.items():
            data[k] = self.to_mongo(v, use_db_field, fields)

        value = data
        if is_list:  # Convert back to a list
            value = [v for k, v in sorted(data.items(), key=itemgetter(0))]
        return value

    def to_python(self, value):
        if isinstance(value, dict) and "_cls" in value:
            doc_cls = _DocumentRegistry.get(value["_cls"])
            if doc_cls._is_document:
                return LazyReference(
                    document_type=doc_cls, pk=value["_ref"].id, passthrough=True
                )
            else:
                return doc_cls._from_son(value)
        return super().to_python(value)

    def lookup_member(self, member_name):
        return member_name

    def prepare_query_value(self, op, value):
        from mongoengine.fields.string import StringField

        if isinstance(value, str):
            return StringField().prepare_query_value(op, value)
        return super().prepare_query_value(op, self.to_mongo(value))

    def validate(self, value, clean=True):
        if hasattr(value, "validate"):
            value.validate(clean=clean)


__all__ = ("DynamicField",)
