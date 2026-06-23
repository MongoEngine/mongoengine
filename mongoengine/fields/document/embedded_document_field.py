from mongoengine.base import BaseField, _DocumentRegistry
from mongoengine.document import EmbeddedDocument
from mongoengine.errors import InvalidQueryError

RECURSIVE_REFERENCE_CONSTANT = "self"


class EmbeddedDocumentField(BaseField):
    """An embedded document field - with a declared document_type.
    Only valid values are subclasses of :class:`~mongoengine.EmbeddedDocument`.
    """

    def __init__(self, document_type, **kwargs):
        if not (
            isinstance(document_type, str)
            or issubclass(document_type, EmbeddedDocument)
        ):
            self.error(
                "Invalid embedded document class provided to an EmbeddedDocumentField"
            )

        self.document_type_obj = document_type
        super().__init__(**kwargs)

    @property
    def document_type(self):
        if isinstance(self.document_type_obj, str):
            if self.document_type_obj == RECURSIVE_REFERENCE_CONSTANT:
                resolved_document_type = self.owner_document
            else:
                resolved_document_type = _DocumentRegistry.get(self.document_type_obj)

            if not issubclass(resolved_document_type, EmbeddedDocument):
                # Due to the late resolution of the document_type
                # There is a chance that it won't be an EmbeddedDocument (#1661)
                self.error(
                    "Invalid embedded document class provided to an "
                    "EmbeddedDocumentField"
                )
            self.document_type_obj = resolved_document_type

        return self.document_type_obj

    def to_python(self, value):
        if not isinstance(value, self.document_type):
            return self.document_type._from_son(value)
        return value

    def to_mongo(self, value, use_db_field=True, fields=None):
        if not isinstance(value, self.document_type):
            return value
        return self.document_type.to_mongo(value, use_db_field, fields)

    def validate(self, value, clean=True):
        """Make sure that the document instance is an instance of the
        EmbeddedDocument subclass provided when the document was defined.
        """
        # Using isinstance also works for subclasses of self.document
        if not isinstance(value, self.document_type):
            self.error(
                "Invalid embedded document instance provided to an "
                "EmbeddedDocumentField"
            )
        value.validate(clean=clean)

    def lookup_member(self, member_name):
        doc_and_subclasses = [self.document_type] + self.document_type.__subclasses__()
        for doc_type in doc_and_subclasses:
            field = doc_type._fields.get(member_name)
            if field:
                return field
        return None

    def prepare_query_value(self, op, value):
        if value is not None and not isinstance(value, self.document_type):
            # Short circuit for special operators, returning them as is
            if isinstance(value, dict) and all(k.startswith("$") for k in value.keys()):
                return value
            try:
                value = self.document_type._from_son(value)
            except ValueError:
                raise InvalidQueryError(
                    "Querying the embedded document '%s' failed, due to an invalid query value"
                    % (self.document_type._class_name,)
                )
        super().prepare_query_value(op, value)
        return self.to_mongo(value)


__all__ = ("EmbeddedDocumentField",)
