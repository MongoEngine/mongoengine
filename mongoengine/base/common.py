from collections import defaultdict

from mongoengine.errors import NotRegistered

__all__ = (
    "UPDATE_OPERATORS",
    "get_document",
    "update_document_if_registered",
    "_document_registry",
    "_undefined_document_delete_rules",
)


UPDATE_OPERATORS = {
    "set",
    "unset",
    "inc",
    "dec",
    "mul",
    "pop",
    "push",
    "push_all",
    "pull",
    "pull_all",
    "add_to_set",
    "set_on_insert",
    "min",
    "max",
    "rename",
}


_document_registry = {}
_undefined_document_delete_rules = defaultdict(list)


def get_document(name):
    """Get a registered Document class by name."""
    doc = _document_registry.get(name, None)
    if not doc:
        # Possible old style name
        single_end = name.split(".")[-1]
        compound_end = ".%s" % single_end
        possible_match = [
            k for k in _document_registry if k.endswith(compound_end) or k == single_end
        ]
        if len(possible_match) == 1:
            doc = _document_registry.get(possible_match.pop(), None)
    if not doc:
        raise NotRegistered(
            """
            `%s` has not been registered in the document registry.
            Importing the document class automatically registers it, has it
            been imported?
        """.strip()
            % name
        )
    return doc


def _get_documents_by_db(connection_alias, default_connection_alias):
    """Get all registered Documents class attached to a given database"""

    def get_doc_alias(doc_cls):
        return doc_cls._meta.get("db_alias", default_connection_alias)

    return [
        doc_cls
        for doc_cls in _document_registry.values()
        if get_doc_alias(doc_cls) == connection_alias
    ]


def update_document_if_registered(document):
    """Converts a string to a Document if registered in the registry."""
    if isinstance(document, str):
        try:
            return get_document(document)
        except NotRegistered:
            pass
    return document
