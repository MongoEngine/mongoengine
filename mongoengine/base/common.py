import warnings

from mongoengine.errors import NotRegistered

__all__ = ("UPDATE_OPERATORS", "_DocumentRegistry")


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


class _DocumentRegistry:
    """Wrapper for the document registry (providing a singleton pattern).
    This is part of MongoEngine's internals, not meant to be used directly by end-users
    """

    @staticmethod
    def get(name):
        doc = _document_registry.get(name, None)
        if not doc:
            # Possible old style name
            single_end = name.split(".")[-1]
            compound_end = ".%s" % single_end
            possible_match = [
                k
                for k in _document_registry
                if k.endswith(compound_end) or k == single_end
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

    @staticmethod
    def register(DocCls):
        ExistingDocCls = _document_registry.get(DocCls._class_name)
        if (
            ExistingDocCls is not None
            and ExistingDocCls.__module__ != DocCls.__module__
        ):
            # A sign that a codebase may have named two different classes with the same name accidentally,
            # this could cause issues with dereferencing because MongoEngine makes the assumption that a Document
            # class name is unique.
            warnings.warn(
                f"Multiple Document classes named `{DocCls._class_name}` were registered, "
                f"first from: `{ExistingDocCls.__module__}`, then from: `{DocCls.__module__}`. "
                "this may lead to unexpected behavior during dereferencing.",
                stacklevel=4,
            )
        _document_registry[DocCls._class_name] = DocCls

    @staticmethod
    def unregister(doc_cls_name):
        _document_registry.pop(doc_cls_name)


def _get_documents_by_db(connection_alias, default_connection_alias):
    """Get all registered Documents class attached to a given database"""

    def get_doc_alias(doc_cls):
        return doc_cls._meta.get("db_alias", default_connection_alias)

    return [
        doc_cls
        for doc_cls in _document_registry.values()
        if get_doc_alias(doc_cls) == connection_alias
    ]
