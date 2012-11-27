from mongoengine.errors import NotRegistered

__all__ = ('ALLOW_INHERITANCE', 'get_document', '_document_registry')

ALLOW_INHERITANCE = False

_document_registry = {}


def get_document(name):
    doc = _document_registry.get(name, None)
    if not doc and '.' in name:
        # Possible old style name
        end = name.split('.')[-1]
        possible_match = [k for k in _document_registry.keys() if k == end]
        if len(possible_match) == 1:
            doc = _document_registry.get(possible_match.pop(), None)
    if not doc:
        raise NotRegistered("""
            `%s` has not been registered in the document registry.
            Importing the document class automatically registers it, has it
            been imported?
        """.strip() % name)
    return doc
