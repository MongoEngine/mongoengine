from mongoengine.errors import NotRegistered

__all__ = ('UPDATE_OPERATORS', 'get_document', 'get_document_by_collection',
           '_document_registry')


UPDATE_OPERATORS = set(['set', 'unset', 'inc', 'dec', 'pop', 'push',
                        'push_all', 'pull', 'pull_all', 'add_to_set',
                        'set_on_insert', 'min', 'max', 'rename'])


_document_registry = {}


def _is_cls_collection(cls, collection):
    """Check if a document is associated with the given collection"""
    doc = _document_registry[cls]
    return doc._meta.get('collection') == collection


def get_document(name):
    """Get a document class by name."""
    doc = _document_registry.get(name, None)
    if not doc:
        # Possible old style name
        single_end = name.split('.')[-1]
        compound_end = '.%s' % single_end
        possible_match = [k for k in _document_registry.keys()
                          if k.endswith(compound_end) or k == single_end]
        if len(possible_match) == 1:
            doc = _document_registry.get(possible_match.pop(), None)
    if not doc:
        raise NotRegistered("""
            `%s` has not been registered in the document registry.
            Importing the document class automatically registers it, has it
            been imported?
        """.strip() % name)
    return doc


def get_document_by_collection(collection):
    """Get a document class by the name of its collection."""
    potential_docs = filter(
        lambda cls: _is_cls_collection(cls, collection),
        _document_registry
    )
    if not len(potential_docs) == 1:
        return None
    return get_document(potential_docs[0])
