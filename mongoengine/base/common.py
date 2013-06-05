from mongoengine.errors import NotRegistered

__all__ = ('ALLOW_INHERITANCE', 'get_document', '_document_registry', '_trait_registry')

ALLOW_INHERITANCE = False

_document_registry = {}
_trait_registry = {}

def get_document(name):
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

def apply_traits(doc, names):
    """Apply traits to a document"""
    traits = set()
    for name in sorted(names):
        if name not in _trait_registry:
            raise NotRegistered("""
            `%s` is not a known Trait in the trait registry.
            Importing the Trait class automatically registers it, has it 
            been imported?
            """.strip() % name)
        traits.add(_trait_registry[name])
    bases =  tuple([base for base in doc.__bases__ 
                    if base.__name__ not in _trait_registry])
    dic = {k: v for k, v in doc.__dict__.iteritems() if not k.startswith("__")}
    dic['_traits'] = tuple(traits)
    dic['_traitnames'] = set(names)
    return type(doc.__name__, tuple(traits) + bases, dic)

