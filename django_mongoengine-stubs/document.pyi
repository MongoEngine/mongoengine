from mongoengine import document as md
from .queryset import QuerySetManager

class DjangoFlavor(object):
    objects = QuerySetManager()

# ignoring mixin narrowing in override that mypy doesn't like
class Document(DjangoFlavor, md.Document): ... # type: ignore
class EmbeddedDocument(DjangoFlavor, md.EmbeddedDocument): ... # type: ignore
class DynamicDocument(DjangoFlavor, md.DynamicDocument): ... # type: ignore
