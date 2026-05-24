"""Base datastructures for MongoEngine."""

from .base_dict import BaseDict
from .strict_dict import StrictDict
from .base_list import BaseList
from .embedded_document_list import EmbeddedDocumentList
from .lazy_reference import LazyReference

__all__ = (
    "BaseDict",
    "StrictDict",
    "BaseList",
    "EmbeddedDocumentList",
    "LazyReference",
)
