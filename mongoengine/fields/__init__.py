"""
MongoEngine field types for document schema definition.

This module provides all field types used to define MongoDB document schemas.
Fields handle validation, type conversion, and MongoDB BSON serialization.
"""

# Import all field types
from .string import StringField, URLField, EmailField
from .numeric import IntField, FloatField, DecimalField, Decimal128Field
from .boolean import *
from .datetime import DateTimeField, DateField, ComplexDateTimeField
from .document import (
    EmbeddedDocumentField,
    GenericEmbeddedDocumentField,
    DynamicField,
)
from .complex import (
    ListField,
    EmbeddedDocumentListField,
    SortedListField,
    DictField,
    MapField,
)
from .reference import ReferenceField, GenericReferenceField
from .file import (
    BinaryField,
    GridFSProxy,
    FileField,
    ImageGridFsProxy,
    ImageField,
)
from .geo import (
    GeoPointField,
    PointField,
    LineStringField,
    PolygonField,
    MultiPointField,
    MultiLineStringField,
    MultiPolygonField,
)
from .enum import *
from .uuid import *
from .sequence import *
from .exceptions import *

# Import base classes for re-export
from mongoengine.base import ObjectIdField, GeoJsonBaseField

# Import base field classes (not in __all__ but needed for backwards compatibility)
from mongoengine.base import (
    BaseDocument,
    BaseField,
    ComplexBaseField,
    LazyReference,
    _DocumentRegistry,
)

# Consolidate __all__ from all submodules
__all__ = (
    # string.py
    "StringField",
    "URLField",
    "EmailField",
    # numeric.py
    "IntField",
    "FloatField",
    "DecimalField",
    "Decimal128Field",
    # boolean.py
    "BooleanField",
    # datetime.py
    "DateTimeField",
    "DateField",
    "ComplexDateTimeField",
    # document.py
    "EmbeddedDocumentField",
    "GenericEmbeddedDocumentField",
    "DynamicField",
    # complex.py
    "ListField",
    "SortedListField",
    "EmbeddedDocumentListField",
    "DictField",
    "MapField",
    # reference.py
    "ReferenceField",
    "GenericReferenceField",
    # file.py
    "BinaryField",
    "GridFSError",
    "GridFSProxy",
    "FileField",
    "ImageGridFsProxy",
    "ImproperlyConfigured",
    "ImageField",
    # geo.py
    "GeoPointField",
    "PointField",
    "LineStringField",
    "PolygonField",
    "MultiPointField",
    "MultiLineStringField",
    "MultiPolygonField",
    # enum.py
    "EnumField",
    # uuid.py
    "UUIDField",
    # sequence.py
    "SequenceField",
    # Base classes re-exported for convenience
    "ObjectIdField",
    "GeoJsonBaseField",
)
