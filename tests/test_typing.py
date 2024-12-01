# mypy: enable-error-code="var-annotated"
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import UUID

from bson import ObjectId
from typing_extensions import assert_type

from mongoengine import Document, EmbeddedDocument, fields
from mongoengine.base.datastructures import LazyReference


def test_it_uses_correct_types() -> None:

    class ImageEmbedded(EmbeddedDocument):
        pass

    class ImageDocument(Document):
        pass

    class Color(Enum):
        RED = "red"

    class Doc(Document):
        stringfield = fields.StringField()
        urlfield = fields.URLField()
        emailfield = fields.EmailField()
        intfield = fields.IntField()
        floatfield = fields.FloatField()
        decimalfield = fields.DecimalField()
        booleanfield = fields.BooleanField()
        datetimefield = fields.DateTimeField()
        datefield = fields.DateField()
        complexdatetimefield = fields.ComplexDateTimeField()
        embeddeddocumentfield = fields.EmbeddedDocumentField(ImageEmbedded)
        objectidfield = fields.ObjectIdField()
        genericembeddeddocumentfield = fields.GenericEmbeddedDocumentField()
        dynamicfield = fields.DynamicField()
        listfield = fields.ListField(fields.StringField())
        sortedlistfield = fields.SortedListField(fields.StringField())
        embeddeddocumentlistfield = fields.EmbeddedDocumentListField(ImageEmbedded)
        dictfield = fields.DictField(fields.StringField(required=True))
        mapfield = fields.MapField(fields.StringField())
        referencefield = fields.ReferenceField(ImageDocument)
        cachedreferencefield = fields.CachedReferenceField(ImageDocument)
        lazyreferencefield = fields.LazyReferenceField(ImageDocument)
        genericlazyreferencefield = fields.GenericLazyReferenceField()
        genericreferencefield = fields.GenericReferenceField()
        binaryfield = fields.BinaryField()
        filefield = fields.FileField()
        imagefield = fields.ImageField()
        geopointfield = fields.GeoPointField()
        pointfield = fields.PointField()
        linestringfield = fields.LineStringField()
        polygonfield = fields.PolygonField()
        sequencefield = fields.SequenceField()
        uuidfield = fields.UUIDField()
        enumfield = fields.EnumField(Color)
        multipointfield = fields.MultiPointField()
        multilinestringfield = fields.MultiLineStringField()
        multipolygonfield = fields.MultiPolygonField()
        decimal128field = fields.Decimal128Field()

    # Setting sequencefield prevents database access in tests.
    doc = Doc(sequencefield=1)

    assert_type(doc.stringfield, Optional[str])
    assert_type(doc.urlfield, Optional[str])
    assert_type(doc.emailfield, Optional[str])
    assert_type(doc.intfield, Optional[int])
    assert_type(doc.floatfield, Optional[float])
    assert_type(doc.decimalfield, Optional[Decimal])
    assert_type(doc.booleanfield, Optional[bool])
    assert_type(doc.datetimefield, Optional[datetime])
    assert_type(doc.datefield, Optional[date])
    assert_type(doc.complexdatetimefield, Optional[datetime])
    assert_type(doc.embeddeddocumentfield, Optional[ImageEmbedded])
    assert_type(doc.objectidfield, Optional[ObjectId])
    assert_type(doc.genericembeddeddocumentfield, Optional[Any])
    assert_type(doc.dynamicfield, Optional[Any])
    assert_type(doc.listfield, List[Optional[str]])
    assert_type(doc.sortedlistfield, List[Optional[str]])
    assert_type(doc.embeddeddocumentlistfield, List[ImageEmbedded])
    assert_type(doc.dictfield, Dict[str, str])
    assert_type(doc.mapfield, Dict[str, Optional[str]])
    assert_type(doc.referencefield, Optional[ImageDocument])
    assert_type(doc.cachedreferencefield, Optional[ImageDocument])
    assert_type(doc.lazyreferencefield, ImageDocument)
    assert_type(doc.genericlazyreferencefield, LazyReference[Any])
    assert_type(doc.genericreferencefield, Any)
    assert_type(doc.binaryfield, Optional[bytes])
    assert_type(doc.filefield, Any)
    assert_type(doc.imagefield, Any)
    assert_type(doc.geopointfield, Optional[List[float]])
    assert_type(doc.pointfield, Dict[str, Any])
    assert_type(doc.linestringfield, Dict[str, Any])
    assert_type(doc.polygonfield, Dict[str, Any])
    assert_type(doc.sequencefield, Any)
    assert_type(doc.uuidfield, Optional[UUID])
    assert_type(doc.enumfield, Optional[Color])
    assert_type(doc.multipointfield, Dict[str, Any])
    assert_type(doc.multilinestringfield, Dict[str, Any])
    assert_type(doc.multipolygonfield, Dict[str, Any])
    assert_type(doc.decimal128field, Optional[Decimal])
