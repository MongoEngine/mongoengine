# mypy: enable-error-code="var-annotated"
from typing import Optional

from typing_extensions import assert_type

from mongoengine import Document, EmbeddedDocument
from mongoengine.fields import (
    BinaryField,
    BooleanField,
    CachedReferenceField,
    ComplexDateTimeField,
    DateField,
    DateTimeField,
    Decimal128Field,
    DecimalField,
    DictField,
    DynamicField,
    EmailField,
    EmbeddedDocumentField,
    EmbeddedDocumentListField,
    EnumField,
    FileField,
    FloatField,
    GenericEmbeddedDocumentField,
    GenericLazyReferenceField,
    GenericReferenceField,
    GeoJsonBaseField,
    GeoPointField,
    GridFSError,
    GridFSProxy,
    ImageField,
    ImageGridFsProxy,
    ImproperlyConfigured,
    IntField,
    LazyReferenceField,
    LineStringField,
    ListField,
    LongField,
    MapField,
    MultiLineStringField,
    MultiPointField,
    MultiPolygonField,
    ObjectIdField,
    PointField,
    PolygonField,
    ReferenceField,
    SequenceField,
    SortedListField,
    StringField,
    URLField,
    UUIDField,
)


def test_it_uses_correct_types() -> None:
    class Image(EmbeddedDocument):
        pass

    class Doc(Document):
        stringfield = StringField()
        urlfield = URLField()
        emailfield = EmailField()
        intfield = IntField()
        longfield = LongField()
        floatfield = FloatField()
        decimalfield = DecimalField()
        booleanfield = BooleanField()
        datetimefield = DateTimeField()
        datefield = DateField()
        complexdatetimefield = ComplexDateTimeField()
        embeddeddocumentfield = EmbeddedDocumentField()
        objectidfield = ObjectIdField()
        genericembeddeddocumentfield = GenericEmbeddedDocumentField()
        dynamicfield = DynamicField()
        listfield = ListField()
        sortedlistfield = SortedListField()
        embeddeddocumentlistfield = EmbeddedDocumentListField()
        dictfield = DictField()
        mapfield = MapField()
        referencefield = ReferenceField()
        cachedreferencefield = CachedReferenceField()
        lazyreferencefield = LazyReferenceField()
        genericlazyreferencefield = GenericLazyReferenceField()
        genericreferencefield = GenericReferenceField()
        binaryfield = BinaryField()
        gridfserror = GridFSError()
        gridfsproxy = GridFSProxy()
        filefield = FileField()
        imagegridfsproxy = ImageGridFsProxy()
        improperlyconfigured = ImproperlyConfigured()
        imagefield = ImageField()
        geopointfield = GeoPointField()
        pointfield = PointField()
        linestringfield = LineStringField()
        polygonfield = PolygonField()
        sequencefield = SequenceField()
        uuidfield = UUIDField()
        enumfield = EnumField()
        multipointfield = MultiPointField()
        multilinestringfield = MultiLineStringField()
        multipolygonfield = MultiPolygonField()
        geojsonbasefield = GeoJsonBaseField()
        decimal128field = Decimal128Field()

    doc = Doc()

    assert_type(doc.stringfield, Optional[str])
    assert_type(doc.urlfield, Optional[str])
    assert_type(doc.emailfield, Optional[str])
    assert_type(doc.intfield, Optional[str])
    assert_type(doc.longfield, Optional[str])
    assert_type(doc.floatfield, Optional[str])
    assert_type(doc.decimalfield, Optional[str])
    assert_type(doc.booleanfield, Optional[str])
    assert_type(doc.datetimefield, Optional[str])
    assert_type(doc.datefield, Optional[str])
    assert_type(doc.complexdatetimefield, Optional[str])
    assert_type(doc.embeddeddocumentfield, Optional[str])
    assert_type(doc.objectidfield, Optional[str])
    assert_type(doc.genericembeddeddocumentfield, Optional[str])
    assert_type(doc.dynamicfield, Optional[str])
    assert_type(doc.listfield, Optional[str])
    assert_type(doc.sortedlistfield, Optional[str])
    assert_type(doc.embeddeddocumentlistfield, Optional[str])
    assert_type(doc.dictfield, Optional[str])
    assert_type(doc.mapfield, Optional[str])
    assert_type(doc.referencefield, Optional[str])
    assert_type(doc.cachedreferencefield, Optional[str])
    assert_type(doc.lazyreferencefield, Optional[str])
    assert_type(doc.genericlazyreferencefield, Optional[str])
    assert_type(doc.genericreferencefield, Optional[str])
    assert_type(doc.binaryfield, Optional[str])
    assert_type(doc.gridfserror, Optional[str])
    assert_type(doc.gridfsproxy, Optional[str])
    assert_type(doc.filefield, Optional[str])
    assert_type(doc.imagegridfsproxy, Optional[str])
    assert_type(doc.improperlyconfigured, Optional[str])
    assert_type(doc.imagefield, Optional[str])
    assert_type(doc.geopointfield, Optional[str])
    assert_type(doc.pointfield, Optional[str])
    assert_type(doc.linestringfield, Optional[str])
    assert_type(doc.polygonfield, Optional[str])
    assert_type(doc.sequencefield, Optional[str])
    assert_type(doc.uuidfield, Optional[str])
    assert_type(doc.enumfield, Optional[str])
    assert_type(doc.multipointfield, Optional[str])
    assert_type(doc.multilinestringfield, Optional[str])
    assert_type(doc.multipolygonfield, Optional[str])
    assert_type(doc.geojsonbasefield, Optional[str])
    assert_type(doc.decimal128field, Optional[str])
