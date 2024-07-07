# mypy: enable-error-code="var-annotated"
from typing import Any, Dict, List, Optional

from typing_extensions import assert_type, reveal_type

from mongoengine import (
    Document,
    EmbeddedDocument,
    EmbeddedDocumentListField,
    FileField,
    IntField,
    ListField,
    StringField,
    DictField,
)
from mongoengine.fields import EmbeddedDocumentField


def test_it_uses_correct_types() -> None:
    class Image(EmbeddedDocument):
        pass

    class Book(Document):
        number = IntField()
        name = StringField()
        authors = ListField(StringField())
        cover = FileField()
        dict = DictField(StringField())
        embedded_doc = EmbeddedDocumentField(Image)
        images = EmbeddedDocumentListField(Image)

    book = Book()

    assert_type(book.number, Optional[int])
    assert_type(book.name, Optional[str])
    assert_type(book.authors, List[Optional[str]])
    assert_type(book.cover, Any)
    assert_type(book.images, List[Image])
    assert_type(book.dict, Dict[str, Optional[str]])
    assert_type(book.embedded_doc, Optional[Image])

    book.authors = ["Ok"]
