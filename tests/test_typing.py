# mypy: enable-error-code="var-annotated"
from typing import Any, List, Optional

from typing_extensions import assert_type, reveal_type

from mongoengine import (
    Document,
    EmbeddedDocument,
    EmbeddedDocumentListField,
    FileField,
    IntField,
    ListField,
    StringField,
)


def test_it_uses_correct_types() -> None:
    class Image(EmbeddedDocument):
        pass

    class Book(Document):
        number = IntField()
        name = StringField()
        authors = ListField(StringField())
        cover = FileField()
        images = EmbeddedDocumentListField(Image)

    book = Book()
    reveal_type(Book.authors)
    reveal_type(book.authors)

    assert_type(book.number, Optional[int])
    assert_type(book.name, Optional[str])
    assert_type(book.authors, List[Optional[str]])
    assert_type(book.cover, Any)
    assert_type(book.images, List[Image])

    book.authors = ["Ok"]
