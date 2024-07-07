# mypy: enable-error-code="var-annotated"
from typing import Any, Optional, List

from typing_extensions import assert_type

from mongoengine import (
    Document,
    FileField,
    IntField,
    ListField,
    StringField,
)


def test_it_uses_correct_types() -> None:
    class Book(Document):
        number = IntField()
        name = StringField()
        authors = ListField(StringField())
        cover = FileField()

    book = Book()

    assert_type(book.number, Optional[int])
    assert_type(book.name, Optional[str])
    assert_type(book.authors, List[Optional[str]])
    assert_type(book.cover, Any)
