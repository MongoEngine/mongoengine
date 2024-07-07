# mypy: enable-error-code="var-annotated"
from __future__ import annotations

from typing import Any

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

    assert_type(book.number, int | None)
    assert_type(book.name, str | None)
    assert_type(book.authors, list[str | None])
    assert_type(book.cover, Any)
