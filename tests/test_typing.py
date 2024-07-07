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


def test_it_uses_correct_types():
    class Book(Document):
        number = IntField()
        name = StringField()
        authors = ListField(StringField())
        cover = FileField()

    book = Book()

    assert_type(book.number, int)
    assert_type(book.name, str)
    assert_type(book.authors, list[str])
    assert_type(book.cover, Any)
