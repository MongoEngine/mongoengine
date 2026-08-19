import pytest

from mongoengine import Document
from mongoengine.base import _DocumentRegistry, common as base_common
from mongoengine.common import _import_class


class TestCommon:
    def test__import_class(self):
        doc_cls = _import_class("Document")
        assert doc_cls is Document

    def test__import_class_raise_if_not_known(self):
        with pytest.raises(ValueError):
            _import_class("UnknownClass")

    def test_document_registry_warns_for_same_class_name_from_different_modules(
        self, monkeypatch
    ):
        monkeypatch.setattr(base_common, "_document_registry", {})
        first = type(
            "DuplicateDocument",
            (),
            {"_class_name": "DuplicateDocument", "__module__": "first"},
        )
        second = type(
            "DuplicateDocument",
            (),
            {"_class_name": "DuplicateDocument", "__module__": "second"},
        )

        _DocumentRegistry.register(first)
        with pytest.warns(UserWarning, match="Multiple Document classes named"):
            _DocumentRegistry.register(second)
