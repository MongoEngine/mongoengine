from bson import DBRef

from mongoengine.errors import DoesNotExist


class LazyReference(DBRef):
    __slots__ = ("_cached_doc", "passthrough", "document_type", "_async")

    def fetch(self, force=False):
        self.document_type._get_db()
        if not self._cached_doc or force:
            self._cached_doc = self.document_type.objects.get(pk=self.pk)
            if not self._cached_doc:
                raise DoesNotExist("Trying to dereference unknown document %s" % (self))
        return self._cached_doc

    async def afetch(self, force=False):
        await self.document_type._async_get_db()
        if not self._cached_doc or force:
            self._cached_doc = await self.document_type.aobjects.get(pk=self.pk)
            if not self._cached_doc:
                raise DoesNotExist("Trying to dereference unknown document %s" % (self))
        return self._cached_doc

    @property
    def pk(self):
        return self.id

    @property
    def value(self):
        return {
            "_ref": DBRef(self.document_type._get_collection_name(), self.id),
            "_cls": self.document_type.__name__,
        }

    def to_dbref(self):
        return DBRef(self.document_type._get_collection_name(), self.id)

    def __init__(
        self, document_type, pk, cached_doc=None, passthrough=False, _async=False
    ):
        self.document_type = document_type
        self._cached_doc = cached_doc
        self.passthrough = passthrough
        self._async = _async
        super().__init__(self.document_type._get_collection_name(), pk)

    def __getitem__(self, name):
        if not object.__getattribute__(self, "passthrough"):
            raise AttributeError()
        if not self.passthrough:
            raise KeyError()
        document = self.fetch()
        return document[name]

    def __getattr__(self, name):
        if not object.__getattribute__(self, "passthrough"):
            raise AttributeError()
        if not self._cached_doc:
            self.fetch()
        document = self._cached_doc
        try:
            return document[name]
        except KeyError:
            raise AttributeError()

    def __repr__(self):
        return f"<LazyReference({self.document_type}, {self.pk!r})>"


__all__ = ("LazyReference",)
