from pymongo import ReturnDocument

from mongoengine.base import BaseField
from mongoengine.synchronous.connection import DEFAULT_CONNECTION_NAME, get_db
from mongoengine.session import _get_session
from mongoengine.asynchronous import async_get_db
from mongoengine.document import Document


class SequenceField(BaseField):
    """Provides a sequential counter see:
     https://www.mongodb.com/docs/manual/reference/method/ObjectId/#ObjectIDs-SequenceNumbers

    .. note::

             Although traditional databases often use increasing sequence
             numbers for primary keys. In MongoDB, the preferred approach is to
             use Object IDs instead.  The concept is that in a very large
             cluster of machines, it is easier to create an object ID than have
             global, uniformly increasing sequence numbers.

    :param collection_name:  Name of the counter collection (default 'mongoengine.counters')
    :param sequence_name: Name of the sequence in the collection (default 'ClassName.counter')
    :param value_decorator: Any callable to use as a counter (default int)

    Use any callable as `value_decorator` to transform calculated counter into
    any value suitable for your needs, e.g. string or hexadecimal
    representation of the default integer counter value.

    .. note::

        In case the counter is defined in the abstract document, it will be
        common to all inherited documents and the default sequence name will
        be the class name of the abstract document.
    """

    _auto_gen = True
    COLLECTION_NAME = "mongoengine.counters"
    VALUE_DECORATOR = int

    def __init__(
        self,
        collection_name=None,
        db_alias=None,
        sequence_name=None,
        value_decorator=None,
        *args,
        **kwargs,
    ):
        self.collection_name = collection_name or self.COLLECTION_NAME
        self.db_alias = db_alias or DEFAULT_CONNECTION_NAME
        self.sequence_name = sequence_name
        self.value_decorator = (
            value_decorator if callable(value_decorator) else self.VALUE_DECORATOR
        )
        super().__init__(*args, **kwargs)

    # ============================================================
    # SYNC VERSION
    # ============================================================

    def generate(self):
        """Sync generate."""
        sequence_name = self.get_sequence_name()
        sequence_id = f"{sequence_name}.{self.name}"
        collection = get_db(alias=self.db_alias)[self.collection_name]

        counter = collection.find_one_and_update(
            filter={"_id": sequence_id},
            update={"$inc": {"next": 1}},
            return_document=ReturnDocument.AFTER,
            upsert=True,
            session=_get_session(),
        )
        return self.value_decorator(counter["next"])

    def set_next_value(self, value):
        sequence_name = self.get_sequence_name()
        sequence_id = f"{sequence_name}.{self.name}"
        collection = get_db(alias=self.db_alias)[self.collection_name]

        counter = collection.find_one_and_update(
            {"_id": sequence_id},
            {"$set": {"next": value}},
            return_document=ReturnDocument.AFTER,
            upsert=True,
            session=_get_session(),
        )
        return self.value_decorator(counter["next"])

    async def aset_next_value(self, value):
        sequence_name = self.get_sequence_name()
        sequence_id = f"{sequence_name}.{self.name}"
        collection = (await async_get_db(alias=self.db_alias))[self.collection_name]

        counter = await collection.find_one_and_update(
            {"_id": sequence_id},
            {"$set": {"next": value}},
            return_document=ReturnDocument.AFTER,
            upsert=True,
            session=_get_session(),
        )
        return self.value_decorator(counter["next"])

    def get_next_value(self):
        sequence_name = self.get_sequence_name()
        sequence_id = f"{sequence_name}.{self.name}"
        collection = get_db(alias=self.db_alias)[self.collection_name]

        data = collection.find_one({"_id": sequence_id}, session=_get_session())
        if data:
            return self.value_decorator(data["next"] + 1)
        return self.value_decorator(1)

    async def aget_next_value(self):
        sequence_name = self.get_sequence_name()
        sequence_id = f"{sequence_name}.{self.name}"
        collection = (await async_get_db(alias=self.db_alias))[self.collection_name]

        data = await collection.find_one({"_id": sequence_id}, session=_get_session())
        if data:
            return self.value_decorator(data["next"] + 1)
        return self.value_decorator(1)

    # ============================================================
    # ASYNC VERSION
    # ============================================================

    async def async_generate(self):
        """Async generate and increment counter."""
        sequence_name = self.get_sequence_name()
        sequence_id = f"{sequence_name}.{self.name}"
        async_col = (await async_get_db(alias=self.db_alias))[self.collection_name]

        counter = await async_col.find_one_and_update(
            filter={"_id": sequence_id},
            update={"$inc": {"next": 1}},
            return_document=ReturnDocument.AFTER,
            upsert=True,
            session=_get_session(),
        )
        return self.value_decorator(counter["next"])

    async def async_set_next_value(self, value):
        sequence_name = self.get_sequence_name()
        sequence_id = f"{sequence_name}.{self.name}"
        async_col = (await async_get_db(alias=self.db_alias))[self.collection_name]

        counter = await async_col.find_one_and_update(
            {"_id": sequence_id},
            {"$set": {"next": value}},
            return_document=ReturnDocument.AFTER,
            upsert=True,
            session=_get_session(),
        )
        return self.value_decorator(counter["next"])

    async def async_get_next_value(self):
        sequence_name = self.get_sequence_name()
        sequence_id = f"{sequence_name}.{self.name}"
        async_col = (await async_get_db(alias=self.db_alias))[self.collection_name]

        data = await async_col.find_one({"_id": sequence_id}, session=_get_session())
        if data:
            return self.value_decorator(data["next"] + 1)
        return self.value_decorator(1)

    # ============================================================
    # SHARED UTILS
    # ============================================================

    def get_sequence_name(self):
        if self.sequence_name:
            return self.sequence_name

        owner = self.owner_document
        if issubclass(owner, Document) and not owner._meta.get("abstract"):
            return owner._get_collection_name()

        # Abstract class → generate name
        return (
            "".join("_%s" % c if c.isupper() else c for c in owner._class_name)
            .strip("_")
            .lower()
        )

    def __get__(self, instance, owner):
        if instance is None:
            return self

        value = super().__get__(instance, owner)

        if value is None and instance._initialised:
            value = self.generate()
            instance._data[self.name] = value
            instance._mark_as_changed(self.name)

        return value

    async def aget(self, instance, owner):
        if instance is None:
            return self

        value = super().__get__(instance, owner)

        if value is None and instance._initialised:
            value = await self.async_generate()
            instance._data[self.name] = value
            instance._mark_as_changed(self.name)

        return value

    def __set__(self, instance, value):
        # If value is None, auto-generate
        if value is None and instance._initialised:
            value = None

        return super().__set__(instance, value)

    async def aset(self, instance, value):
        # If value is None, auto-generate
        if value is None and instance._initialised:
            value = await self.async_generate()

        return super().__set__(instance, value)

    def prepare_query_value(self, op, value):
        return self.value_decorator(value)


__all__ = ("SequenceField",)
