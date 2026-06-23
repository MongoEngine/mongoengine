import pickle

from mongoengine import Document, IntField, StringField
from tests.asynchronous.utils import MongoDBAsyncTestCase


class Person(Document):
    name = StringField()
    age = IntField()


class TestQuerysetPickable(MongoDBAsyncTestCase):
    """
    Test for adding pickling support for QuerySet instances
    See issue https://github.com/MongoEngine/mongoengine/issues/442
    """

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.john = await Person.aobjects.create(name="John", age=21)

    async def test_picke_simple_qs(self):
        qs = Person.aobjects.all()
        pickle.dumps(qs)

    async def _get_loaded(self, qs):
        s = pickle.dumps(qs)
        return pickle.loads(s)

    async def test_unpickle(self):
        qs = Person.aobjects.all()

        loadedQs = await self._get_loaded(qs)

        assert await qs.count() == await loadedQs.count()

        # can update loadedQs
        await loadedQs.update(age=23)

        # check
        assert (await Person.aobjects.first()).age == 23

    async def test_pickle_support_filtration(self):
        await Person.aobjects.create(name="Alice", age=22)

        await Person.aobjects.create(name="Bob", age=23)

        qs = Person.aobjects.filter(age__gte=22)
        assert await qs.count() == 2

        loaded = await self._get_loaded(qs)

        assert await loaded.count() == 2
        assert (await loaded.filter(name="Bob").first()).age == 23
