import pickle

from mongoengine import Document, IntField, StringField
from tests.utils import MongoDBTestCase


class Person(Document):
    name = StringField()
    age = IntField()


class TestQuerysetPickable(MongoDBTestCase):
    """
    Test for adding pickling support for QuerySet instances
    See issue https://github.com/MongoEngine/mongoengine/issues/442
    """

    def setUp(self):
        super().setUp()
        self.john = Person.objects.create(name="John", age=21)

    def test_picke_simple_qs(self):
        qs = Person.objects.all()
        pickle.dumps(qs)

    def _get_loaded(self, qs):
        s = pickle.dumps(qs)
        return pickle.loads(s)

    def test_unpickle(self):
        qs = Person.objects.all()

        loadedQs = self._get_loaded(qs)

        assert qs.count() == loadedQs.count()

        # can update loadedQs
        loadedQs.update(age=23)

        # check
        assert Person.objects.first().age == 23

    def test_pickle_support_filtration(self):
        Person.objects.create(name="Alice", age=22)

        Person.objects.create(name="Bob", age=23)

        qs = Person.objects.filter(age__gte=22)
        assert qs.count() == 2

        loaded = self._get_loaded(qs)

        assert loaded.count() == 2
        assert loaded.filter(name="Bob").first().age == 23
