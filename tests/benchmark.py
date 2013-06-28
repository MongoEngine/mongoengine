from mongoengine import *
from timeit import repeat
import unittest

conn_settings = {
    'db': 'mongomallard-test',
}

connect(**conn_settings)

def timeit(f, n=10000):
    return min(repeat(f, repeat=3, number=n))/float(n)

class BenchmarkTestCase(unittest.TestCase):
    def setUp(self):
        pass

    def test_basic(self):
        class Book(Document):
            name = StringField()
            pages = IntField()
            tags = ListField(StringField())
            is_published = BooleanField()

        Book.drop_collection()

        create_book = lambda: Book(name='Always be closing', pages=100, tags=['self-help', 'sales'], is_published=True)
        print 'Doc initialization: %.3fus' % (timeit(create_book, 1000) * 10**6)

        b = create_book()

        print 'Doc getattr: %.3fus' % (timeit(lambda: b.name, 10000) * 10**6)

        print 'Doc setattr: %.3fus' % (timeit(lambda: setattr(b, 'name', 'New name'), 10000) * 10**6)

        print 'Doc to mongo: %.3fus' % (timeit(b.to_mongo, 1000) * 10**6)

        def save_book():
            b._mark_as_changed('name')
            b._mark_as_changed('tags')
            b.save()

        save_book()
        son = b.to_mongo()

        print 'Load from SON: %.3fus'  % (timeit(lambda: Book._from_son(son), 1000) * 10**6)

        print 'Save to database: %.3fus' % (timeit(save_book, 100) * 10**6)

        print 'Load from database: %.3fus' % (timeit(lambda: Book.objects[0], 100) * 10**6)

    def test_embedded(self):
        class Contact(EmbeddedDocument):
            name = StringField()
            title = StringField()
            address = StringField()

        class Company(Document):
            name = StringField()
            contacts = ListField(EmbeddedDocumentField(Contact))

        Company.drop_collection()

        def get_company():
            return Company(
                name='Elastic',
                contacts=[
                    Contact(
                        name='Contact %d' % x,
                        title='CEO',
                        address='Address %d' % x,
                    )
                for x in range(1000)]
            )

        def create_company():
            c = get_company()
            c.save()
            c.delete()

        print 'Save/delete big object to database: %.3fms' % (timeit(create_company, 10) * 10**3)

        c = get_company().save()

        print 'Serialize big object from database: %.3fms' % (timeit(c.to_mongo, 100) * 10**3)
        print 'Load big object from database: %.3fms' % (timeit(lambda: Company.objects[0], 100) * 10**3)


if __name__ == '__main__':
    unittest.main()
