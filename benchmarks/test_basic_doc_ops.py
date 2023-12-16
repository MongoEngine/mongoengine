from timeit import repeat

import mongoengine
from mongoengine import (
    BooleanField,
    Document,
    EmailField,
    EmbeddedDocument,
    EmbeddedDocumentField,
    IntField,
    ListField,
    StringField,
)

mongoengine.connect(db="mongoengine_benchmark_test", w=1)


def timeit(f, n=10000):
    return min(repeat(f, repeat=3, number=n)) / float(n)


def test_basic():
    class Book(Document):
        name = StringField()
        pages = IntField()
        tags = ListField(StringField())
        is_published = BooleanField()
        author_email = EmailField()

    Book.drop_collection()

    def init_book():
        return Book(
            name="Always be closing",
            pages=100,
            tags=["self-help", "sales"],
            is_published=True,
            author_email="alec@example.com",
        )

    print("Doc initialization: %.3fus" % (timeit(init_book, 1000) * 10**6))

    b = init_book()
    print("Doc getattr: %.3fus" % (timeit(lambda: b.name, 10000) * 10**6))

    print(
        "Doc setattr: %.3fus"
        % (timeit(lambda: setattr(b, "name", "New name"), 10000) * 10**6)  # noqa B010
    )

    print("Doc to mongo: %.3fus" % (timeit(b.to_mongo, 1000) * 10**6))

    print("Doc validation: %.3fus" % (timeit(b.validate, 1000) * 10**6))

    def save_book():
        b._mark_as_changed("name")
        b._mark_as_changed("tags")
        b.save()

    print("Save to database: %.3fus" % (timeit(save_book, 100) * 10**6))

    son = b.to_mongo()
    print(
        "Load from SON: %.3fus" % (timeit(lambda: Book._from_son(son), 1000) * 10**6)
    )

    print(
        "Load from database: %.3fus" % (timeit(lambda: Book.objects[0], 100) * 10**6)
    )

    def create_and_delete_book():
        b = init_book()
        b.save()
        b.delete()

    print(
        "Init + save to database + delete: %.3fms"
        % (timeit(create_and_delete_book, 10) * 10**3)
    )


def test_big_doc():
    class Contact(EmbeddedDocument):
        name = StringField()
        title = StringField()
        address = StringField()

    class Company(Document):
        name = StringField()
        contacts = ListField(EmbeddedDocumentField(Contact))

    Company.drop_collection()

    def init_company():
        return Company(
            name="MongoDB, Inc.",
            contacts=[
                Contact(name="Contact %d" % x, title="CEO", address="Address %d" % x)
                for x in range(1000)
            ],
        )

    company = init_company()
    print("Big doc to mongo: %.3fms" % (timeit(company.to_mongo, 100) * 10**3))

    print("Big doc validation: %.3fms" % (timeit(company.validate, 1000) * 10**3))

    company.save()

    def save_company():
        company._mark_as_changed("name")
        company._mark_as_changed("contacts")
        company.save()

    print("Save to database: %.3fms" % (timeit(save_company, 100) * 10**3))

    son = company.to_mongo()
    print(
        "Load from SON: %.3fms"
        % (timeit(lambda: Company._from_son(son), 100) * 10**3)
    )

    print(
        "Load from database: %.3fms"
        % (timeit(lambda: Company.objects[0], 100) * 10**3)
    )

    def create_and_delete_company():
        c = init_company()
        c.save()
        c.delete()

    print(
        "Init + save to database + delete: %.3fms"
        % (timeit(create_and_delete_company, 10) * 10**3)
    )


if __name__ == "__main__":
    test_basic()
    print("-" * 100)
    test_big_doc()
