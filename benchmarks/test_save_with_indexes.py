import timeit


def main():
    setup = """
from pymongo import MongoClient

connection = MongoClient()
connection.drop_database("mongoengine_benchmark_test")
connection.close()

from mongoengine import connect, Document, IntField, StringField
connect("mongoengine_benchmark_test", w=1)

class User0(Document):
    name = StringField()
    age = IntField()

class User1(Document):
    name = StringField()
    age = IntField()
    meta = {"indexes": [["name"]]}

class User2(Document):
    name = StringField()
    age = IntField()
    meta = {"indexes": [["name", "age"]]}

class User3(Document):
    name = StringField()
    age = IntField()
    meta = {"indexes": [["name"]], "auto_create_index_on_save": True}

class User4(Document):
    name = StringField()
    age = IntField()
    meta = {"indexes": [["name", "age"]], "auto_create_index_on_save": True}
"""

    stmt = """
for i in range(10000):
    User0(name="Nunu", age=9).save()
"""
    print("-" * 80)
    print("Save 10000 documents with 0 indexes.")
    t = timeit.Timer(stmt=stmt, setup=setup)
    print(f"{min(t.repeat(repeat=3, number=1))}s")

    stmt = """
for i in range(10000):
    User1(name="Nunu", age=9).save()
"""
    print("-" * 80)
    print("Save 10000 documents with 1 index.")
    t = timeit.Timer(stmt=stmt, setup=setup)
    print(f"{min(t.repeat(repeat=3, number=1))}s")

    stmt = """
for i in range(10000):
    User2(name="Nunu", age=9).save()
"""
    print("-" * 80)
    print("Save 10000 documents with 2 indexes.")
    t = timeit.Timer(stmt=stmt, setup=setup)
    print(f"{min(t.repeat(repeat=3, number=1))}s")

    stmt = """
for i in range(10000):
    User3(name="Nunu", age=9).save()
"""
    print("-" * 80)
    print("Save 10000 documents with 1 index (auto_create_index_on_save=True).")
    t = timeit.Timer(stmt=stmt, setup=setup)
    print(f"{min(t.repeat(repeat=3, number=1))}s")

    stmt = """
for i in range(10000):
    User4(name="Nunu", age=9).save()
"""
    print("-" * 80)
    print("Save 10000 documents with 2 indexes (auto_create_index_on_save=True).")
    t = timeit.Timer(stmt=stmt, setup=setup)
    print(f"{min(t.repeat(repeat=3, number=1))}s")


if __name__ == "__main__":
    main()
