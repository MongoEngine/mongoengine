import timeit


def main():
    setup = """
from pymongo import MongoClient

connection = MongoClient()
connection.drop_database('mongoengine_benchmark_test')
"""

    stmt = """
from pymongo import MongoClient

connection = MongoClient()

db = connection.mongoengine_benchmark_test
noddy = db.noddy

for i in range(10000):
    example = {'fields': {}}
    for j in range(20):
        example['fields']["key"+str(j)] = "value "+str(j)

    noddy.insert_one(example)

myNoddys = noddy.find()
[n for n in myNoddys]  # iterate
"""

    print("-" * 100)
    print("PyMongo: Creating 10000 dictionaries.")
    t = timeit.Timer(stmt=stmt, setup=setup)
    print("{}s".format(t.timeit(1)))

    stmt = """
from pymongo import MongoClient, WriteConcern
connection = MongoClient()

db = connection.mongoengine_benchmark_test
noddy = db.noddy.with_options(write_concern=WriteConcern(w=0))

for i in range(10000):
    example = {'fields': {}}
    for j in range(20):
        example['fields']["key"+str(j)] = "value "+str(j)

    noddy.insert_one(example)

myNoddys = noddy.find()
[n for n in myNoddys]  # iterate
"""

    print("-" * 100)
    print('PyMongo: Creating 10000 dictionaries (write_concern={"w": 0}).')
    t = timeit.Timer(stmt=stmt, setup=setup)
    print("{}s".format(t.timeit(1)))

    setup = """
from pymongo import MongoClient

connection = MongoClient()
connection.drop_database('mongoengine_benchmark_test')
connection.close()

from mongoengine import Document, DictField, connect
connect("mongoengine_benchmark_test")

class Noddy(Document):
    fields = DictField()
"""

    stmt = """
for i in range(10000):
    noddy = Noddy()
    for j in range(20):
        noddy.fields["key"+str(j)] = "value "+str(j)
    noddy.save()

myNoddys = Noddy.objects()
[n for n in myNoddys]  # iterate
"""

    print("-" * 100)
    print("MongoEngine: Creating 10000 dictionaries.")
    t = timeit.Timer(stmt=stmt, setup=setup)
    print("{}s".format(t.timeit(1)))

    stmt = """
for i in range(10000):
    noddy = Noddy()
    fields = {}
    for j in range(20):
        fields["key"+str(j)] = "value "+str(j)
    noddy.fields = fields
    noddy.save()

myNoddys = Noddy.objects()
[n for n in myNoddys]  # iterate
"""

    print("-" * 100)
    print("MongoEngine: Creating 10000 dictionaries (using a single field assignment).")
    t = timeit.Timer(stmt=stmt, setup=setup)
    print("{}s".format(t.timeit(1)))

    stmt = """
for i in range(10000):
    noddy = Noddy()
    for j in range(20):
        noddy.fields["key"+str(j)] = "value "+str(j)
    noddy.save(write_concern={"w": 0})

myNoddys = Noddy.objects()
[n for n in myNoddys] # iterate
"""

    print("-" * 100)
    print('MongoEngine: Creating 10000 dictionaries (write_concern={"w": 0}).')
    t = timeit.Timer(stmt=stmt, setup=setup)
    print("{}s".format(t.timeit(1)))

    stmt = """
for i in range(10000):
    noddy = Noddy()
    for j in range(20):
        noddy.fields["key"+str(j)] = "value "+str(j)
    noddy.save(write_concern={"w": 0}, validate=False)

myNoddys = Noddy.objects()
[n for n in myNoddys] # iterate
"""

    print("-" * 100)
    print(
        'MongoEngine: Creating 10000 dictionaries (write_concern={"w": 0}, validate=False).'
    )
    t = timeit.Timer(stmt=stmt, setup=setup)
    print("{}s".format(t.timeit(1)))

    stmt = """
for i in range(10000):
    noddy = Noddy()
    for j in range(20):
        noddy.fields["key"+str(j)] = "value "+str(j)
    noddy.save(force_insert=True, write_concern={"w": 0}, validate=False)

myNoddys = Noddy.objects()
[n for n in myNoddys] # iterate
"""

    print("-" * 100)
    print(
        'MongoEngine: Creating 10000 dictionaries (force_insert=True, write_concern={"w": 0}, validate=False).'
    )
    t = timeit.Timer(stmt=stmt, setup=setup)
    print("{}s".format(t.timeit(1)))


if __name__ == "__main__":
    main()
