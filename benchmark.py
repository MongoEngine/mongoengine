#!/usr/bin/env python

"""
Simple benchmark comparing PyMongo and MongoEngine.

Sample run on a mid 2015 MacBook Pro (commit b282511):

Benchmarking...
----------------------------------------------------------------------------------------------------
Creating 10000 dictionaries - Pymongo
2.58979988098
----------------------------------------------------------------------------------------------------
Creating 10000 dictionaries - Pymongo write_concern={"w": 0}
1.26657605171
----------------------------------------------------------------------------------------------------
Creating 10000 dictionaries - MongoEngine
8.4351580143
----------------------------------------------------------------------------------------------------
Creating 10000 dictionaries without continual assign - MongoEngine
7.20191693306
----------------------------------------------------------------------------------------------------
Creating 10000 dictionaries - MongoEngine - write_concern={"w": 0}, cascade = True
6.31104588509
----------------------------------------------------------------------------------------------------
Creating 10000 dictionaries - MongoEngine, write_concern={"w": 0}, validate=False, cascade=True
6.07083487511
----------------------------------------------------------------------------------------------------
Creating 10000 dictionaries - MongoEngine, write_concern={"w": 0}, validate=False
5.97704291344
----------------------------------------------------------------------------------------------------
Creating 10000 dictionaries - MongoEngine, force_insert=True, write_concern={"w": 0}, validate=False
5.9111430645
"""

import timeit


def main():
    print("Benchmarking...")

    setup = """
from pymongo import MongoClient
connection = MongoClient()
connection.drop_database('timeit_test')
"""

    stmt = """
from pymongo import MongoClient
connection = MongoClient()

db = connection.timeit_test
noddy = db.noddy

for i in range(10000):
    example = {'fields': {}}
    for j in range(20):
        example['fields']['key' + str(j)] = 'value ' + str(j)

    noddy.save(example)

myNoddys = noddy.find()
[n for n in myNoddys] # iterate
"""

    print("-" * 100)
    print("""Creating 10000 dictionaries - Pymongo""")
    t = timeit.Timer(stmt=stmt, setup=setup)
    print(t.timeit(1))

    stmt = """
from pymongo import MongoClient
from pymongo.write_concern import WriteConcern
connection = MongoClient()

db = connection.get_database('timeit_test', write_concern=WriteConcern(w=0))
noddy = db.noddy

for i in range(10000):
    example = {'fields': {}}
    for j in range(20):
        example['fields']["key"+str(j)] = "value "+str(j)

    noddy.save(example)

myNoddys = noddy.find()
[n for n in myNoddys] # iterate
"""

    print("-" * 100)
    print("""Creating 10000 dictionaries - Pymongo write_concern={"w": 0}""")
    t = timeit.Timer(stmt=stmt, setup=setup)
    print(t.timeit(1))

    setup = """
from pymongo import MongoClient
connection = MongoClient()
connection.drop_database('timeit_test')
connection.close()

from mongoengine import Document, DictField, connect
connect('timeit_test')

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
[n for n in myNoddys] # iterate
"""

    print("-" * 100)
    print("""Creating 10000 dictionaries - MongoEngine""")
    t = timeit.Timer(stmt=stmt, setup=setup)
    print(t.timeit(1))

    stmt = """
for i in range(10000):
    noddy = Noddy()
    fields = {}
    for j in range(20):
        fields["key"+str(j)] = "value "+str(j)
    noddy.fields = fields
    noddy.save()

myNoddys = Noddy.objects()
[n for n in myNoddys] # iterate
"""

    print("-" * 100)
    print("""Creating 10000 dictionaries without continual assign - MongoEngine""")
    t = timeit.Timer(stmt=stmt, setup=setup)
    print(t.timeit(1))

    stmt = """
for i in range(10000):
    noddy = Noddy()
    for j in range(20):
        noddy.fields["key"+str(j)] = "value "+str(j)
    noddy.save(write_concern={"w": 0}, cascade=True)

myNoddys = Noddy.objects()
[n for n in myNoddys] # iterate
"""

    print("-" * 100)
    print("""Creating 10000 dictionaries - MongoEngine - write_concern={"w": 0}, cascade = True""")
    t = timeit.Timer(stmt=stmt, setup=setup)
    print(t.timeit(1))

    stmt = """
for i in range(10000):
    noddy = Noddy()
    for j in range(20):
        noddy.fields["key"+str(j)] = "value "+str(j)
    noddy.save(write_concern={"w": 0}, validate=False, cascade=True)

myNoddys = Noddy.objects()
[n for n in myNoddys] # iterate
"""

    print("-" * 100)
    print("""Creating 10000 dictionaries - MongoEngine, write_concern={"w": 0}, validate=False, cascade=True""")
    t = timeit.Timer(stmt=stmt, setup=setup)
    print(t.timeit(1))

    stmt = """
for i in range(10000):
    noddy = Noddy()
    for j in range(20):
        noddy.fields["key"+str(j)] = "value "+str(j)
    noddy.save(validate=False, write_concern={"w": 0})

myNoddys = Noddy.objects()
[n for n in myNoddys] # iterate
"""

    print("-" * 100)
    print("""Creating 10000 dictionaries - MongoEngine, write_concern={"w": 0}, validate=False""")
    t = timeit.Timer(stmt=stmt, setup=setup)
    print(t.timeit(1))

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
    print("""Creating 10000 dictionaries - MongoEngine, force_insert=True, write_concern={"w": 0}, validate=False""")
    t = timeit.Timer(stmt=stmt, setup=setup)
    print(t.timeit(1))


if __name__ == "__main__":
    main()
