#!/usr/bin/env python

import timeit


def cprofile_main():
    from pymongo import Connection
    connection = Connection()
    connection.drop_database('timeit_test')
    connection.disconnect()

    from mongoengine import Document, DictField, connect
    connect("timeit_test")

    class Noddy(Document):
        fields = DictField()

    for i in xrange(1):
        noddy = Noddy()
        for j in range(20):
            noddy.fields["key" + str(j)] = "value " + str(j)
        noddy.save()


def main():
    """
    0.4 Performance Figures ...

    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries - Pymongo
    3.86744189262
    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries - MongoEngine
    6.23374891281
    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries - MongoEngine, safe=False, validate=False
    5.33027005196
    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries - MongoEngine, safe=False, validate=False, cascade=False
    pass - No Cascade

    0.5.X
    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries - Pymongo
    3.89597702026
    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries - MongoEngine
    21.7735359669
    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries - MongoEngine, safe=False, validate=False
    19.8670389652
    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries - MongoEngine, safe=False, validate=False, cascade=False
    pass - No Cascade

    0.6.X
    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries - Pymongo
    3.81559205055
    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries - MongoEngine
    10.0446798801
    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries - MongoEngine, safe=False, validate=False
    9.51354718208
    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries - MongoEngine, safe=False, validate=False, cascade=False
    9.02567505836
    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries - MongoEngine, force=True
    8.44933390617

    0.7.X
    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries - Pymongo
    3.78801012039
    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries - MongoEngine
    9.73050498962
    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries - MongoEngine, safe=False, validate=False
    8.33456707001
    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries - MongoEngine, safe=False, validate=False, cascade=False
    8.37778115273
    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries - MongoEngine, force=True
    8.36906409264
    0.8.X
    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries - Pymongo
    3.69964408875
    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries - Pymongo write_concern={"w": 0}
    3.5526599884
    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries - MongoEngine
    7.00959801674
    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries without continual assign - MongoEngine
    5.60943293571
    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries - MongoEngine - write_concern={"w": 0}, cascade=True
    6.715102911
    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries - MongoEngine, write_concern={"w": 0}, validate=False, cascade=True
    5.50644683838
    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries - MongoEngine, write_concern={"w": 0}, validate=False
    4.69851183891
    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries - MongoEngine, force_insert=True, write_concern={"w": 0}, validate=False
    4.68946313858
    ----------------------------------------------------------------------------------------------------
    """

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

for i in xrange(10000):
    example = {'fields': {}}
    for j in range(20):
        example['fields']["key"+str(j)] = "value "+str(j)

    noddy.save(example)

myNoddys = noddy.find()
[n for n in myNoddys] # iterate
"""

    print "-" * 100
    print """Creating 10000 dictionaries - Pymongo"""
    t = timeit.Timer(stmt=stmt, setup=setup)
    print t.timeit(1)

    stmt = """
from pymongo import MongoClient
connection = MongoClient()

db = connection.timeit_test
noddy = db.noddy

for i in xrange(10000):
    example = {'fields': {}}
    for j in range(20):
        example['fields']["key"+str(j)] = "value "+str(j)

    noddy.save(example, write_concern={"w": 0})

myNoddys = noddy.find()
[n for n in myNoddys] # iterate
"""

    print "-" * 100
    print """Creating 10000 dictionaries - Pymongo write_concern={"w": 0}"""
    t = timeit.Timer(stmt=stmt, setup=setup)
    print t.timeit(1)

    setup = """
from pymongo import MongoClient
connection = MongoClient()
connection.drop_database('timeit_test')
connection.disconnect()

from mongoengine import Document, DictField, connect
connect("timeit_test")

class Noddy(Document):
    fields = DictField()
"""

    stmt = """
for i in xrange(10000):
    noddy = Noddy()
    for j in range(20):
        noddy.fields["key"+str(j)] = "value "+str(j)
    noddy.save()

myNoddys = Noddy.objects()
[n for n in myNoddys] # iterate
"""

    print "-" * 100
    print """Creating 10000 dictionaries - MongoEngine"""
    t = timeit.Timer(stmt=stmt, setup=setup)
    print t.timeit(1)

    stmt = """
for i in xrange(10000):
    noddy = Noddy()
    fields = {}
    for j in range(20):
        fields["key"+str(j)] = "value "+str(j)
    noddy.fields = fields
    noddy.save()

myNoddys = Noddy.objects()
[n for n in myNoddys] # iterate
"""

    print "-" * 100
    print """Creating 10000 dictionaries without continual assign - MongoEngine"""
    t = timeit.Timer(stmt=stmt, setup=setup)
    print t.timeit(1)

    stmt = """
for i in xrange(10000):
    noddy = Noddy()
    for j in range(20):
        noddy.fields["key"+str(j)] = "value "+str(j)
    noddy.save(write_concern={"w": 0}, cascade=True)

myNoddys = Noddy.objects()
[n for n in myNoddys] # iterate
"""

    print "-" * 100
    print """Creating 10000 dictionaries - MongoEngine - write_concern={"w": 0}, cascade = True"""
    t = timeit.Timer(stmt=stmt, setup=setup)
    print t.timeit(1)

    stmt = """
for i in xrange(10000):
    noddy = Noddy()
    for j in range(20):
        noddy.fields["key"+str(j)] = "value "+str(j)
    noddy.save(write_concern={"w": 0}, validate=False, cascade=True)

myNoddys = Noddy.objects()
[n for n in myNoddys] # iterate
"""

    print "-" * 100
    print """Creating 10000 dictionaries - MongoEngine, write_concern={"w": 0}, validate=False, cascade=True"""
    t = timeit.Timer(stmt=stmt, setup=setup)
    print t.timeit(1)

    stmt = """
for i in xrange(10000):
    noddy = Noddy()
    for j in range(20):
        noddy.fields["key"+str(j)] = "value "+str(j)
    noddy.save(validate=False, write_concern={"w": 0})

myNoddys = Noddy.objects()
[n for n in myNoddys] # iterate
"""

    print "-" * 100
    print """Creating 10000 dictionaries - MongoEngine, write_concern={"w": 0}, validate=False"""
    t = timeit.Timer(stmt=stmt, setup=setup)
    print t.timeit(1)

    stmt = """
for i in xrange(10000):
    noddy = Noddy()
    for j in range(20):
        noddy.fields["key"+str(j)] = "value "+str(j)
    noddy.save(force_insert=True, write_concern={"w": 0}, validate=False)

myNoddys = Noddy.objects()
[n for n in myNoddys] # iterate
"""

    print "-" * 100
    print """Creating 10000 dictionaries - MongoEngine, force_insert=True, write_concern={"w": 0}, validate=False"""
    t = timeit.Timer(stmt=stmt, setup=setup)
    print t.timeit(1)


if __name__ == "__main__":
    main()