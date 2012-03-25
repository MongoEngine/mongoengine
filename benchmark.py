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
    1.1141769886
    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries - MongoEngine
    2.37724113464
    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries - MongoEngine, safe=False, validate=False
    1.92479610443

    0.5.X
    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries - Pymongo
    1.10552310944
    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries - MongoEngine
    16.5169169903
    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries - MongoEngine, safe=False, validate=False
    14.9446101189
    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries - MongoEngine, safe=False, validate=False, cascade=False
    14.912801981
    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries - MongoEngine, force=True
    14.9617750645

    Performance
    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries - Pymongo
    1.10072994232
    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries - MongoEngine
    5.27341103554
    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries - MongoEngine, safe=False, validate=False
    4.49365401268
    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries - MongoEngine, safe=False, validate=False, cascade=False
    4.43459296227
    ----------------------------------------------------------------------------------------------------
    Creating 10000 dictionaries - MongoEngine, force=True
    4.40114378929
    """

    setup = """
from pymongo import Connection
connection = Connection()
connection.drop_database('timeit_test')
"""

    stmt = """
from pymongo import Connection
connection = Connection()

db = connection.timeit_test
noddy = db.noddy

for i in xrange(10000):
    example = {'fields': {}}
    for j in range(20):
        example['fields']["key"+str(j)] = "value "+str(j)

    noddy.insert(example)

myNoddys = noddy.find()
[n for n in myNoddys] # iterate
"""

    print "-" * 100
    print """Creating 10000 dictionaries - Pymongo"""
    t = timeit.Timer(stmt=stmt, setup=setup)
    print t.timeit(1)

    setup = """
from pymongo import Connection
connection = Connection()
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
    for j in range(20):
        noddy.fields["key"+str(j)] = "value "+str(j)
    noddy.save(safe=False, validate=False)

myNoddys = Noddy.objects()
[n for n in myNoddys] # iterate
"""

    print "-" * 100
    print """Creating 10000 dictionaries - MongoEngine, safe=False, validate=False"""
    t = timeit.Timer(stmt=stmt, setup=setup)
    print t.timeit(1)


    stmt = """
for i in xrange(10000):
    noddy = Noddy()
    for j in range(20):
        noddy.fields["key"+str(j)] = "value "+str(j)
    noddy.save(safe=False, validate=False, cascade=False)

myNoddys = Noddy.objects()
[n for n in myNoddys] # iterate
"""

    print "-" * 100
    print """Creating 10000 dictionaries - MongoEngine, safe=False, validate=False, cascade=False"""
    t = timeit.Timer(stmt=stmt, setup=setup)
    print t.timeit(1)

    stmt = """
for i in xrange(10000):
    noddy = Noddy()
    for j in range(20):
        noddy.fields["key"+str(j)] = "value "+str(j)
    noddy.save(force_insert=True, safe=False, validate=False, cascade=False)

myNoddys = Noddy.objects()
[n for n in myNoddys] # iterate
"""

    print "-" * 100
    print """Creating 10000 dictionaries - MongoEngine, force=True"""
    t = timeit.Timer(stmt=stmt, setup=setup)
    print t.timeit(1)

if __name__ == "__main__":
    main()
