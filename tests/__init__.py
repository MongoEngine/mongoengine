import pymongo

from all_warnings import AllWarnings
from document import *
from queryset import *
from fields import *
from migration import *

if not hasattr(pymongo.collection.Collection, 'insert_one'):
    setattr(pymongo.collection.Collection, 'insert_one', pymongo.collection.Collection.insert)
