=========
Upgrading
=========

0.4 to 0.5
===========

There have been the following backwards incompatibilities from 0.4 to 0.5:

# Choice options:

Are now expected to be an iterable of tuples, with  the first element in each
tuple being the actual value to be stored. The second element is the
human-readable name for the option.

# PyMongo / MongoDB

map reduce now requires pymongo 1.11+ More methods now use map_reduce as db.eval
is not supported for sharding - the following have been changed:

    * sum
    * average
    * item_frequencies

#. Default collection naming.

Previously it was just lowercase, its now much more pythonic and readable as its
lowercase and underscores, previously ::

    class MyAceDocument(Document):
        pass

    MyAceDocument._meta['collection'] == myacedocument

In 0.5 this will change to ::

    class MyAceDocument(Document):
        pass

    MyAceDocument._get_collection_name() == my_ace_document

To upgrade use a Mixin class to set meta like so ::

    class BaseMixin(object):
        meta = {
            'collection': lambda c: c.__name__.lower()
        }

    class MyAceDocument(Document, BaseMixin):
        pass

    MyAceDocument._get_collection_name() == myacedocument

Alternatively, you can rename your collections eg ::

    from mongoengine.connection import _get_db
    from mongoengine.base import _document_registry

    def rename_collections():
        db = _get_db()

        failure = False

        collection_names = [d._get_collection_name() for d in _document_registry.values()]

        for new_style_name in collection_names:
            if not new_style_name:  # embedded documents don't have collections
                continue
            old_style_name = new_style_name.replace('_', '')

            if old_style_name == new_style_name:
                continue  # Nothing to do

            existing = db.collection_names()
            if old_style_name in existing:
                if new_style_name in existing:
                    failure = True
                    print "FAILED to rename: %s to %s (already exists)" % (
                        old_style_name, new_style_name)
                else:
                    db[old_style_name].rename(new_style_name)
                    print "Renamed:  %s to %s" % (old_style_name, new_style_name)

        if failure:
            print "Upgrading  collection names failed"
        else:
            print "Upgraded collection names"

