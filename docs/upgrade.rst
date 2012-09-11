=========
Upgrading
=========

0.6 to 0.7
==========

Cascade saves
-------------

Saves will raise a `FutureWarning` if they cascade and cascade hasn't been set
to True.  This is because in 0.8 it will default to False.  If you require
cascading saves then either set it in the `meta` or pass
via `save` eg ::

    # At the class level:
    class Person(Document):
        meta = {'cascade': True}

    # Or in code:
    my_document.save(cascade=True)

.. note ::
    Remember: cascading saves **do not** cascade through lists.

ReferenceFields
---------------

ReferenceFields now can store references as ObjectId strings instead of DBRefs.
This will become the default in 0.8 and if `dbref` is not set a `FutureWarning`
will be raised.


To explicitly continue to use DBRefs change the `dbref` flag
to True ::

   class Person(Document):
       groups = ListField(ReferenceField(Group, dbref=True))

To migrate to using strings instead of DBRefs you will have to manually
migrate ::

        # Step 1 - Migrate the model definition
        class Group(Document):
            author = ReferenceField(User, dbref=False)
            members = ListField(ReferenceField(User, dbref=False))

        # Step 2 - Migrate the data
        for g in Group.objects():
            g.author = g.author
            g.members = g.members
            g.save()


item_frequencies
----------------

In the 0.6 series we added support for null / zero / false values in
item_frequencies.  A side effect was to return keys in the value they are
stored in rather than as string representations.  Your code may need to be
updated to handle native types rather than strings keys for the results of
item frequency queries.

BinaryFields
------------

Binary fields have been updated so that they are native binary types.  If you
previously were doing `str` comparisons with binary field values you will have
to update and wrap the value in a `str`.

0.5 to 0.6
==========

Embedded Documents - if you had a `pk` field you will have to rename it from
`_id` to `pk` as pk is no longer a property of Embedded Documents.

Reverse Delete Rules in Embedded Documents, MapFields and DictFields now throw
an InvalidDocument error as they aren't currently supported.

Document._get_subclasses - Is no longer used and the class method has been
removed.

Document.objects.with_id - now raises an InvalidQueryError if used with a
filter.

FutureWarning - A future warning has been added to all inherited classes that
don't define `allow_inheritance` in their meta.

You may need to update pyMongo to 2.0 for use with Sharding.

0.4 to 0.5
===========

There have been the following backwards incompatibilities from 0.4 to 0.5.  The
main areas of changed are: choices in fields, map_reduce and collection names.

Choice options:
---------------

Are now expected to be an iterable of tuples, with  the first element in each
tuple being the actual value to be stored. The second element is the
human-readable name for the option.


PyMongo / MongoDB
-----------------

map reduce now requires pymongo 1.11+- The pymongo `merge_output` and
`reduce_output` parameters, have been depreciated.

More methods now use map_reduce as db.eval is not supported for sharding as
such the following have been changed:

    * :meth:`~mongoengine.queryset.QuerySet.sum`
    * :meth:`~mongoengine.queryset.QuerySet.average`
    * :meth:`~mongoengine.queryset.QuerySet.item_frequencies`


Default collection naming
-------------------------

Previously it was just lowercase, its now much more pythonic and readable as
its lowercase and underscores, previously ::

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

    MyAceDocument._get_collection_name() == "myacedocument"

Alternatively, you can rename your collections eg ::

    from mongoengine.connection import _get_db
    from mongoengine.base import _document_registry

    def rename_collections():
        db = _get_db()

        failure = False

        collection_names = [d._get_collection_name()
                            for d in _document_registry.values()]

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
                    print "Renamed:  %s to %s" % (old_style_name,
                                                  new_style_name)

        if failure:
            print "Upgrading  collection names failed"
        else:
            print "Upgraded collection names"

