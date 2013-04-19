=========
Upgrading
=========

0.7 to 0.8
==========

Inheritance
-----------

Data Model
~~~~~~~~~~

The inheritance model has changed, we no longer need to store an array of
:attr:`types` with the model we can just use the classname in :attr:`_cls`.
This means that you will have to update your indexes for each of your
inherited classes like so: ::

    # 1. Declaration of the class
    class Animal(Document):
        name = StringField()
        meta = {
            'allow_inheritance': True,
            'indexes': ['name']
        }

    # 2. Remove _types
    collection = Animal._get_collection()
    collection.update({}, {"$unset": {"_types": 1}}, multi=True)

    # 3. Confirm extra data is removed
    count = collection.find({'_types': {"$exists": True}}).count()
    assert count == 0

    # 4. Remove indexes
    info = collection.index_information()
    indexes_to_drop = [key for key, value in info.iteritems()
                       if '_types' in dict(value['key'])]
    for index in indexes_to_drop:
        collection.drop_index(index)

    # 5. Recreate indexes
    Animal.ensure_indexes()


Document Definition
~~~~~~~~~~~~~~~~~~~

The default for inheritance has changed - its now off by default and
:attr:`_cls` will not be stored automatically with the class.  So if you extend
your :class:`~mongoengine.Document` or :class:`~mongoengine.EmbeddedDocuments`
you will need to declare :attr:`allow_inheritance` in the meta data like so: ::

    class Animal(Document):
        name = StringField()

        meta = {'allow_inheritance': True}

Previously, if you had data the database that wasn't defined in the Document
definition, it would set it as an attribute on the document.  This is no longer
the case and the data is set only in the ``document._data`` dictionary: ::

    >>> from mongoengine import *
    >>> class Animal(Document):
    ...    name = StringField()
    ...
    >>> cat = Animal(name="kit", size="small")

    # 0.7
    >>> cat.size
    u'small'

    # 0.8
    >>> cat.size
    Traceback (most recent call last):
      File "<stdin>", line 1, in <module>
    AttributeError: 'Animal' object has no attribute 'size'

Querysets
~~~~~~~~~

Querysets now return clones and should no longer be considered editable in
place.  This brings us in line with how Django's querysets work and removes a
long running gotcha.  If you edit your querysets inplace you will have to
update your code like so: ::

    # Old code:
    mammals = Animal.objects(type="mammal")
    mammals.filter(order="Carnivora")       # Returns a cloned queryset that isn't assigned to anything - so this will break in 0.8
    [m for m in mammals]                    # This will return all mammals in 0.8 as the 2nd filter returned a new queryset

    # Update example a) assign queryset after a change:
    mammals = Animal.objects(type="mammal")
    carnivores = mammals.filter(order="Carnivora") # Reassign the new queryset so fitler can be applied
    [m for m in carnivores]                        # This will return all carnivores

    # Update example b) chain the queryset:
    mammals = Animal.objects(type="mammal").filter(order="Carnivora")  # The final queryset is assgined to mammals
    [m for m in mammals]                                               # This will return all carnivores

Indexes
-------

Index methods are no longer tied to querysets but rather to the document class.
Although `QuerySet._ensure_indexes` and `QuerySet.ensure_index` still exist.
They should be replaced with :func:`~mongoengine.Document.ensure_indexes` /
:func:`~mongoengine.Document.ensure_index`.

SequenceFields
--------------

:class:`~mongoengine.fields.SequenceField` now inherits from `BaseField` to
allow flexible storage of the calculated value.  As such MIN and MAX settings
are no longer handled.

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
don't define :attr:`allow_inheritance` in their meta.

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


mongodb 1.8 > 2.0 +
===================

Its been reported that indexes may need to be recreated to the newer version of indexes.
To do this drop indexes and call ``ensure_indexes`` on each model.
