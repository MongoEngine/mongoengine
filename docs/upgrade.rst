#########
Upgrading
#########

Development
***********
(Fill this out whenever you introduce breaking changes to MongoEngine)

URLField's constructor no longer takes `verify_exists`

0.15.0
******

0.14.0
******
This release includes a few bug fixes and a significant code cleanup. The most
important change is that `QuerySet.as_pymongo` no longer supports a
`coerce_types` mode. If you used it in the past, a) please let us know of your
use case, b) you'll need to override `as_pymongo` to get the desired outcome.

This release also makes the EmbeddedDocument not hashable by default. If you
use embedded documents in sets or dictionaries, you might have to override
`__hash__` and implement a hashing logic specific to your use case. See #1528
for the reason behind this change.

0.13.0
******
This release adds Unicode support to the `EmailField` and changes its
structure significantly. Previously, email addresses containing Unicode
characters didn't work at all. Starting with v0.13.0, domains with Unicode
characters are supported out of the box, meaning some emails that previously
didn't pass validation now do. Make sure the rest of your application can
accept such email addresses. Additionally, if you subclassed the `EmailField`
in your application and overrode `EmailField.EMAIL_REGEX`, you will have to
adjust your code to override `EmailField.USER_REGEX`, `EmailField.DOMAIN_REGEX`,
and potentially `EmailField.UTF8_USER_REGEX`.

0.12.0
******
This release includes various fixes for the `BaseQuerySet` methods and how they
are chained together. Since version 0.10.1 applying limit/skip/hint/batch_size
to an already-existing queryset wouldn't modify the underlying PyMongo cursor.
This has been fixed now, so you'll need to make sure that your code didn't rely
on the broken implementation.

Additionally, a public `BaseQuerySet.clone_into` has been renamed to a private
`_clone_into`. If you directly used that method in your code, you'll need to
rename its occurrences.

0.11.0
******
This release includes a major rehaul of MongoEngine's code quality and
introduces a few breaking changes. It also touches many different parts of
the package and although all the changes have been tested and scrutinized,
you're encouraged to thorougly test the upgrade.

First breaking change involves renaming `ConnectionError` to `MongoEngineConnectionError`.
If you import or catch this exception, you'll need to rename it in your code.

Second breaking change drops Python v2.6 support. If you run MongoEngine on
that Python version, you'll need to upgrade it first.

Third breaking change drops an old backward compatibility measure where
`from mongoengine.base import ErrorClass` would work on top of
`from mongoengine.errors import ErrorClass` (where `ErrorClass` is e.g.
`ValidationError`). If you import any exceptions from `mongoengine.base`,
change it to `mongoengine.errors`.

0.10.8
******
This version fixed an issue where specifying a MongoDB URI host would override
more information than it should. These changes are minor, but they still
subtly modify the connection logic and thus you're encouraged to test your
MongoDB connection before shipping v0.10.8 in production.

0.10.7
******

`QuerySet.aggregate_sum` and `QuerySet.aggregate_average` are dropped. Use
`QuerySet.sum` and `QuerySet.average` instead which use the aggreation framework
by default from now on.

0.9.0
*****

The 0.8.7 package on pypi was corrupted.  If upgrading from 0.8.7 to 0.9.0 please follow: ::

    pip uninstall pymongo
    pip uninstall mongoengine
    pip install pymongo==2.8
    pip install mongoengine

0.8.7
*****

Calling reload on deleted / nonexistent documents now raises a DoesNotExist
exception.


0.8.2 to 0.8.3
**************

Minor change that may impact users:

DynamicDocument fields are now stored in creation order after any declared
fields.  Previously they were stored alphabetically.


0.7 to 0.8
**********

There have been numerous backwards breaking changes in 0.8.  The reasons for
these are to ensure that MongoEngine has sane defaults going forward and that it
performs the best it can out of the box.  Where possible there have been
FutureWarnings to help get you ready for the change, but that hasn't been
possible for the whole of the release.

.. warning:: Breaking changes - test upgrading on a test system before putting
    live. There maybe multiple manual steps in migrating and these are best honed
    on a staging / test system.

Python and PyMongo
==================

MongoEngine requires python 2.6 (or above) and pymongo 2.5 (or above)

Data Model
==========

Inheritance
-----------

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
-------------------

The default for inheritance has changed - it is now off by default and
:attr:`_cls` will not be stored automatically with the class.  So if you extend
your :class:`~mongoengine.Document` or :class:`~mongoengine.EmbeddedDocuments`
you will need to declare :attr:`allow_inheritance` in the meta data like so: ::

    class Animal(Document):
        name = StringField()

        meta = {'allow_inheritance': True}

Previously, if you had data in the database that wasn't defined in the Document
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

The Document class has introduced a reserved function `clean()`, which will be
called before saving the document. If your document class happens to have a method
with the same name, please try to rename it.

    def clean(self):
        pass

ReferenceField
--------------

ReferenceFields now store ObjectIds by default - this is more efficient than
DBRefs as we already know what Document types they reference::

    # Old code
    class Animal(Document):
        name = ReferenceField('self')

    # New code to keep dbrefs
    class Animal(Document):
        name = ReferenceField('self', dbref=True)

To migrate all the references you need to touch each object and mark it as dirty
eg::

    # Doc definition
    class Person(Document):
        name = StringField()
        parent = ReferenceField('self')
        friends = ListField(ReferenceField('self'))

    # Mark all ReferenceFields as dirty and save
    for p in Person.objects:
        p._mark_as_changed('parent')
        p._mark_as_changed('friends')
        p.save()

`An example test migration for ReferenceFields is available on github
<https://github.com/MongoEngine/mongoengine/blob/master/tests/migration/refrencefield_dbref_to_object_id.py>`_.

.. Note:: Internally mongoengine handles ReferenceFields the same, so they are
   converted to DBRef on loading and ObjectIds or DBRefs depending on settings
   on storage.

UUIDField
---------

UUIDFields now default to storing binary values::

    # Old code
    class Animal(Document):
        uuid = UUIDField()

    # New code
    class Animal(Document):
        uuid = UUIDField(binary=False)

To migrate all the uuids you need to touch each object and mark it as dirty
eg::

    # Doc definition
    class Animal(Document):
        uuid = UUIDField()

    # Mark all UUIDFields as dirty and save
    for a in Animal.objects:
        a._mark_as_changed('uuid')
        a.save()

`An example test migration for UUIDFields is available on github
<https://github.com/MongoEngine/mongoengine/blob/master/tests/migration/uuidfield_to_binary.py>`_.

DecimalField
------------

DecimalFields now store floats - previously it was storing strings and that
made it impossible to do comparisons when querying correctly.::

    # Old code
    class Person(Document):
        balance = DecimalField()

    # New code
    class Person(Document):
        balance = DecimalField(force_string=True)

To migrate all the DecimalFields you need to touch each object and mark it as dirty
eg::

    # Doc definition
    class Person(Document):
        balance = DecimalField()

    # Mark all DecimalField's as dirty and save
    for p in Person.objects:
        p._mark_as_changed('balance')
        p.save()

.. note:: DecimalFields have also been improved with the addition of precision
    and rounding.  See :class:`~mongoengine.fields.DecimalField` for more information.

`An example test migration for DecimalFields is available on github
<https://github.com/MongoEngine/mongoengine/blob/master/tests/migration/decimalfield_as_float.py>`_.

Cascading Saves
---------------
To improve performance document saves will no longer automatically cascade.
Any changes to a Document's references will either have to be saved manually or
you will have to explicitly tell it to cascade on save::

    # At the class level:
    class Person(Document):
        meta = {'cascade': True}

    # Or on save:
    my_document.save(cascade=True)

Storage
-------

Document and Embedded Documents are now serialized based on declared field order.
Previously, the data was passed to mongodb as a dictionary and which meant that
order wasn't guaranteed - so things like ``$addToSet`` operations on
:class:`~mongoengine.EmbeddedDocument` could potentially fail in unexpected
ways.

If this impacts you, you may want to rewrite the objects using the
``doc.mark_as_dirty('field')`` pattern described above.  If you are using a
compound primary key then you will need to ensure the order is fixed and match
your EmbeddedDocument to that order.

Querysets
=========

Attack of the clones
--------------------

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
    carnivores = mammals.filter(order="Carnivora") # Reassign the new queryset so filter can be applied
    [m for m in carnivores]                        # This will return all carnivores

    # Update example b) chain the queryset:
    mammals = Animal.objects(type="mammal").filter(order="Carnivora")  # The final queryset is assgined to mammals
    [m for m in mammals]                                               # This will return all carnivores

Len iterates the queryset
-------------------------

If you ever did `len(queryset)` it previously did a `count()` under the covers,
this caused some unusual issues.  As `len(queryset)` is most often used by
`list(queryset)` we now cache the queryset results and use that for the length.

This isn't as performant as a `count()` and if you aren't iterating the
queryset you should upgrade to use count::

    # Old code
    len(Animal.objects(type="mammal"))

    # New code
    Animal.objects(type="mammal").count()


.only() now inline with .exclude()
----------------------------------

The behaviour of `.only()` was highly ambiguous, now it works in mirror fashion
to `.exclude()`.  Chaining `.only()` calls will increase the fields required::

    # Old code
    Animal.objects().only(['type', 'name']).only('name', 'order')  # Would have returned just `name`

    # New code
    Animal.objects().only('name')

    # Note:
    Animal.objects().only(['name']).only('order')  # Now returns `name` *and* `order`


Client
======
PyMongo 2.4 came with a new connection client; MongoClient_ and started the
depreciation of the old :class:`~pymongo.connection.Connection`. MongoEngine
now uses the latest `MongoClient` for connections.  By default operations were
`safe` but if you turned them off or used the connection directly this will
impact your queries.

Querysets
---------

Safe
^^^^

`safe` has been depreciated in the new MongoClient connection.  Please use
`write_concern` instead.  As `safe` always defaulted as `True` normally no code
change is required. To disable confirmation of the write just pass `{"w": 0}`
eg: ::

   # Old
   Animal(name="Dinasour").save(safe=False)

   # new code:
   Animal(name="Dinasour").save(write_concern={"w": 0})

Write Concern
^^^^^^^^^^^^^

`write_options` has been replaced with `write_concern` to bring it inline with
pymongo. To upgrade simply rename any instances where you used the `write_option`
keyword  to `write_concern` like so::

   # Old code:
   Animal(name="Dinasour").save(write_options={"w": 2})

   # new code:
   Animal(name="Dinasour").save(write_concern={"w": 2})


Indexes
=======

Index methods are no longer tied to querysets but rather to the document class.
Although `QuerySet._ensure_indexes` and `QuerySet.ensure_index` still exist.
They should be replaced with :func:`~mongoengine.Document.ensure_indexes` /
:func:`~mongoengine.Document.ensure_index`.

SequenceFields
==============

:class:`~mongoengine.fields.SequenceField` now inherits from `BaseField` to
allow flexible storage of the calculated value.  As such MIN and MAX settings
are no longer handled.

.. _MongoClient: http://blog.mongodb.org/post/36666163412/introducing-mongoclient

0.6 to 0.7
**********

Cascade saves
=============

Saves will raise a `FutureWarning` if they cascade and cascade hasn't been set
to True.  This is because in 0.8 it will default to False.  If you require
cascading saves then either set it in the `meta` or pass
via `save` eg ::

    # At the class level:
    class Person(Document):
        meta = {'cascade': True}

    # Or in code:
    my_document.save(cascade=True)

.. note::
    Remember: cascading saves **do not** cascade through lists.

ReferenceFields
===============

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
================

In the 0.6 series we added support for null / zero / false values in
item_frequencies.  A side effect was to return keys in the value they are
stored in rather than as string representations.  Your code may need to be
updated to handle native types rather than strings keys for the results of
item frequency queries.

BinaryFields
============

Binary fields have been updated so that they are native binary types.  If you
previously were doing `str` comparisons with binary field values you will have
to update and wrap the value in a `str`.

0.5 to 0.6
**********

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
**********

There have been the following backwards incompatibilities from 0.4 to 0.5.  The
main areas of changed are: choices in fields, map_reduce and collection names.

Choice options:
===============

Are now expected to be an iterable of tuples, with the first element in each
tuple being the actual value to be stored. The second element is the
human-readable name for the option.


PyMongo / MongoDB
=================

map reduce now requires pymongo 1.11+- The pymongo `merge_output` and
`reduce_output` parameters, have been depreciated.

More methods now use map_reduce as db.eval is not supported for sharding as
such the following have been changed:

    * :meth:`~mongoengine.queryset.QuerySet.sum`
    * :meth:`~mongoengine.queryset.QuerySet.average`
    * :meth:`~mongoengine.queryset.QuerySet.item_frequencies`


Default collection naming
=========================

Previously it was just lowercase, it's now much more pythonic and readable as
it's lowercase and underscores, previously ::

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

It's been reported that indexes may need to be recreated to the newer version of indexes.
To do this drop indexes and call ``ensure_indexes`` on each model.
