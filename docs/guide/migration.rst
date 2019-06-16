==============================
Handling migration
==============================

The structure of your documents and their associated mongoengine schemas are likely
to change over the lifetime of an application. This section provides guidance and
recommendations on how to deal with migrations.

Due to the very flexible nature of mongodb, migrations of models aren't trivial and
for people that know about `alembic` for `sqlalchemy`, there is unfortunately no equivalent
library that will manage the migration in an automatic fashion for mongoengine.

First of all, let's take a simple example of model change and review the different option you
have to deal with the migration.

Let's assume we start with the following schema and save an instance:

.. code-block:: python

    class User(Document):
        name = StringField()

    User(name=username).save()

    # print the objects as they exist in mongodb
    print(User.objects().as_pymongo())    # [{u'_id': ObjectId('5d06b9c3d7c1f18db3e7c874'), u'name': u'John'}]

On the next version of your application, let's now assume that a new field `enabled` gets added to the
existing User model with a `default=True`. Thus you simply update the `User` class to the following:

.. code-block:: python

    class User(Document):
        name = StringField(required=True)
        enabled = BooleaField(default=True)

Without migration, we now reload an object from the database into the `User` class and checks its `enabled`
attribute:

.. code-block:: python

    assert User.objects.count() == 1
    user = User.objects().first()
    assert user.enabled is True
    print(User.objects(enabled=True).count())    # 0 ! uh?
    print(User.objects(enabled=False).count())   # 0 ! uh?

    # but this is consistent with what we have in database
    print(User.objects().as_pymongo().first())    # {u'_id': ObjectId('5d06b9c3d7c1f18db3e7c874'), u'name': u'John'}
    assert User.objects(enabled=None).count() == 1

As you can see, even if the document wasn't updated, mongoengine applies the default value seemlessly when it
loads the pymongo dict into a `User` instance. At first sight it looks like you don't need to migrate the
existing documents when adding new fields but this actually leads to inconsistencies when it comes to querying.

In fact, when querying, mongoengine isn't trying to account for the default value of the new field and so
if you don't actually migrate the existing documents, you are taking a risk that querying/updating
will be missing relevant record.

When adding fields/modifying default values, you can use any of the following to do the migration
as a standalone script:

.. code-block:: python

    User.objects().update(enabled=True)
    # or
    user_coll = User._get_collection()
    user_coll.update_many({}, {'$set': {'enabled': True}})


