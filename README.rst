===========
MongoEngine
===========

:Info: MongoEngine is an Object-Document Mapper (ODM) for MongoDB.
:Repository: https://github.com/MongoEngine/mongoengine
:Author: Harry Marr (http://github.com/hmarr)
:Maintainer: Bastien Gerard (http://github.com/bagerard)

.. image:: https://github.com/MongoEngine/mongoengine/actions/workflows/github-actions.yml/badge.svg?branch=master
   :target: https://github.com/MongoEngine/mongoengine/actions

.. image:: https://coveralls.io/repos/github/MongoEngine/mongoengine/badge.svg?branch=master
   :target: https://coveralls.io/github/MongoEngine/mongoengine?branch=master

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
   :target: https://github.com/psf/black

.. image:: https://pepy.tech/badge/mongoengine/month
   :target: https://pepy.tech/project/mongoengine

.. image:: https://img.shields.io/pypi/v/mongoengine.svg
   :target: https://pypi.python.org/pypi/mongoengine

.. image:: https://readthedocs.org/projects/mongoengine-odm/badge/?version=latest
   :target: https://mongoengine-odm.readthedocs.io/


About
=====

MongoEngine is a Python Object-Document Mapper (ODM) that provides a high-level,
Pythonic API for working with MongoDB. It builds on top of PyMongo and offers
schema enforcement, validation, inheritance, and both synchronous and
asynchronous query APIs.

Documentation is available at:
https://mongoengine-odm.readthedocs.io

Including:

- Tutorial
- User Guide
- API Reference


Supported MongoDB Versions
==========================

MongoEngine is tested against the following MongoDB versions:

- MongoDB 4.4
- MongoDB 5.0
- MongoDB 6.0
- MongoDB 7.0
- MongoDB 8.0

Newer MongoDB versions are expected to work. Please report issues if encountered.


Installation
============

We recommend using ``virtualenv`` and ``pip``:

.. code-block:: shell

    python -m pip install -U mongoengine

Alternatively:

.. code-block:: shell

    pip install mongoengine

Python 3.8+ is required. Python 2 support was dropped in MongoEngine 0.20.0.


Dependencies
============

Core dependency:

- pymongo >= 4.14

Optional dependencies:

- python-dateutil (for DateTimeField parsing)
- Pillow (for ImageField / GridFS)
- blinker (for signals)


Synchronous Usage
=================

A simple synchronous example:

.. code-block:: python

    import datetime
    from mongoengine import (
        connect,
        Document,
        StringField,
        DateTimeField,
        ListField,
    )

    connect("mydb")

    class BlogPost(Document):
        title = StringField(required=True, max_length=200)
        posted = DateTimeField(default=datetime.datetime.utcnow)
        tags = ListField(StringField(max_length=50))

    post = BlogPost(
        title="Using MongoEngine",
        tags=["mongodb", "mongoengine"],
    )
    post.save()

    count = BlogPost.objects(tags="mongoengine").count()
    print(count)


Async Usage
===========

MongoEngine provides a **fully supported asyncio-native API**.
The async API mirrors the synchronous API and uses ``.aobjects`` along with
``await`` for all I/O operations.

Async support is **first-class** and designed for modern Python applications.

.. code-block:: python

    import asyncio
    from mongoengine import (
        Document,
        StringField,
        async_connect,
    )

    async_connect("mydb")

    class User(Document):
        name = StringField(required=True)

    async def main():
        # Create
        alice = await User.aobjects.create(name="Alice")

        # Query
        first = await User.aobjects.first()
        assert first == alice

        # Update
        await User.aobjects(name="Alice").update(set__name="Alicia")

        # Delete
        await User.aobjects(name="Alicia").delete()

    asyncio.run(main())



Tests
=====

To run the test suite locally:

.. code-block:: shell

    pytest tests/

To run against all supported Python and MongoDB versions:

.. code-block:: shell

    python -m pip install tox
    tox


Community
=========

- MongoEngine Users mailing list:
  http://groups.google.com/group/mongoengine-users
- MongoEngine Developers mailing list:
  http://groups.google.com/group/mongoengine-dev


Contributing
============

Contributions are welcome!

Please see:
https://github.com/MongoEngine/mongoengine/blob/master/CONTRIBUTING.rst
