===========
MongoEngine
===========
:Info: MongoEngine is an ORM-like layer on top of PyMongo.
:Repository: https://github.com/MongoEngine/mongoengine
:Author: Harry Marr (http://github.com/hmarr)
:Maintainer: Bastien Gerard (http://github.com/bagerard)

.. image:: https://travis-ci.org/MongoEngine/mongoengine.svg?branch=master
  :target: https://travis-ci.org/MongoEngine/mongoengine

.. image:: https://coveralls.io/repos/github/MongoEngine/mongoengine/badge.svg?branch=master
  :target: https://coveralls.io/github/MongoEngine/mongoengine?branch=master

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
  :target: https://github.com/ambv/black

.. image:: https://pepy.tech/badge/mongoengine/month
  :target: https://pepy.tech/project/mongoengine

.. image:: https://img.shields.io/pypi/v/mongoengine.svg
  :target: https://pypi.python.org/pypi/mongoengine


.. image:: https://readthedocs.org/projects/mongoengine-odm/badge/?version=latest
  :target: https://readthedocs.org/projects/mongoengine-odm/builds/

About
=====
MongoEngine is a Python Object-Document Mapper for working with MongoDB.
Documentation is available at https://mongoengine-odm.readthedocs.io - there
is currently a `tutorial <https://mongoengine-odm.readthedocs.io/tutorial.html>`_,
a `user guide <https://mongoengine-odm.readthedocs.io/guide/index.html>`_, and
an `API reference <https://mongoengine-odm.readthedocs.io/apireference.html>`_.

Supported MongoDB Versions
==========================
MongoEngine is currently tested against MongoDB v3.6, v4.0, v4.4, v5.0, v6.0, v7.0 and v8.0. Future versions
should be supported as well, but aren't actively tested at the moment. Make
sure to open an issue or submit a pull request if you experience any problems
with a more recent MongoDB versions.

Installation
============
We recommend the use of `virtualenv <https://virtualenv.pypa.io/>`_ and of
`pip <https://pip.pypa.io/>`_. You can then use ``python -m pip install -U mongoengine``.
You may also have `setuptools <http://peak.telecommunity.com/DevCenter/setuptools>`_
and thus you can use ``easy_install -U mongoengine``. Another option is
`pipenv <https://docs.pipenv.org/>`_. You can then use ``pipenv install mongoengine``
to both create the virtual environment and install the package. Otherwise, you can
download the source from `GitHub <https://github.com/MongoEngine/mongoengine>`_ and
run ``python setup.py install``.

The support for Python2 was dropped with MongoEngine 0.20.0

Dependencies
============
All of the dependencies can easily be installed via `python -m pip <https://pip.pypa.io/>`_.
At the very least, you'll need these two packages to use MongoEngine:

- pymongo>=3.12

If you utilize a ``DateTimeField``, you might also use a more flexible date parser:

- dateutil>=2.1.0

If you need to use an ``ImageField`` or ``ImageGridFsProxy``:

- Pillow>=7.0.0

If you need to use signals:

- blinker>=1.3

Examples
========
Some simple examples of what MongoEngine code looks like:

.. code :: python
    import datetime
    from mongoengine import *

    connect('mydb')

    class BlogPost(Document):
        title = StringField(required=True, max_length=200)
        posted = DateTimeField(default=lambda: datetime.datetime.now(datetime.timezone.utc))
        tags = ListField(StringField(max_length=50))
        meta = {'allow_inheritance': True}

    class TextPost(BlogPost):
        content = StringField(required=True)

    class LinkPost(BlogPost):
        url = StringField(required=True)

    # Create a text-based post
    >>> post1 = TextPost(title='Using MongoEngine', content='See the tutorial')
    >>> post1.tags = ['mongodb', 'mongoengine']
    >>> post1.save()

    # Create a link-based post
    >>> post2 = LinkPost(title='MongoEngine Docs', url='hmarr.com/mongoengine')
    >>> post2.tags = ['mongoengine', 'documentation']
    >>> post2.save()

    # Iterate over all posts using the BlogPost superclass
    >>> for post in BlogPost.objects:
    ...     print('===', post.title, '===')
    ...     if isinstance(post, TextPost):
    ...         print(post.content)
    ...     elif isinstance(post, LinkPost):
    ...         print('Link:', post.url)
    ...

    # Count all blog posts and its subtypes
    >>> BlogPost.objects.count()
    2
    >>> TextPost.objects.count()
    1
    >>> LinkPost.objects.count()
    1

    # Count tagged posts
    >>> BlogPost.objects(tags='mongoengine').count()
    2
    >>> BlogPost.objects(tags='mongodb').count()
    1

Async Support (Experimental)
============================
MongoEngine now supports asynchronous operations using PyMongo's AsyncMongoClient.
This allows you to use async/await syntax for database operations:

.. code :: python

    import datetime
    import asyncio
    from mongoengine import *

    async def main():
        # Connect asynchronously
        await connect_async('mydb')

        # All document operations have async equivalents
        post = TextPost(title='Async Post', content='Async content')
        await post.async_save()

        # Async queries
        post = await TextPost.objects.async_get(title='Async Post')
        await post.async_delete()

        # Async reload
        await post.async_reload()

    # Run the async function
    asyncio.run(main())

Note: Async support is experimental and currently includes basic CRUD operations.
QuerySet async methods and advanced features are still under development.

Tests
=====
To run the test suite, ensure you are running a local instance of MongoDB on
the standard port and have ``pytest`` installed. Then, run ``pytest tests/``.

To run the test suite on every supported Python and PyMongo version, you can
use ``tox``. You'll need to make sure you have each supported Python version
installed in your environment and then:

.. code-block:: shell

    # Install tox
    $ python -m pip install tox
    # Run the test suites
    $ tox

Community
=========
- `MongoEngine Users mailing list
  <http://groups.google.com/group/mongoengine-users>`_
- `MongoEngine Developers mailing list
  <http://groups.google.com/group/mongoengine-dev>`_

Contributing
============
We welcome contributions! See the `Contribution guidelines <https://github.com/MongoEngine/mongoengine/blob/master/CONTRIBUTING.rst>`_
