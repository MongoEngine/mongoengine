===========
MongoEngine
===========
:Info: MongoEngine is an ORM-like layer on top of PyMongo.
:Repository: https://github.com/MongoEngine/mongoengine
:Author: Harry Marr (https://github.com/hmarr)
:Maintainer: Bastien Gerard (https://github.com/bagerard)

.. image:: https://github.com/MongoEngine/mongoengine/actions/workflows/github-actions.yml/badge.svg?branch=master
  :target: https://github.com/MongoEngine/mongoengine/actions

.. image:: https://coveralls.io/repos/github/MongoEngine/mongoengine/badge.svg?branch=master
  :target: https://coveralls.io/github/MongoEngine/mongoengine?branch=master

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
  :target: https://github.com/psf/black

.. image:: https://pepy.tech/badge/mongoengine/month
  :target: https://pepy.tech/project/mongoengine

.. image:: https://img.shields.io/pypi/v/mongoengine.svg
  :target: https://pypi.org/project/mongoengine/


.. image:: https://readthedocs.org/projects/mongoengine-odm/badge/?version=latest
  :target: https://mongoengine-odm.readthedocs.io/


⚠️ **Warning:** ``mongoengine.org`` is no longer controlled by the MongoEngine
project and appears to be an expired domain takeover. The official MongoEngine
project is maintained on `GitHub <https://github.com/MongoEngine/mongoengine>`_
and documented on
`Read the Docs <https://mongoengine-odm.readthedocs.io/>`_.

About
=====
MongoEngine is a Python Object-Document Mapper for working with MongoDB.
Documentation is available at https://mongoengine-odm.readthedocs.io - there
is currently a `tutorial <https://mongoengine-odm.readthedocs.io/tutorial.html>`_,
a `user guide <https://mongoengine-odm.readthedocs.io/guide/index.html>`_, and
an `API reference <https://mongoengine-odm.readthedocs.io/apireference.html>`_.

Supported MongoDB Versions
==========================
MongoEngine is currently tested against MongoDB v4.4, v5.0, v6.0, v7.0 and
v8.0. Future versions should be supported as well, but aren't actively tested
at the moment. Make sure to open an issue or submit a pull request if you
experience any problems with more recent MongoDB versions.

Installation
============
MongoEngine requires Python 3.10 or newer. Install it from PyPI with:

.. code-block:: console

    $ python -m pip install -U mongoengine

To install a source checkout, run ``python -m pip install .`` from the
repository root.

Dependencies
============
MongoEngine requires:

- PyMongo >=3.12,<5.0

The following optional packages enable additional functionality:

If you utilize a ``DateTimeField``, you might also use a more flexible date parser:

- python-dateutil >=2.1.0

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
    >>> post2 = LinkPost(title='Example Docs', url='https://example.com/')
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

    # Count all blog posts and their subtypes
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

Tests
=====
To run the test suite, ensure MongoDB is running on the standard port, then
install the package with its test dependencies and run pytest:

.. code-block:: console

    $ python -m pip install -e ".[test]"
    $ pytest tests/

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
- Ask usage questions on `Stack Overflow
  <https://stackoverflow.com/questions/tagged/mongoengine>`_.
- Report confirmed bugs on `GitHub Issues
  <https://github.com/MongoEngine/mongoengine/issues>`_.

Contributing
============
We welcome contributions! See the `Contribution guidelines <https://github.com/MongoEngine/mongoengine/blob/master/CONTRIBUTING.rst>`_
