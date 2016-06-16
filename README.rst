===========
MongoEngine
===========
:Info: MongoEngine is an ORM-like layer on top of PyMongo.
:Repository: https://github.com/MongoEngine/mongoengine
:Author: Harry Marr (http://github.com/hmarr)
:Maintainer: Ross Lawley (http://github.com/rozza)

.. image:: https://secure.travis-ci.org/MongoEngine/mongoengine.png?branch=master
  :target: http://travis-ci.org/MongoEngine/mongoengine

.. image:: https://coveralls.io/repos/MongoEngine/mongoengine/badge.png?branch=master
  :target: https://coveralls.io/r/MongoEngine/mongoengine?branch=master

.. image:: https://landscape.io/github/MongoEngine/mongoengine/master/landscape.png
   :target: https://landscape.io/github/MongoEngine/mongoengine/master
   :alt: Code Health

About
=====
MongoEngine is a Python Object-Document Mapper for working with MongoDB.
Documentation available at https://mongoengine-odm.readthedocs.io - there is currently
a `tutorial <https://mongoengine-odm.readthedocs.io/tutorial.html>`_, a `user guide
<https://mongoengine-odm.readthedocs.io/guide/index.html>`_ and an `API reference
<https://mongoengine-odm.readthedocs.io/apireference.html>`_.

Installation
============
We recommend the use of `virtualenv <https://virtualenv.pypa.io/>`_ and of
`pip <https://pip.pypa.io/>`_. You can then use ``pip install -U mongoengine``.
You may also have `setuptools <http://peak.telecommunity.com/DevCenter/setuptools>`_ and thus
you can use ``easy_install -U mongoengine``. Otherwise, you can download the
source from `GitHub <http://github.com/MongoEngine/mongoengine>`_ and run ``python
setup.py install``.

Dependencies
============
- pymongo>=2.7.1
- sphinx (optional - for documentation generation)

Optional Dependencies
---------------------
- **Image Fields**: Pillow>=2.0.0
- dateutil>=2.1.0

.. note
   MongoEngine always runs it's test suite against the latest patch version of each dependecy. e.g.: PyMongo 3.0.1

Examples
========
Some simple examples of what MongoEngine code looks like:

.. code :: python

    class BlogPost(Document):
        title = StringField(required=True, max_length=200)
        posted = DateTimeField(default=datetime.datetime.now)
        tags = ListField(StringField(max_length=50))

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
    ...     print '===', post.title, '==='
    ...     if isinstance(post, TextPost):
    ...         print post.content
    ...     elif isinstance(post, LinkPost):
    ...         print 'Link:', post.url
    ...     print
    ...

    >>> len(BlogPost.objects)
    2
    >>> len(TextPost.objects)
    1
    >>> len(LinkPost.objects)
    1

    # Find tagged posts
    >>> len(BlogPost.objects(tags='mongoengine'))
    2
    >>> len(BlogPost.objects(tags='mongodb'))
    1

Tests
=====
To run the test suite, ensure you are running a local instance of MongoDB on
the standard port, and run: ``python setup.py nosetests``.

To run the test suite on every supported Python version and every supported PyMongo version,
you can use ``tox``.
tox and each supported Python version should be installed in your environment:

.. code-block:: shell

    # Install tox
    $ pip install tox
    # Run the test suites
    $ tox

If you wish to run one single or selected tests, use the nosetest convention. It will find the folder,
eventually the file, go to the TestClass specified after the colon and eventually right to the single test.
Also use the -s argument if you want to print out whatever or access pdb while testing.

.. code-block:: shell

    $ python setup.py nosetests --tests tests/fields/fields.py:FieldTest.test_cls_field -s

Community
=========
- `MongoEngine Users mailing list
  <http://groups.google.com/group/mongoengine-users>`_
- `MongoEngine Developers mailing list
  <http://groups.google.com/group/mongoengine-dev>`_
- `#mongoengine IRC channel <http://webchat.freenode.net/?channels=mongoengine>`_

Contributing
============
We welcome contributions! see  the `Contribution guidelines <https://github.com/MongoEngine/mongoengine/blob/master/CONTRIBUTING.rst>`_
