===========
MongoEngine
===========
:Info: MongoEngine is an ORM-like layer on top of PyMongo.
:Repository: https://github.com/MongoEngine/mongoengine
:Author: Harry Marr (http://github.com/hmarr)
:Maintainer: Ross Lawley (http://github.com/rozza)

.. image:: https://secure.travis-ci.org/MongoEngine/mongoengine.png?branch=master
  :target: http://travis-ci.org/MongoEngine/mongoengine

About
=====
MongoEngine is a Python Object-Document Mapper for working with MongoDB.
Documentation available at http://mongoengine-odm.rtfd.org - there is currently
a `tutorial <http://readthedocs.org/docs/mongoengine-odm/en/latest/tutorial.html>`_, a `user guide
<http://readthedocs.org/docs/mongoengine-odm/en/latest/userguide.html>`_ and an `API reference
<http://readthedocs.org/docs/mongoengine-odm/en/latest/apireference.html>`_.

Installation
============
If you have `setuptools <http://peak.telecommunity.com/DevCenter/setuptools>`_
you can use ``easy_install -U mongoengine``. Otherwise, you can download the
source from `GitHub <http://github.com/MongoEngine/mongoengine>`_ and run ``python
setup.py install``.

Dependencies
============
- pymongo 2.1.1+
- sphinx (optional - for documentation generation)

Examples
========
Some simple examples of what MongoEngine code looks like::

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
    >>> len(HtmlPost.objects)
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
the standard port, and run: ``python setup.py test``.

Community
=========
- `MongoEngine Users mailing list
  <http://groups.google.com/group/mongoengine-users>`_
- `MongoEngine Developers mailing list
  <http://groups.google.com/group/mongoengine-dev>`_
- `#mongoengine IRC channel <http://webchat.freenode.net/?channels=mongoengine>`_

Contributing
============
We welcome contributions! see  the`Contribution guidelines <https://github.com/MongoEngine/mongoengine/blob/master/CONTRIBUTING.rst>`_
