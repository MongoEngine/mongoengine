===========
MongoEngine
===========
:Info: MongoEngine is an ORM-like layer on top of PyMongo.
:Author: Harry Marr (http://github.com/hmarr)

About
=====
MongoEngine is a Python Object-Document Mapper for working with MongoDB. 
Documentation available at http://hmarr.com/mongoengine/ - there is currently 
a `tutorial <http://hmarr.com/mongoengine/tutorial.html>`_, a `user guide 
<http://hmarr.com/mongoengine/userguide.html>`_ and an `API reference
<http://hmarr.com/mongoengine/apireference.html>`_.

Dependencies
============
- pymongo 1.1+
- sphinx (optional - for documentation generation)

Examples
========
::
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
    === Using MongoEngine ===
    See the tutorial

    === MongoEngine Docs ===
    Link: hmarr.com/mongoengine

    >>> BlogPost.objects.count()
    2
    >>> HtmlPost.objects.count()
    1
    >>> LinkPost.objects.count()
    1

    # Find tagged posts
    >>> BlogPost.objects(tags='mongoengine').count()
    2
    >>> BlogPost.objects(tags='mongodb').count()
    1
