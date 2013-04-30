==============
Django Support
==============

.. note:: Updated to support Django 1.5

Connecting
==========
In your **settings.py** file, ignore the standard database settings (unless you
also plan to use the ORM in your project), and instead call
:func:`~mongoengine.connect` somewhere in the settings module.

.. note::
   If you are not using another Database backend you may need to add a dummy
   database backend to ``settings.py`` eg::

        DATABASES = {
            'default': {
                'ENGINE': 'django.db.backends.dummy'
            }
        }

Authentication
==============
MongoEngine includes a Django authentication backend, which uses MongoDB. The
:class:`~mongoengine.django.auth.User` model is a MongoEngine
:class:`~mongoengine.Document`, but implements most of the methods and
attributes that the standard Django :class:`User` model does - so the two are
moderately compatible. Using this backend will allow you to store users in
MongoDB but still use many of the Django authentication infrastucture (such as
the :func:`login_required` decorator and the :func:`authenticate` function). To
enable the MongoEngine auth backend, add the following to you **settings.py**
file::

    AUTHENTICATION_BACKENDS = (
        'mongoengine.django.auth.MongoEngineBackend',
    )

The :mod:`~mongoengine.django.auth` module also contains a
:func:`~mongoengine.django.auth.get_user` helper function, that takes a user's
:attr:`id` and returns a :class:`~mongoengine.django.auth.User` object.

.. versionadded:: 0.1.3

Custom User model
=================
Django 1.5 introduced `Custom user Models
<https://docs.djangoproject.com/en/dev/topics/auth/customizing/#auth-custom-user>`
which can be used as an alternative the Mongoengine authentication backend.

The main advantage of this option is that other components relying on
:mod:`django.contrib.auth` and supporting the new swappable user model are more
likely to work. For example, you can use the ``createsuperuser`` management
command as usual.

To enable the custom User model in Django, add ``'mongoengine.django.mongo_auth'``
in your ``INSTALLED_APPS`` and set ``'mongo_auth.MongoUser'`` as the custom user
user model to use. In your **settings.py** file you will have::

    INSTALLED_APPS = (
        ...
        'django.contrib.auth',
        'mongoengine.django.mongo_auth',
        ...
    )

    AUTH_USER_MODEL = 'mongo_auth.MongoUser'

An additional ``MONGOENGINE_USER_DOCUMENT`` setting enables you to replace the
:class:`~mongoengine.django.auth.User` class with another class of your choice::

    MONGOENGINE_USER_DOCUMENT = 'mongoengine.django.auth.User'

The custom :class:`User` must be a :class:`~mongoengine.Document` class, but
otherwise has the same requirements as a standard custom user model,
as specified in the `Django Documentation
<https://docs.djangoproject.com/en/dev/topics/auth/customizing/>`.
In particular, the custom class must define :attr:`USERNAME_FIELD` and
:attr:`REQUIRED_FIELDS` attributes.

Sessions
========
Django allows the use of different backend stores for its sessions. MongoEngine
provides a MongoDB-based session backend for Django, which allows you to use
sessions in you Django application with just MongoDB. To enable the MongoEngine
session backend, ensure that your settings module has
``'django.contrib.sessions.middleware.SessionMiddleware'`` in the
``MIDDLEWARE_CLASSES`` field  and ``'django.contrib.sessions'`` in your
``INSTALLED_APPS``. From there, all you need to do is add the following line
into you settings module::

    SESSION_ENGINE = 'mongoengine.django.sessions'

Django provides session cookie, which expires after ```SESSION_COOKIE_AGE``` seconds, but doesnt delete cookie at sessions backend, so ``'mongoengine.django.sessions'`` supports  `mongodb TTL
<http://docs.mongodb.org/manual/tutorial/expire-data/>`_.

.. versionadded:: 0.2.1

Storage
=======
With MongoEngine's support for GridFS via the :class:`~mongoengine.fields.FileField`,
it is useful to have a Django file storage backend that wraps this. The new
storage module is called :class:`~mongoengine.django.storage.GridFSStorage`.
Using it is very similar to using the default FileSystemStorage.::

    from mongoengine.django.storage import GridFSStorage
    fs = GridFSStorage()

    filename = fs.save('hello.txt', 'Hello, World!')

All of the `Django Storage API methods
<http://docs.djangoproject.com/en/dev/ref/files/storage/>`_ have been
implemented except :func:`path`. If the filename provided already exists, an
underscore and a number (before # the file extension, if one exists) will be
appended to the filename until the generated filename doesn't exist. The
:func:`save` method will return the new filename.::

    >>> fs.exists('hello.txt')
    True
    >>> fs.open('hello.txt').read()
    'Hello, World!'
    >>> fs.size('hello.txt')
    13
    >>> fs.url('hello.txt')
    'http://your_media_url/hello.txt'
    >>> fs.open('hello.txt').name
    'hello.txt'
    >>> fs.listdir()
    ([], [u'hello.txt'])

All files will be saved and retrieved in GridFS via the :class::`FileDocument`
document, allowing easy access to the files without the GridFSStorage
backend.::

    >>> from mongoengine.django.storage import FileDocument
    >>> FileDocument.objects()
    [<FileDocument: FileDocument object>]

.. versionadded:: 0.4
