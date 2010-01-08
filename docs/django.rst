=============================
Using MongoEngine with Django
=============================

Connecting
==========
In your **settings.py** file, ignore the standard database settings (unless you
also plan to use the ORM in your project), and instead call 
:func:`~mongoengine.connect` somewhere in the settings module.

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
