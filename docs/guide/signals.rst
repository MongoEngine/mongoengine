.. _signals:

Signals
=======

.. versionadded:: 0.5

Signal support is provided by the excellent `blinker`_ library and
will gracefully fall back if it is not available.


The following document signals exist in MongoEngine and are pretty self explanatory:

  * `mongoengine.signals.pre_init`
  * `mongoengine.signals.post_init`
  * `mongoengine.signals.pre_save`
  * `mongoengine.signals.post_save`
  * `mongoengine.signals.pre_delete`
  * `mongoengine.signals.post_delete`
  * `mongoengine.signals.pre_bulk_insert`
  * `mongoengine.signals.post_bulk_insert`

Example usage::

    from mongoengine import *
    from mongoengine import signals

    class Author(Document):
        name = StringField()

        def __unicode__(self):
            return self.name

        @classmethod
        def pre_save(cls, sender, document, **kwargs):
            logging.debug("Pre Save: %s" % document.name)

        @classmethod
        def post_save(cls, sender, document, **kwargs):
            logging.debug("Post Save: %s" % document.name)
            if 'created' in kwargs:
                if kwargs['created']:
                    logging.debug("Created")
                else:
                    logging.debug("Updated")

    signals.pre_save.connect(Author.pre_save, sender=Author)
    signals.post_save.connect(Author.post_save, sender=Author)


.. _blinker: http://pypi.python.org/pypi/blinker
