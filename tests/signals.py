# -*- coding: utf-8 -*-
import unittest

from mongoengine import *
from mongoengine import signals

signal_output = []


class SignalTests(unittest.TestCase):
    """
    Testing signals before/after saving and deleting.
    """

    def get_signal_output(self, fn, *args, **kwargs):
        # Flush any existing signal output
        global signal_output
        signal_output = []
        fn(*args, **kwargs)
        return signal_output

    def setUp(self):
        connect(db='mongoenginetest')
        class Author(Document):
            name = StringField()

            def __unicode__(self):
                return self.name

            @classmethod
            def pre_init(cls, sender, document, *args, **kwargs):
                signal_output.append('pre_init signal, %s' % cls.__name__)
                signal_output.append(str(kwargs['values']))

            @classmethod
            def post_init(cls, sender, document, **kwargs):
                signal_output.append('post_init signal, %s' % document)

            @classmethod
            def pre_save(cls, sender, document, **kwargs):
                signal_output.append('pre_save signal, %s' % document)

            @classmethod
            def post_save(cls, sender, document, **kwargs):
                signal_output.append('post_save signal, %s' % document)
                if 'created' in kwargs:
                    if kwargs['created']:
                        signal_output.append('Is created')
                    else:
                        signal_output.append('Is updated')

            @classmethod
            def pre_delete(cls, sender, document, **kwargs):
                signal_output.append('pre_delete signal, %s' % document)

            @classmethod
            def post_delete(cls, sender, document, **kwargs):
                signal_output.append('post_delete signal, %s' % document)
        self.Author = Author


        class Another(Document):
            name = StringField()

            def __unicode__(self):
                return self.name

            @classmethod
            def pre_init(cls, sender, document, **kwargs):
                signal_output.append('pre_init Another signal, %s' % cls.__name__)
                signal_output.append(str(kwargs['values']))

            @classmethod
            def post_init(cls, sender, document, **kwargs):
                signal_output.append('post_init Another signal, %s' % document)

            @classmethod
            def pre_save(cls, sender, document, **kwargs):
                signal_output.append('pre_save Another signal, %s' % document)

            @classmethod
            def post_save(cls, sender, document, **kwargs):
                signal_output.append('post_save Another signal, %s' % document)
                if 'created' in kwargs:
                    if kwargs['created']:
                        signal_output.append('Is created')
                    else:
                        signal_output.append('Is updated')

            @classmethod
            def pre_delete(cls, sender, document, **kwargs):
                signal_output.append('pre_delete Another signal, %s' % document)

            @classmethod
            def post_delete(cls, sender, document, **kwargs):
                signal_output.append('post_delete Another signal, %s' % document)

        self.Another = Another
        # Save up the number of connected signals so that we can check at the end
        # that all the signals we register get properly unregistered
        self.pre_signals = (
            len(signals.pre_init.receivers),
            len(signals.post_init.receivers),
            len(signals.pre_save.receivers),
            len(signals.post_save.receivers),
            len(signals.pre_delete.receivers),
            len(signals.post_delete.receivers)
        )

        signals.pre_init.connect(Author.pre_init, sender=Author)
        signals.post_init.connect(Author.post_init, sender=Author)
        signals.pre_save.connect(Author.pre_save, sender=Author)
        signals.post_save.connect(Author.post_save, sender=Author)
        signals.pre_delete.connect(Author.pre_delete, sender=Author)
        signals.post_delete.connect(Author.post_delete, sender=Author)

        signals.pre_init.connect(Another.pre_init, sender=Another)
        signals.post_init.connect(Another.post_init, sender=Another)
        signals.pre_save.connect(Another.pre_save, sender=Another)
        signals.post_save.connect(Another.post_save, sender=Another)
        signals.pre_delete.connect(Another.pre_delete, sender=Another)
        signals.post_delete.connect(Another.post_delete, sender=Another)

    def tearDown(self):
        signals.pre_init.disconnect(self.Author.pre_init)
        signals.post_init.disconnect(self.Author.post_init)
        signals.post_delete.disconnect(self.Author.post_delete)
        signals.pre_delete.disconnect(self.Author.pre_delete)
        signals.post_save.disconnect(self.Author.post_save)
        signals.pre_save.disconnect(self.Author.pre_save)

        signals.pre_init.disconnect(self.Another.pre_init)
        signals.post_init.disconnect(self.Another.post_init)
        signals.post_delete.disconnect(self.Another.post_delete)
        signals.pre_delete.disconnect(self.Another.pre_delete)
        signals.post_save.disconnect(self.Another.post_save)
        signals.pre_save.disconnect(self.Another.pre_save)

        # Check that all our signals got disconnected properly.
        post_signals = (
            len(signals.pre_init.receivers),
            len(signals.post_init.receivers),
            len(signals.pre_save.receivers),
            len(signals.post_save.receivers),
            len(signals.pre_delete.receivers),
            len(signals.post_delete.receivers)
        )

        self.assertEqual(self.pre_signals, post_signals)

    def test_model_signals(self):
        """ Model saves should throw some signals. """

        def create_author():
            a1 = self.Author(name='Bill Shakespeare')

        self.assertEqual(self.get_signal_output(create_author), [
            "pre_init signal, Author",
            "{'name': 'Bill Shakespeare'}",
            "post_init signal, Bill Shakespeare",
        ])

        a1 = self.Author(name='Bill Shakespeare')
        self.assertEqual(self.get_signal_output(a1.save), [
            "pre_save signal, Bill Shakespeare",
            "post_save signal, Bill Shakespeare",
            "Is created"
        ])

        a1.reload()
        a1.name='William Shakespeare'
        self.assertEqual(self.get_signal_output(a1.save), [
            "pre_save signal, William Shakespeare",
            "post_save signal, William Shakespeare",
            "Is updated"
        ])

        self.assertEqual(self.get_signal_output(a1.delete), [
            'pre_delete signal, William Shakespeare',
            'post_delete signal, William Shakespeare',
        ])