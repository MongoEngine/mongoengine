# -*- coding: utf-8 -*-
import sys
sys.path[0:0] = [""]
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
            def pre_save_post_validation(cls, sender, document, **kwargs):
                signal_output.append('pre_save_post_validation signal, %s' % document)
                if 'created' in kwargs:
                    if kwargs['created']:
                        signal_output.append('Is created')
                    else:
                        signal_output.append('Is updated')

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

            @classmethod
            def pre_bulk_insert(cls, sender, documents, **kwargs):
                signal_output.append('pre_bulk_insert signal, %s' % documents)

            @classmethod
            def post_bulk_insert(cls, sender, documents, **kwargs):
                signal_output.append('post_bulk_insert signal, %s' % documents)
                if kwargs.get('loaded', False):
                    signal_output.append('Is loaded')
                else:
                    signal_output.append('Not loaded')
        self.Author = Author
        Author.drop_collection()

        class Another(Document):

            name = StringField()

            def __unicode__(self):
                return self.name

            @classmethod
            def pre_delete(cls, sender, document, **kwargs):
                signal_output.append('pre_delete signal, %s' % document)

            @classmethod
            def post_delete(cls, sender, document, **kwargs):
                signal_output.append('post_delete signal, %s' % document)

        self.Another = Another
        Another.drop_collection()

        class ExplicitId(Document):
            id = IntField(primary_key=True)

            @classmethod
            def post_save(cls, sender, document, **kwargs):
                if 'created' in kwargs:
                    if kwargs['created']:
                        signal_output.append('Is created')
                    else:
                        signal_output.append('Is updated')

        self.ExplicitId = ExplicitId
        ExplicitId.drop_collection()

        # Save up the number of connected signals so that we can check at the
        # end that all the signals we register get properly unregistered
        self.pre_signals = (
            len(signals.pre_init.receivers),
            len(signals.post_init.receivers),
            len(signals.pre_save.receivers),
            len(signals.pre_save_post_validation.receivers),
            len(signals.post_save.receivers),
            len(signals.pre_delete.receivers),
            len(signals.post_delete.receivers),
            len(signals.pre_bulk_insert.receivers),
            len(signals.post_bulk_insert.receivers),
        )

        signals.pre_init.connect(Author.pre_init, sender=Author)
        signals.post_init.connect(Author.post_init, sender=Author)
        signals.pre_save.connect(Author.pre_save, sender=Author)
        signals.pre_save_post_validation.connect(Author.pre_save_post_validation, sender=Author)
        signals.post_save.connect(Author.post_save, sender=Author)
        signals.pre_delete.connect(Author.pre_delete, sender=Author)
        signals.post_delete.connect(Author.post_delete, sender=Author)
        signals.pre_bulk_insert.connect(Author.pre_bulk_insert, sender=Author)
        signals.post_bulk_insert.connect(Author.post_bulk_insert, sender=Author)

        signals.pre_delete.connect(Another.pre_delete, sender=Another)
        signals.post_delete.connect(Another.post_delete, sender=Another)

        signals.post_save.connect(ExplicitId.post_save, sender=ExplicitId)

    def tearDown(self):
        signals.pre_init.disconnect(self.Author.pre_init)
        signals.post_init.disconnect(self.Author.post_init)
        signals.post_delete.disconnect(self.Author.post_delete)
        signals.pre_delete.disconnect(self.Author.pre_delete)
        signals.post_save.disconnect(self.Author.post_save)
        signals.pre_save_post_validation.disconnect(self.Author.pre_save_post_validation)
        signals.pre_save.disconnect(self.Author.pre_save)
        signals.pre_bulk_insert.disconnect(self.Author.pre_bulk_insert)
        signals.post_bulk_insert.disconnect(self.Author.post_bulk_insert)

        signals.post_delete.disconnect(self.Another.post_delete)
        signals.pre_delete.disconnect(self.Another.pre_delete)

        signals.post_save.disconnect(self.ExplicitId.post_save)

        # Check that all our signals got disconnected properly.
        post_signals = (
            len(signals.pre_init.receivers),
            len(signals.post_init.receivers),
            len(signals.pre_save.receivers),
            len(signals.pre_save_post_validation.receivers),
            len(signals.post_save.receivers),
            len(signals.pre_delete.receivers),
            len(signals.post_delete.receivers),
            len(signals.pre_bulk_insert.receivers),
            len(signals.post_bulk_insert.receivers),
        )

        self.ExplicitId.objects.delete()

        self.assertEqual(self.pre_signals, post_signals)

    def test_model_signals(self):
        """ Model saves should throw some signals. """

        def create_author():
            self.Author(name='Bill Shakespeare')

        def bulk_create_author_with_load():
            a1 = self.Author(name='Bill Shakespeare')
            self.Author.objects.insert([a1], load_bulk=True)

        def bulk_create_author_without_load():
            a1 = self.Author(name='Bill Shakespeare')
            self.Author.objects.insert([a1], load_bulk=False)

        self.assertEqual(self.get_signal_output(create_author), [
            "pre_init signal, Author",
            "{'name': 'Bill Shakespeare'}",
            "post_init signal, Bill Shakespeare",
        ])

        a1 = self.Author(name='Bill Shakespeare')
        self.assertEqual(self.get_signal_output(a1.save), [
            "pre_save signal, Bill Shakespeare",
            "pre_save_post_validation signal, Bill Shakespeare",
            "Is created",
            "post_save signal, Bill Shakespeare",
            "Is created"
        ])

        a1.reload()
        a1.name = 'William Shakespeare'
        self.assertEqual(self.get_signal_output(a1.save), [
            "pre_save signal, William Shakespeare",
            "pre_save_post_validation signal, William Shakespeare",
            "Is updated",
            "post_save signal, William Shakespeare",
            "Is updated"
        ])

        self.assertEqual(self.get_signal_output(a1.delete), [
            'pre_delete signal, William Shakespeare',
            'post_delete signal, William Shakespeare',
        ])

        signal_output = self.get_signal_output(bulk_create_author_with_load)

        # The output of this signal is not entirely deterministic. The reloaded
        # object will have an object ID. Hence, we only check part of the output
        self.assertEqual(signal_output[3],
            "pre_bulk_insert signal, [<Author: Bill Shakespeare>]")
        self.assertEqual(signal_output[-2:],
            ["post_bulk_insert signal, [<Author: Bill Shakespeare>]",
             "Is loaded",])

        self.assertEqual(self.get_signal_output(bulk_create_author_without_load), [
            "pre_init signal, Author",
            "{'name': 'Bill Shakespeare'}",
            "post_init signal, Bill Shakespeare",
            "pre_bulk_insert signal, [<Author: Bill Shakespeare>]",
            "post_bulk_insert signal, [<Author: Bill Shakespeare>]",
            "Not loaded",
        ])

    def test_queryset_delete_signals(self):
        """ Queryset delete should throw some signals. """

        self.Another(name='Bill Shakespeare').save()
        self.assertEqual(self.get_signal_output(self.Another.objects.delete), [
            'pre_delete signal, Bill Shakespeare',
            'post_delete signal, Bill Shakespeare',
        ])

    def test_signals_with_explicit_doc_ids(self):
        """ Model saves must have a created flag the first time."""
        ei = self.ExplicitId(id=123)
        # post save must received the created flag, even if there's already
        # an object id present
        self.assertEqual(self.get_signal_output(ei.save), ['Is created'])
        # second time, it must be an update
        self.assertEqual(self.get_signal_output(ei.save), ['Is updated'])

if __name__ == '__main__':
    unittest.main()
