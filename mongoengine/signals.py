# -*- coding: utf-8 -*-

from __future__ import absolute_import
__all__ = ['pre_init', 'post_init', 'pre_save', 'pre_save_post_validation',
           'post_save', 'pre_delete', 'post_delete']

from blinker import Namespace
signals_available = True

# the namespace for code signals.  If you are not mongoengine code, do
# not put signals in here.  Create your own namespace instead.
_signals = Namespace()

pre_init = _signals.signal('pre_init')
post_init = _signals.signal('post_init')
pre_save = _signals.signal('pre_save')
pre_save_post_validation = _signals.signal('pre_save_post_validation')
pre_post_save_signal = _signals.signal('pre_post_save_signal')
post_save = _signals.signal('post_save')
pre_delete = _signals.signal('pre_delete')
post_delete = _signals.signal('post_delete')
pre_bulk_insert = _signals.signal('pre_bulk_insert')
post_bulk_insert = _signals.signal('post_bulk_insert')
