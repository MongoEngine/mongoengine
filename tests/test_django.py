from __future__ import with_statement
import unittest
from nose.plugins.skip import SkipTest
from mongoengine.python_support import PY3
from mongoengine import *

try:
    from mongoengine.django.shortcuts import get_document_or_404

    from django.http import Http404
    from django.template import Context, Template
    from django.conf import settings
    from django.core.paginator import Paginator

    settings.configure(USE_TZ=True)

    from django.contrib.sessions.tests import SessionTestsMixin
    from mongoengine.django.sessions import SessionStore, MongoSession
except Exception, err:
    if PY3:
        SessionTestsMixin = type  # dummy value so no error
        SessionStore = None  # dummy value so no error
    else:
        raise err


from datetime import tzinfo, timedelta
ZERO = timedelta(0)

class FixedOffset(tzinfo):
    """Fixed offset in minutes east from UTC."""

    def __init__(self, offset, name):
        self.__offset = timedelta(minutes = offset)
        self.__name = name

    def utcoffset(self, dt):
        return self.__offset

    def tzname(self, dt):
        return self.__name

    def dst(self, dt):
        return ZERO


def activate_timezone(tz):
    """Activate Django timezone support if it is available.
    """
    try:
        from django.utils import timezone
        timezone.deactivate()
        timezone.activate(tz)
    except ImportError:
        pass


class QuerySetTest(unittest.TestCase):

    def setUp(self):
        if PY3:
            raise SkipTest('django does not have Python 3 support')
        connect(db='mongoenginetest')

        class Person(Document):
            name = StringField()
            age = IntField()
        self.Person = Person

    def test_order_by_in_django_template(self):
        """Ensure that QuerySets are properly ordered in Django template.
        """
        self.Person.drop_collection()

        self.Person(name="A", age=20).save()
        self.Person(name="D", age=10).save()
        self.Person(name="B", age=40).save()
        self.Person(name="C", age=30).save()

        t = Template("{% for o in ol %}{{ o.name }}-{{ o.age }}:{% endfor %}")

        d = {"ol": self.Person.objects.order_by('-name')}
        self.assertEqual(t.render(Context(d)), u'D-10:C-30:B-40:A-20:')
        d = {"ol": self.Person.objects.order_by('+name')}
        self.assertEqual(t.render(Context(d)), u'A-20:B-40:C-30:D-10:')
        d = {"ol": self.Person.objects.order_by('-age')}
        self.assertEqual(t.render(Context(d)), u'B-40:C-30:A-20:D-10:')
        d = {"ol": self.Person.objects.order_by('+age')}
        self.assertEqual(t.render(Context(d)), u'D-10:A-20:C-30:B-40:')

        self.Person.drop_collection()

    def test_q_object_filter_in_template(self):

        self.Person.drop_collection()

        self.Person(name="A", age=20).save()
        self.Person(name="D", age=10).save()
        self.Person(name="B", age=40).save()
        self.Person(name="C", age=30).save()

        t = Template("{% for o in ol %}{{ o.name }}-{{ o.age }}:{% endfor %}")

        d = {"ol": self.Person.objects.filter(Q(age=10) | Q(name="C"))}
        self.assertEqual(t.render(Context(d)), 'D-10:C-30:')

        # Check double rendering doesn't throw an error
        self.assertEqual(t.render(Context(d)), 'D-10:C-30:')

    def test_get_document_or_404(self):
        p = self.Person(name="G404")
        p.save()

        self.assertRaises(Http404, get_document_or_404, self.Person, pk='1234')
        self.assertEqual(p, get_document_or_404(self.Person, pk=p.pk))

    def test_pagination(self):
        """Ensure that Pagination works as expected
        """
        class Page(Document):
            name = StringField()

        Page.drop_collection()

        for i in xrange(1, 11):
            Page(name=str(i)).save()

        paginator = Paginator(Page.objects.all(), 2)

        t = Template("{% for i in page.object_list  %}{{ i.name }}:{% endfor %}")
        for p in paginator.page_range:
            d = {"page": paginator.page(p)}
            end = p * 2
            start = end - 1
            self.assertEqual(t.render(Context(d)), u'%d:%d:' % (start, end))



class MongoDBSessionTest(SessionTestsMixin, unittest.TestCase):
    backend = SessionStore

    def setUp(self):
        if PY3:
            raise SkipTest('django does not have Python 3 support')
        connect(db='mongoenginetest')
        MongoSession.drop_collection()
        super(MongoDBSessionTest, self).setUp()

    def test_first_save(self):
        session = SessionStore()
        session['test'] = True
        session.save()
        self.assertTrue('test' in session)

    def test_session_expiration_tz(self):
        activate_timezone(FixedOffset(60, 'UTC+1'))
        # create and save new session
        session = SessionStore()
        session.set_expiry(600) # expire in 600 seconds
        session['test_expire'] = True
        session.save()
        # reload session with key
        key = session.session_key
        session = SessionStore(key)
        self.assertTrue('test_expire' in session, 'Session has expired before it is expected')
