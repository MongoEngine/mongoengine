import sys
sys.path[0:0] = [""]
import unittest
from nose.plugins.skip import SkipTest

from mongoengine import *
from mongoengine.django.shortcuts import get_document_or_404

import django
from django.http import Http404
from django.template import Context, Template
from django.conf import settings
from django.core.paginator import Paginator

settings.configure(
    USE_TZ=True,
    INSTALLED_APPS=('django.contrib.auth', 'mongoengine.django.mongo_auth'),
    AUTH_USER_MODEL=('mongo_auth.MongoUser'),
    AUTHENTICATION_BACKENDS = ('mongoengine.django.auth.MongoEngineBackend',)
)

try:
    # For Django >= 1.7
    if hasattr(django, 'setup'):
        django.setup()
except RuntimeError:
    pass

try:
    from django.contrib.auth import authenticate, get_user_model
    from mongoengine.django.auth import User, ContentType, Permission, Group
    from mongoengine.django.mongo_auth.models import (
        MongoUser,
        MongoUserManager,
        get_user_document,
    )
    DJ15 = True
except Exception:
    DJ15 = False
try:
    from django.test import modify_settings
    DJ17 = True
except Exception:
    DJ17 = False
from mongoengine.django.sessions import SessionStore, MongoSession
from mongoengine.django.tests import MongoTestCase
from datetime import tzinfo, timedelta
ZERO = timedelta(0)


class FixedOffset(tzinfo):
    """Fixed offset in minutes east from UTC."""

    def __init__(self, offset, name):
        self.__offset = timedelta(minutes=offset)
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

    def test_nested_queryset_template_iterator(self):
        # Try iterating the same queryset twice, nested, in a Django template.
        names = ['A', 'B', 'C', 'D']

        class CustomUser(Document):
            name = StringField()

            def __unicode__(self):
                return self.name

        CustomUser.drop_collection()

        for name in names:
            CustomUser(name=name).save()

        users = CustomUser.objects.all().order_by('name')
        template = Template("{% for user in users %}{{ user.name }}{% ifequal forloop.counter 2 %} {% for inner_user in users %}{{ inner_user.name }}{% endfor %} {% endifequal %}{% endfor %}")
        rendered = template.render(Context({'users': users}))
        self.assertEqual(rendered, 'AB ABCD CD')

    def test_filter(self):
        """Ensure that a queryset and filters work as expected
        """

        class LimitCountQuerySet(QuerySet):
            def count(self, with_limit_and_skip=True):
                return super(LimitCountQuerySet, self).count(with_limit_and_skip)

        class Note(Document):
            meta = dict(queryset_class=LimitCountQuerySet)
            name = StringField()

        Note.drop_collection()

        for i in xrange(1, 101):
            Note(name="Note: %s" % i).save()

        # Check the count
        self.assertEqual(Note.objects.count(), 100)

        # Get the first 10 and confirm
        notes = Note.objects[:10]
        self.assertEqual(notes.count(), 10)

        # Test djangos template filters
        # self.assertEqual(length(notes), 10)
        t = Template("{{ notes.count }}")
        c = Context({"notes": notes})
        self.assertEqual(t.render(c), "10")

        # Test with skip
        notes = Note.objects.skip(90)
        self.assertEqual(notes.count(), 10)

        # Test djangos template filters
        self.assertEqual(notes.count(), 10)
        t = Template("{{ notes.count }}")
        c = Context({"notes": notes})
        self.assertEqual(t.render(c), "10")

        # Test with limit
        notes = Note.objects.skip(90)
        self.assertEqual(notes.count(), 10)

        # Test djangos template filters
        self.assertEqual(notes.count(), 10)
        t = Template("{{ notes.count }}")
        c = Context({"notes": notes})
        self.assertEqual(t.render(c), "10")

        # Test with skip and limit
        notes = Note.objects.skip(10).limit(10)

        # Test djangos template filters
        self.assertEqual(notes.count(), 10)
        t = Template("{{ notes.count }}")
        c = Context({"notes": notes})
        self.assertEqual(t.render(c), "10")


class _BaseMongoDBSessionTest(unittest.TestCase):
    backend = SessionStore

    def setUp(self):
        connect(db='mongoenginetest')
        MongoSession.drop_collection()
        super(_BaseMongoDBSessionTest, self).setUp()

    def assertIn(self, first, second, msg=None):
        self.assertTrue(first in second, msg)

    def assertNotIn(self, first, second, msg=None):
        self.assertFalse(first in second, msg)

    def test_first_save(self):
        session = SessionStore()
        session['test'] = True
        session.save()
        self.assertTrue('test' in session)

    def test_session_expiration_tz(self):
        activate_timezone(FixedOffset(60, 'UTC+1'))
        # create and save new session
        session = SessionStore()
        session.set_expiry(600)  # expire in 600 seconds
        session['test_expire'] = True
        session.save()
        # reload session with key
        key = session.session_key
        session = SessionStore(key)
        self.assertTrue('test_expire' in session, 'Session has expired before it is expected')


try:
    # SessionTestsMixin isn't available for import on django > 1.8a1
    from django.contrib.sessions.tests import SessionTestsMixin

    class _MongoDBSessionTest(SessionTestsMixin):
        pass

    class MongoDBSessionTest(_BaseMongoDBSessionTest):
        pass

except ImportError:
    class MongoDBSessionTest(_BaseMongoDBSessionTest):
        pass


class MongoAuthTest(unittest.TestCase):
    user_data = {
        'username': 'user',
        'email': 'user@example.com',
        'password': 'test',
    }

    def setUp(self):
        if not DJ15:
            raise SkipTest('mongo_auth requires Django 1.5')
        connect(db='mongoenginetest')
        User.drop_collection()
        super(MongoAuthTest, self).setUp()

    def test_get_user_model(self):
        self.assertEqual(get_user_model(), MongoUser)

    def test_get_user_document(self):
        self.assertEqual(get_user_document(), User)

    def test_user_manager(self):
        manager = get_user_model()._default_manager
        self.assertTrue(isinstance(manager, MongoUserManager))

    def test_user_manager_exception(self):
        manager = get_user_model()._default_manager
        self.assertRaises(MongoUser.DoesNotExist, manager.get,
                          username='not found')

    def test_create_user(self):
        manager = get_user_model()._default_manager
        user = manager.create_user(**self.user_data)
        self.assertTrue(isinstance(user, User))
        db_user = User.objects.get(username='user')
        self.assertEqual(user.id, db_user.id)

    def test_authenticate(self):
        get_user_model()._default_manager.create_user(**self.user_data)
        user = authenticate(username='user', password='fail')
        self.assertEqual(None, user)
        user = authenticate(username='user', password='test')
        db_user = User.objects.get(username='user')
        self.assertEqual(user.id, db_user.id)


class MongoAuthBackendTest(MongoTestCase):
    user_data = {
        'username': 'user',
        'email': 'user@example.com',
        'password': 'test',
    }
    backend = 'mongoengine.django.auth.MongoEngineBackend'

    UserModel = MongoUser

    def create_users(self):
        self.user = MongoUser.objects.create_user(
            username='test',
            email='test@example.com',
            password='test',
        )
        self.superuser = MongoUser.objects.create_superuser(
            username='test2',
            email='test2@example.com',
            password='test',
        )

    def setUp(self):
        if not DJ17:
            raise SkipTest('mongo_auth backend tests require Django 1.7')
        self.patched_settings = modify_settings(
            AUTHENTICATION_BACKENDS={'append': self.backend},
        )
        self.patched_settings.enable()
        connect(db='mongoenginetest')
        User.drop_collection()
        ContentType.drop_collection()
        Permission.drop_collection()
        self.create_users()
        super(MongoAuthBackendTest, self).setUp()

    def tearDown(self):
        self.patched_settings.disable()
        ContentType.objects.clear_cache()

    def test_has_perm(self):
        user = self.UserModel._default_manager.get(pk=self.user.pk)
        self.assertEqual(user.has_perm('auth.test'), False)

        user.is_staff = True
        user.save()
        self.assertEqual(user.has_perm('auth.test'), False)

        user.is_superuser = True
        user.save()
        self.assertEqual(user.has_perm('auth.test'), True)

        user.is_staff = True
        user.is_superuser = True
        user.is_active = False
        user.save()
        self.assertEqual(user.has_perm('auth.test'), False)

    def test_custom_perms(self):
        user = self.UserModel._default_manager.get(pk=self.user.pk)
        content_type = ContentType(app_label='test', model='group').save()
        perm = Permission(name='test', content_type=content_type, codename='test').save()
        user.user_permissions.append(perm)
        user.save()

        # reloading user to purge the _perm_cache
        user = self.UserModel._default_manager.get(pk=self.user.pk)
        raise SkipTest("Permission logic not implemented on Mongo Backend")
        self.assertEqual(user.get_all_permissions() == set(['auth.test']), True)
        self.assertEqual(user.get_group_permissions(), set())
        self.assertEqual(user.has_module_perms('Group'), False)
        self.assertEqual(user.has_module_perms('auth'), True)
        self.assertEqual(user.has_perm('auth.test'), True)
        self.assertEqual(user.has_perms(['auth.test']), True)


class MongoTestCaseTest(MongoTestCase):
    def test_mongo_test_case(self):
        self.db.dummy_collection.insert({'collection': 'will be dropped'})


if __name__ == '__main__':
    unittest.main()
