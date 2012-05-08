import datetime

from mongoengine import *

from django.utils.encoding import smart_str
from django.contrib.auth.models import AnonymousUser
from django.utils.translation import ugettext_lazy as _

try:
    from django.contrib.auth.hashers import check_password, make_password
except ImportError:
    """Handle older versions of Django"""
    from django.utils.hashcompat import md5_constructor, sha_constructor

    def get_hexdigest(algorithm, salt, raw_password):
        raw_password, salt = smart_str(raw_password), smart_str(salt)
        if algorithm == 'md5':
            return md5_constructor(salt + raw_password).hexdigest()
        elif algorithm == 'sha1':
            return sha_constructor(salt + raw_password).hexdigest()
        raise ValueError('Got unknown password algorithm type in password')

    def check_password(raw_password, password):
        algo, salt, hash = password.split('$')
        return hash == get_hexdigest(algo, salt, raw_password)

    def make_password(raw_password):
        from random import random
        algo = 'sha1'
        salt = get_hexdigest(algo, str(random()), str(random()))[:5]
        hash = get_hexdigest(algo, salt, raw_password)
        return '%s$%s$%s' % (algo, salt, hash)


REDIRECT_FIELD_NAME = 'next'

class User(Document):
    """A User document that aims to mirror most of the API specified by Django
    at http://docs.djangoproject.com/en/dev/topics/auth/#users
    """
    username = StringField(max_length=30, required=True,
                           verbose_name=_('username'),
                           help_text=_("Required. 30 characters or fewer. Letters, numbers and @/./+/-/_ characters"))

    first_name = StringField(max_length=30,
                             verbose_name=_('first name'))

    last_name = StringField(max_length=30,
                            verbose_name=_('last name'))
    email = EmailField(verbose_name=_('e-mail address'))
    password = StringField(max_length=128,
                           verbose_name=_('password'),
                           help_text=_("Use '[algo]$[iterations]$[salt]$[hexdigest]' or use the <a href=\"password/\">change password form</a>."))
    is_staff = BooleanField(default=False,
                            verbose_name=_('staff status'),
                            help_text=_("Designates whether the user can log into this admin site."))
    is_active = BooleanField(default=True,
                             verbose_name=_('active'),
                             help_text=_("Designates whether this user should be treated as active. Unselect this instead of deleting accounts."))
    is_superuser = BooleanField(default=False,
                                verbose_name=_('superuser status'),
                                help_text=_("Designates that this user has all permissions without explicitly assigning them."))
    last_login = DateTimeField(default=datetime.datetime.now,
                               verbose_name=_('last login'))
    date_joined = DateTimeField(default=datetime.datetime.now,
                                verbose_name=_('date joined'))

    meta = {
        'allow_inheritance': True,
        'indexes': [
            {'fields': ['username'], 'unique': True}
        ]
    }

    def __unicode__(self):
        return self.username

    def get_full_name(self):
        """Returns the users first and last names, separated by a space.
        """
        full_name = u'%s %s' % (self.first_name or '', self.last_name or '')
        return full_name.strip()

    def is_anonymous(self):
        return False

    def is_authenticated(self):
        return True

    def set_password(self, raw_password):
        """Sets the user's password - always use this rather than directly
        assigning to :attr:`~mongoengine.django.auth.User.password` as the
        password is hashed before storage.
        """
        self.password = make_password(raw_password)
        self.save()
        return self

    def check_password(self, raw_password):
        """Checks the user's password against a provided password - always use
        this rather than directly comparing to
        :attr:`~mongoengine.django.auth.User.password` as the password is
        hashed before storage.
        """
        return check_password(raw_password, self.password)

    @classmethod
    def create_user(cls, username, password, email=None):
        """Create (and save) a new user with the given username, password and
        email address.
        """
        now = datetime.datetime.now()

        # Normalize the address by lowercasing the domain part of the email
        # address.
        if email is not None:
            try:
                email_name, domain_part = email.strip().split('@', 1)
            except ValueError:
                pass
            else:
                email = '@'.join([email_name, domain_part.lower()])

        user = cls(username=username, email=email, date_joined=now)
        user.set_password(password)
        user.save()
        return user

    def get_and_delete_messages(self):
        return []


class MongoEngineBackend(object):
    """Authenticate using MongoEngine and mongoengine.django.auth.User.
    """

    supports_object_permissions = False
    supports_anonymous_user = False
    supports_inactive_user = False

    def authenticate(self, username=None, password=None):
        user = User.objects(username=username).first()
        if user:
            if password and user.check_password(password):
                return user
        return None

    def get_user(self, user_id):
        return User.objects.with_id(user_id)


def get_user(userid):
    """Returns a User object from an id (User.id). Django's equivalent takes
    request, but taking an id instead leaves it up to the developer to store
    the id in any way they want (session, signed cookie, etc.)
    """
    if not userid:
        return AnonymousUser()
    return MongoEngineBackend().get_user(userid) or AnonymousUser()
