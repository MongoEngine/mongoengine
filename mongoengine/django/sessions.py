from django.contrib.sessions.backends.base import SessionBase, CreateError
from django.core.exceptions import SuspiciousOperation
from django.utils.encoding import force_unicode

from mongoengine.document import Document
from mongoengine import fields
from mongoengine.queryset import OperationError
from mongoengine.connection import DEFAULT_CONNECTION_NAME
from django.conf import settings
from datetime import datetime

MONGOENGINE_SESSION_DB_ALIAS = getattr(
    settings, 'MONGOENGINE_SESSION_DB_ALIAS',
    DEFAULT_CONNECTION_NAME)

class MongoSession(Document):
    session_key = fields.StringField(primary_key=True, max_length=40)
    session_data = fields.StringField()
    expire_date = fields.DateTimeField()
    
    meta = {'collection': 'django_session',
            'db_alias': MONGOENGINE_SESSION_DB_ALIAS,
            'allow_inheritance': False}


class SessionStore(SessionBase):
    """A MongoEngine-based session store for Django.
    """

    def load(self):
        try:
            s = MongoSession.objects(session_key=self.session_key,
                                     expire_date__gt=datetime.now())[0]
            return self.decode(force_unicode(s.session_data))
        except (IndexError, SuspiciousOperation):
            self.create()
            return {}

    def exists(self, session_key):
        return bool(MongoSession.objects(session_key=session_key).first())

    def create(self):
        while True:
            self.session_key = self._get_new_session_key()
            try:
                self.save(must_create=True)
            except CreateError:
                continue
            self.modified = True
            self._session_cache = {}
            return

    def save(self, must_create=False):
        s = MongoSession(session_key=self.session_key)
        s.session_data = self.encode(self._get_session(no_load=must_create))
        s.expire_date = self.get_expiry_date()
        try:
            s.save(force_insert=must_create, safe=True)
        except OperationError:
            if must_create:
                raise CreateError
            raise

    def delete(self, session_key=None):
        if session_key is None:
            if self.session_key is None:
                return
            session_key = self.session_key
        MongoSession.objects(session_key=session_key).delete()
