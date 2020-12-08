import pytest

from mongoengine import Document, EmailField, ValidationError
from tests.utils import MongoDBTestCase


class TestEmailField(MongoDBTestCase):
    def test_generic_behavior(self):
        class User(Document):
            email = EmailField()

        user = User(email="ross@example.com")
        user.validate()

        user = User(email="ross@example.co.uk")
        user.validate()

        user = User(
            email=("Kofq@rhom0e4klgauOhpbpNdogawnyIKvQS0wk2mjqrgGQ5SaJIazqqWkm7.net")
        )
        user.validate()

        user = User(email="new-tld@example.technology")
        user.validate()

        user = User(email="ross@example.com.")
        with pytest.raises(ValidationError):
            user.validate()

        # unicode domain
        user = User(email="user@пример.рф")
        user.validate()

        # invalid unicode domain
        user = User(email="user@пример")
        with pytest.raises(ValidationError):
            user.validate()

        # invalid data type
        user = User(email=123)
        with pytest.raises(ValidationError):
            user.validate()

    def test_email_field_unicode_user(self):
        class User(Document):
            email = EmailField()

        # unicode user shouldn't validate by default...
        user = User(email="Dörte@Sörensen.example.com")
        with pytest.raises(ValidationError):
            user.validate()

        # ...but it should be fine with allow_utf8_user set to True
        class User(Document):
            email = EmailField(allow_utf8_user=True)

        user = User(email="Dörte@Sörensen.example.com")
        user.validate()

    def test_email_field_domain_whitelist(self):
        class User(Document):
            email = EmailField()

        # localhost domain shouldn't validate by default...
        user = User(email="me@localhost")
        with pytest.raises(ValidationError):
            user.validate()

        # ...but it should be fine if it's whitelisted
        class User(Document):
            email = EmailField(domain_whitelist=["localhost"])

        user = User(email="me@localhost")
        user.validate()

    def test_email_domain_validation_fails_if_invalid_idn(self):
        class User(Document):
            email = EmailField()

        invalid_idn = ".google.com"
        user = User(email="me@%s" % invalid_idn)

        with pytest.raises(ValidationError) as exc_info:
            user.validate()
        assert "domain failed IDN encoding" in str(exc_info.value)

    def test_email_field_ip_domain(self):
        class User(Document):
            email = EmailField()

        valid_ipv4 = "email@[127.0.0.1]"
        valid_ipv6 = "email@[2001:dB8::1]"
        invalid_ip = "email@[324.0.0.1]"

        # IP address as a domain shouldn't validate by default...
        user = User(email=valid_ipv4)
        with pytest.raises(ValidationError):
            user.validate()

        user = User(email=valid_ipv6)
        with pytest.raises(ValidationError):
            user.validate()

        user = User(email=invalid_ip)
        with pytest.raises(ValidationError):
            user.validate()

        # ...but it should be fine with allow_ip_domain set to True
        class User(Document):
            email = EmailField(allow_ip_domain=True)

        user = User(email=valid_ipv4)
        user.validate()

        user = User(email=valid_ipv6)
        user.validate()

        # invalid IP should still fail validation
        user = User(email=invalid_ip)
        with pytest.raises(ValidationError):
            user.validate()

    def test_email_field_honors_regex(self):
        class User(Document):
            email = EmailField(regex=r"\w+@example.com")

        # Fails regex validation
        user = User(email="me@foo.com")
        with pytest.raises(ValidationError):
            user.validate()

        # Passes regex validation
        user = User(email="me@example.com")
        assert user.validate() is None
