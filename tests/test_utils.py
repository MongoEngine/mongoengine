import re
from datetime import datetime, timezone

import pytest

from mongoengine.base.utils import LazyRegexCompiler
from tests.utils import utcnow_naive

signal_output = []


def test_utcnow_naive__called__returns_current_naive_utc_datetime():
    before = datetime.now(timezone.utc).replace(tzinfo=None)
    result = utcnow_naive()
    after = datetime.now(timezone.utc).replace(tzinfo=None)

    assert result.tzinfo is None
    assert before <= result <= after


class TestLazyRegexCompiler:
    def test_lazy_regex_compiler_verify_laziness_of_descriptor(self):
        class UserEmail:
            EMAIL_REGEX = LazyRegexCompiler("@", flags=32)

        descriptor = UserEmail.__dict__["EMAIL_REGEX"]
        assert descriptor._compiled_regex is None

        regex = UserEmail.EMAIL_REGEX
        assert regex == re.compile("@", flags=32)
        assert regex.search("user@domain.com").group() == "@"

        user_email = UserEmail()
        assert user_email.EMAIL_REGEX is UserEmail.EMAIL_REGEX

    def test_lazy_regex_compiler_verify_cannot_set_descriptor_on_instance(self):
        class UserEmail:
            EMAIL_REGEX = LazyRegexCompiler("@")

        user_email = UserEmail()
        with pytest.raises(AttributeError):
            user_email.EMAIL_REGEX = re.compile("@")

    def test_lazy_regex_compiler_verify_can_override_class_attr(self):
        class UserEmail:
            EMAIL_REGEX = LazyRegexCompiler("@")

        UserEmail.EMAIL_REGEX = re.compile("cookies")
        assert UserEmail.EMAIL_REGEX.search("Cake & cookies").group() == "cookies"
