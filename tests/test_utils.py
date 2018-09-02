import unittest
import re

from mongoengine.base.utils import LazyRegexCompiler

signal_output = []


class LazyRegexCompilerTest(unittest.TestCase):

    def test_lazy_regex_compiler_verify_laziness_of_descriptor(self):
        class UserEmail(object):
            EMAIL_REGEX = LazyRegexCompiler('@', flags=32)

        descriptor = UserEmail.__dict__['EMAIL_REGEX']
        self.assertIsNone(descriptor._compiled_regex)

        regex = UserEmail.EMAIL_REGEX
        self.assertEqual(regex, re.compile('@', flags=32))
        self.assertEqual(regex.search('user@domain.com').group(), '@')

        user_email = UserEmail()
        self.assertIs(user_email.EMAIL_REGEX, UserEmail.EMAIL_REGEX)

    def test_lazy_regex_compiler_verify_cannot_set_descriptor_on_instance(self):
        class UserEmail(object):
            EMAIL_REGEX = LazyRegexCompiler('@')

        user_email = UserEmail()
        with self.assertRaises(AttributeError):
            user_email.EMAIL_REGEX = re.compile('@')

    def test_lazy_regex_compiler_verify_can_override_class_attr(self):
        class UserEmail(object):
            EMAIL_REGEX = LazyRegexCompiler('@')

        UserEmail.EMAIL_REGEX = re.compile('cookies')
        self.assertEqual(UserEmail.EMAIL_REGEX.search('Cake & cookies').group(), 'cookies')
