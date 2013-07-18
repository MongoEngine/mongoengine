import sys
sys.path[0:0] = [""]

import unittest

from mongoengine import *

import jinja2


class TemplateFilterTest(unittest.TestCase):

    def setUp(self):
        connect(db='mongoenginetest')

    def test_jinja2(self):
        env = jinja2.Environment()

        class TestData(Document):
            title = StringField()
            description = StringField()

        TestData.drop_collection()

        examples = [('A', '1'),
                    ('B', '2'),
                    ('C', '3')]

        for title, description in examples:
            TestData(title=title, description=description).save()

        tmpl = """
{%- for record in content -%}
    {%- if loop.first -%}{ {%- endif -%}
    "{{ record.title }}": "{{ record.description }}"
    {%- if loop.last -%} }{%- else -%},{% endif -%}
{%- endfor -%}
"""
        ctx = {'content': TestData.objects}
        template = env.from_string(tmpl)
        rendered = template.render(**ctx)

        self.assertEqual('{"A": "1","B": "2","C": "3"}', rendered)


if __name__ == '__main__':
    unittest.main()
