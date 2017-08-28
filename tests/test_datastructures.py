import unittest

from mongoengine.base.datastructures import StrictDict


class TestStrictDict(unittest.TestCase):
    def strict_dict_class(self, *args, **kwargs):
        return StrictDict.create(*args, **kwargs)

    def setUp(self):
        self.dtype = self.strict_dict_class(("a", "b", "c"))

    def test_init(self):
        d = self.dtype(a=1, b=1, c=1)
        self.assertEqual((d.a, d.b, d.c), (1, 1, 1))

    def test_repr(self):
        d = self.dtype(a=1, b=2, c=3)
        self.assertEqual(repr(d), '{"a": 1, "b": 2, "c": 3}')

        # make sure quotes are escaped properly
        d = self.dtype(a='"', b="'", c="")
        self.assertEqual(repr(d), '{"a": \'"\', "b": "\'", "c": \'\'}')

    def test_init_fails_on_nonexisting_attrs(self):
        with self.assertRaises(AttributeError):
            self.dtype(a=1, b=2, d=3)

    def test_eq(self):
        d = self.dtype(a=1, b=1, c=1)
        dd = self.dtype(a=1, b=1, c=1)
        e = self.dtype(a=1, b=1, c=3)
        f = self.dtype(a=1, b=1)
        g = self.strict_dict_class(("a", "b", "c", "d"))(a=1, b=1, c=1, d=1)
        h = self.strict_dict_class(("a", "c", "b"))(a=1, b=1, c=1)
        i = self.strict_dict_class(("a", "c", "b"))(a=1, b=1, c=2)

        self.assertEqual(d, dd)
        self.assertNotEqual(d, e)
        self.assertNotEqual(d, f)
        self.assertNotEqual(d, g)
        self.assertNotEqual(f, d)
        self.assertEqual(d, h)
        self.assertNotEqual(d, i)

    def test_setattr_getattr(self):
        d = self.dtype()
        d.a = 1
        self.assertEqual(d.a, 1)
        self.assertRaises(AttributeError, getattr, d, 'b')

    def test_setattr_raises_on_nonexisting_attr(self):
        d = self.dtype()
        with self.assertRaises(AttributeError):
            d.x = 1

    def test_setattr_getattr_special(self):
        d = self.strict_dict_class(["items"])
        d.items = 1
        self.assertEqual(d.items, 1)

    def test_get(self):
        d = self.dtype(a=1)
        self.assertEqual(d.get('a'), 1)
        self.assertEqual(d.get('b', 'bla'), 'bla')

    def test_items(self):
        d = self.dtype(a=1)
        self.assertEqual(d.items(), [('a', 1)])
        d = self.dtype(a=1, b=2)
        self.assertEqual(d.items(), [('a', 1), ('b', 2)])

    def test_mappings_protocol(self):
        d = self.dtype(a=1, b=2)
        assert dict(d) == {'a': 1, 'b': 2}
        assert dict(**d) == {'a': 1, 'b': 2}


if __name__ == '__main__':
    unittest.main()
