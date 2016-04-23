import unittest
from mongoengine.base.datastructures import StrictDict, SemiStrictDict 


class TestStrictDict(unittest.TestCase):
    def strict_dict_class(self, *args, **kwargs):
        return StrictDict.create(*args, **kwargs)

    def setUp(self):
        self.dtype = self.strict_dict_class(("a", "b", "c"))

    def test_init(self):
        d = self.dtype(a=1, b=1, c=1)
        self.assertEqual((d.a, d.b, d.c), (1, 1, 1))

    def test_init_fails_on_nonexisting_attrs(self):
        self.assertRaises(AttributeError, lambda: self.dtype(a=1, b=2, d=3))
        
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
        self.assertRaises(AttributeError, lambda: d.b)
    
    def test_setattr_raises_on_nonexisting_attr(self):
        d = self.dtype()

        def _f():
            d.x = 1
        self.assertRaises(AttributeError, _f)
    
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


class TestSemiSrictDict(TestStrictDict):
    def strict_dict_class(self, *args, **kwargs):
        return SemiStrictDict.create(*args, **kwargs)

    def test_init_fails_on_nonexisting_attrs(self):
        # disable irrelevant test
        pass

    def test_setattr_raises_on_nonexisting_attr(self):
        # disable irrelevant test
        pass

    def test_setattr_getattr_nonexisting_attr_succeeds(self):
        d = self.dtype()
        d.x = 1
        self.assertEqual(d.x, 1)

    def test_init_succeeds_with_nonexisting_attrs(self):
        d = self.dtype(a=1, b=1, c=1, x=2)
        self.assertEqual((d.a, d.b, d.c, d.x), (1, 1, 1, 2))
   
    def test_iter_with_nonexisting_attrs(self):
        d = self.dtype(a=1, b=1, c=1, x=2)
        self.assertEqual(list(d), ['a', 'b', 'c', 'x'])

    def test_iteritems_with_nonexisting_attrs(self):
        d = self.dtype(a=1, b=1, c=1, x=2)
        self.assertEqual(list(d.iteritems()), [('a', 1), ('b', 1), ('c', 1), ('x', 2)])

    def tets_cmp_with_strict_dicts(self):
        d = self.dtype(a=1, b=1, c=1)
        dd = StrictDict.create(("a", "b", "c"))(a=1, b=1, c=1)
        self.assertEqual(d, dd)

    def test_cmp_with_strict_dict_with_nonexisting_attrs(self):
        d = self.dtype(a=1, b=1, c=1, x=2)
        dd = StrictDict.create(("a", "b", "c", "x"))(a=1, b=1, c=1, x=2)
        self.assertEqual(d, dd)

if __name__ == '__main__':
    unittest.main()
