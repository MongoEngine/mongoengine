import unittest

from mongoengine import Document
from mongoengine.base.datastructures import StrictDict, BaseList, BaseDict


class DocumentStub(object):
    def __init__(self):
        self._changed_fields = []

    def _mark_as_changed(self, key):
        self._changed_fields.append(key)


class TestBaseDict(unittest.TestCase):

    @staticmethod
    def _get_basedict(dict_items):
        """Get a BaseList bound to a fake document instance"""
        fake_doc = DocumentStub()
        base_list = BaseDict(dict_items, instance=None, name='my_name')
        base_list._instance = fake_doc  # hack to inject the mock, it does not work in the constructor
        return base_list

    def test___init___(self):
        class MyDoc(Document):
            pass

        dict_items = {'k': 'v'}
        doc = MyDoc()
        base_dict = BaseDict(dict_items, instance=doc, name='my_name')
        self.assertIsInstance(base_dict._instance, Document)
        self.assertEqual(base_dict._name, 'my_name')
        self.assertEqual(base_dict, dict_items)

    def test_setdefault_calls_mark_as_changed(self):
        base_dict = self._get_basedict({})
        base_dict.setdefault('k', 'v')
        self.assertEqual(base_dict._instance._changed_fields, [base_dict._name])

    def test_popitems_calls_mark_as_changed(self):
        base_dict = self._get_basedict({'k': 'v'})
        self.assertEqual(base_dict.popitem(), ('k', 'v'))
        self.assertEqual(base_dict._instance._changed_fields, [base_dict._name])
        self.assertFalse(base_dict)

    def test_pop_calls_mark_as_changed(self):
        base_dict = self._get_basedict({'k': 'v'})
        self.assertEqual(base_dict.pop('k'), 'v')
        self.assertEqual(base_dict._instance._changed_fields, [base_dict._name])
        self.assertFalse(base_dict)

    def test_pop_calls_does_not_mark_as_changed_when_it_fails(self):
        base_dict = self._get_basedict({'k': 'v'})
        with self.assertRaises(KeyError):
            base_dict.pop('X')
        self.assertFalse(base_dict._instance._changed_fields)

    def test_clear_calls_mark_as_changed(self):
        base_dict = self._get_basedict({'k': 'v'})
        base_dict.clear()
        self.assertEqual(base_dict._instance._changed_fields, ['my_name'])
        self.assertEqual(base_dict, {})

    def test___delitem___calls_mark_as_changed(self):
        base_dict = self._get_basedict({'k': 'v'})
        del base_dict['k']
        self.assertEqual(base_dict._instance._changed_fields, ['my_name.k'])
        self.assertEqual(base_dict, {})

    def test___getitem____KeyError(self):
        base_dict = self._get_basedict({})
        with self.assertRaises(KeyError):
            base_dict['new']

    def test___getitem____simple_value(self):
        base_dict = self._get_basedict({'k': 'v'})
        base_dict['k'] = 'v'

    def test___getitem____sublist_gets_converted_to_BaseList(self):
        base_dict = self._get_basedict({'k': [0, 1, 2]})
        sub_list = base_dict['k']
        self.assertEqual(sub_list, [0, 1, 2])
        self.assertIsInstance(sub_list, BaseList)
        self.assertIs(sub_list._instance, base_dict._instance)
        self.assertEqual(sub_list._name, 'my_name.k')
        self.assertEqual(base_dict._instance._changed_fields, [])

        # Challenge mark_as_changed from sublist
        sub_list[1] = None
        self.assertEqual(base_dict._instance._changed_fields, ['my_name.k.1'])

    def test___getitem____subdict_gets_converted_to_BaseDict(self):
        base_dict = self._get_basedict({'k': {'subk': 'subv'}})
        sub_dict = base_dict['k']
        self.assertEqual(sub_dict, {'subk': 'subv'})
        self.assertIsInstance(sub_dict, BaseDict)
        self.assertIs(sub_dict._instance, base_dict._instance)
        self.assertEqual(sub_dict._name, 'my_name.k')
        self.assertEqual(base_dict._instance._changed_fields, [])

        # Challenge mark_as_changed from subdict
        sub_dict['subk'] = None
        self.assertEqual(base_dict._instance._changed_fields, ['my_name.k.subk'])

    def test_get_sublist_gets_converted_to_BaseList_just_like__getitem__(self):
        base_dict = self._get_basedict({'k': [0, 1, 2]})
        sub_list = base_dict.get('k')
        self.assertEqual(sub_list, [0, 1, 2])
        self.assertIsInstance(sub_list, BaseList)

    def test_get_returns_the_same_as___getitem__(self):
        base_dict = self._get_basedict({'k': [0, 1, 2]})
        get_ = base_dict.get('k')
        getitem_ = base_dict['k']
        self.assertEqual(get_, getitem_)

    def test_get_default(self):
        base_dict = self._get_basedict({})
        sentinel = object()
        self.assertEqual(base_dict.get('new'), None)
        self.assertIs(base_dict.get('new', sentinel), sentinel)

    def test___setitem___calls_mark_as_changed(self):
        base_dict = self._get_basedict({})
        base_dict['k'] = 'v'
        self.assertEqual(base_dict._instance._changed_fields, ['my_name.k'])
        self.assertEqual(base_dict, {'k': 'v'})

    def test_update_calls_mark_as_changed(self):
        base_dict = self._get_basedict({})
        base_dict.update({'k': 'v'})
        self.assertEqual(base_dict._instance._changed_fields, ['my_name'])

    def test___setattr____not_tracked_by_changes(self):
        base_dict = self._get_basedict({})
        base_dict.a_new_attr = 'test'
        self.assertEqual(base_dict._instance._changed_fields, [])

    def test___delattr____tracked_by_changes(self):
        # This is probably a bug as __setattr__ is not tracked
        # This is even bad because it could be that there is an attribute
        # with the same name as a key
        base_dict = self._get_basedict({})
        base_dict.a_new_attr = 'test'
        del base_dict.a_new_attr
        self.assertEqual(base_dict._instance._changed_fields, ['my_name.a_new_attr'])


class TestBaseList(unittest.TestCase):

    @staticmethod
    def _get_baselist(list_items):
        """Get a BaseList bound to a fake document instance"""
        fake_doc = DocumentStub()
        base_list = BaseList(list_items, instance=None, name='my_name')
        base_list._instance = fake_doc  # hack to inject the mock, it does not work in the constructor
        return base_list

    def test___init___(self):
        class MyDoc(Document):
            pass

        list_items = [True]
        doc = MyDoc()
        base_list = BaseList(list_items, instance=doc, name='my_name')
        self.assertIsInstance(base_list._instance, Document)
        self.assertEqual(base_list._name, 'my_name')
        self.assertEqual(base_list, list_items)

    def test___iter__(self):
        values = [True, False, True, False]
        base_list = BaseList(values, instance=None, name='my_name')
        self.assertEqual(values, list(base_list))

    def test___iter___allow_modification_while_iterating_withou_error(self):
        # regular list allows for this, thus this subclass must comply to that
        base_list = BaseList([True, False, True, False], instance=None, name='my_name')
        for idx, val in enumerate(base_list):
            if val:
                base_list.pop(idx)

    def test_append_calls_mark_as_changed(self):
        base_list = self._get_baselist([])
        self.assertFalse(base_list._instance._changed_fields)
        base_list.append(True)
        self.assertEqual(base_list._instance._changed_fields, ['my_name'])

    def test_subclass_append(self):
        # Due to the way mark_as_changed_wrapper is implemented
        # it is good to test subclasses
        class SubBaseList(BaseList):
            pass

        base_list = SubBaseList([], instance=None, name='my_name')
        base_list.append(True)

    def test___getitem__using_simple_index(self):
        base_list = self._get_baselist([0, 1, 2])
        self.assertEqual(base_list[0], 0)
        self.assertEqual(base_list[1], 1)
        self.assertEqual(base_list[-1], 2)

    def test___getitem__using_slice(self):
        base_list = self._get_baselist([0, 1, 2])
        self.assertEqual(base_list[1:3], [1, 2])
        self.assertEqual(base_list[0:3:2], [0, 2])

    def test___getitem___using_slice_returns_list(self):
        # Bug: using slice does not properly handles the instance
        # and mark_as_changed behaviour.
        base_list = self._get_baselist([0, 1, 2])
        sliced = base_list[1:3]
        self.assertEqual(sliced, [1, 2])
        self.assertIsInstance(sliced, list)
        self.assertEqual(base_list._instance._changed_fields, [])

    def test___getitem__sublist_returns_BaseList_bound_to_instance(self):
        base_list = self._get_baselist(
            [
                [1, 2],
                [3, 4]
            ]
        )
        sub_list = base_list[0]
        self.assertEqual(sub_list, [1, 2])
        self.assertIsInstance(sub_list, BaseList)
        self.assertIs(sub_list._instance, base_list._instance)
        self.assertEqual(sub_list._name, 'my_name.0')
        self.assertEqual(base_list._instance._changed_fields, [])

        # Challenge mark_as_changed from sublist
        sub_list[1] = None
        self.assertEqual(base_list._instance._changed_fields, ['my_name.0.1'])

    def test___getitem__subdict_returns_BaseList_bound_to_instance(self):
        base_list = self._get_baselist(
            [
                {'subk': 'subv'}
            ]
        )
        sub_dict = base_list[0]
        self.assertEqual(sub_dict, {'subk': 'subv'})
        self.assertIsInstance(sub_dict, BaseDict)
        self.assertIs(sub_dict._instance, base_list._instance)
        self.assertEqual(sub_dict._name, 'my_name.0')
        self.assertEqual(base_list._instance._changed_fields, [])

        # Challenge mark_as_changed from subdict
        sub_dict['subk'] = None
        self.assertEqual(base_list._instance._changed_fields, ['my_name.0.subk'])

    def test_extend_calls_mark_as_changed(self):
        base_list = self._get_baselist([])
        base_list.extend([True])
        self.assertEqual(base_list._instance._changed_fields, ['my_name'])

    def test_insert_calls_mark_as_changed(self):
        base_list = self._get_baselist([])
        base_list.insert(0, True)
        self.assertEqual(base_list._instance._changed_fields, ['my_name'])

    def test_remove_calls_mark_as_changed(self):
        base_list = self._get_baselist([True])
        base_list.remove(True)
        self.assertEqual(base_list._instance._changed_fields, ['my_name'])

    def test_remove_not_mark_as_changed_when_it_fails(self):
        base_list = self._get_baselist([True])
        with self.assertRaises(ValueError):
            base_list.remove(False)
        self.assertFalse(base_list._instance._changed_fields)

    def test_pop_calls_mark_as_changed(self):
        base_list = self._get_baselist([True])
        base_list.pop()
        self.assertEqual(base_list._instance._changed_fields, ['my_name'])

    def test_reverse_calls_mark_as_changed(self):
        base_list = self._get_baselist([True, False])
        base_list.reverse()
        self.assertEqual(base_list._instance._changed_fields, ['my_name'])

    def test___delitem___calls_mark_as_changed(self):
        base_list = self._get_baselist([True])
        del base_list[0]
        self.assertEqual(base_list._instance._changed_fields, ['my_name'])

    def test___setitem___calls_with_full_slice_mark_as_changed(self):
        base_list = self._get_baselist([])
        base_list[:] = [0, 1]      # Will use __setslice__ under py2 and __setitem__ under py3
        self.assertEqual(base_list._instance._changed_fields, ['my_name'])
        self.assertEqual(base_list, [0, 1])

    def test___setitem___calls_with_partial_slice_mark_as_changed(self):
        base_list = self._get_baselist([0, 1, 2])
        base_list[0:2] = [1, 0]     # Will use __setslice__ under py2 and __setitem__ under py3
        self.assertEqual(base_list._instance._changed_fields, ['my_name'])
        self.assertEqual(base_list, [1, 0, 2])

    def test___setitem___calls_with_step_slice_mark_as_changed(self):
        base_list = self._get_baselist([0, 1, 2])
        base_list[0:3:2] = [-1, -2]   # uses __setitem__ in both py2 & 3
        self.assertEqual(base_list._instance._changed_fields, ['my_name'])
        self.assertEqual(base_list, [-1, 1, -2])

    def test___setitem___with_slice(self):
        base_list = self._get_baselist([0, 1, 2, 3, 4, 5])
        base_list[0:6:2] = [None, None, None]
        self.assertEqual(base_list._instance._changed_fields, ['my_name'])
        self.assertEqual(base_list, [None, 1, None, 3, None, 5])

    def test___setitem___item_0_calls_mark_as_changed(self):
        base_list = self._get_baselist([True])
        base_list[0] = False
        self.assertEqual(base_list._instance._changed_fields, ['my_name'])
        self.assertEqual(base_list, [False])

    def test___setitem___item_1_calls_mark_as_changed(self):
        base_list = self._get_baselist([True, True])
        base_list[1] = False
        self.assertEqual(base_list._instance._changed_fields, ['my_name.1'])
        self.assertEqual(base_list, [True, False])

    def test___delslice___calls_mark_as_changed(self):
        base_list = self._get_baselist([0, 1])
        del base_list[0:1]
        self.assertEqual(base_list._instance._changed_fields, ['my_name'])
        self.assertEqual(base_list, [1])

    def test___iadd___calls_mark_as_changed(self):
        base_list = self._get_baselist([True])
        base_list += [False]
        self.assertEqual(base_list._instance._changed_fields, ['my_name'])

    def test___imul___calls_mark_as_changed(self):
        base_list = self._get_baselist([True])
        self.assertEqual(base_list._instance._changed_fields, [])
        base_list *= 2
        self.assertEqual(base_list._instance._changed_fields, ['my_name'])

    def test_sort_calls_not_marked_as_changed_when_it_fails(self):
        base_list = self._get_baselist([True])
        with self.assertRaises(TypeError):
            base_list.sort(key=1)

        self.assertEqual(base_list._instance._changed_fields, [])

    def test_sort_calls_mark_as_changed(self):
        base_list = self._get_baselist([True, False])
        base_list.sort()
        self.assertEqual(base_list._instance._changed_fields, ['my_name'])

    def test_sort_calls_with_key(self):
        base_list = self._get_baselist([1, 2, 11])
        base_list.sort(key=lambda i: str(i))
        self.assertEqual(base_list, [1, 11, 2])


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
        self.assertEqual(dict(d), {'a': 1, 'b': 2})
        self.assertEqual(dict(**d), {'a': 1, 'b': 2})


if __name__ == '__main__':
    unittest.main()
