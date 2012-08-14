import unittest
import uuid
from datetime import datetime
import _pongo as pongo
import json

class TestPongo(unittest.TestCase):
    def setUp(self):
        self.db = pongo.open('test.db')
        self.primitive = {
                "null": None,
                "true": True,
                "false": False,
                "int-0": 0,
                "int-1": 1,
                "int-huge": 2**32,
                "now": datetime.utcnow(),
                "uuid": uuid.uuid1(),
                "float-0": 0.0,
                "float-2": 2.0,
                "float-avagadro": 6.022e23,
                "str": "The quick brown fox",
        }
        self.db['primitive'] = self.primitive
        self.db['list'] = [1,2,3,4,5,6]

    def tearDown(self):
        pongo.close(self.db)

    def test_primitive(self):
        p = self.db['primitive']
        for k, v in self.primitive.items():
            self.assertEqual(p[k], v)

    def test_types(self):
        self.assertTrue(isinstance(self.db, pongo.PongoCollection))
        self.assertTrue(isinstance(self.db['primitive'], pongo.PongoDict))
        self.assertTrue(isinstance(self.db['list'], pongo.PongoList))

    def test_list(self):
        l = self.db['list']
        # length
        self.assertEqual(len(l), 6)
        # iteration
        self.assertEqual(sum(l), 21)
        # indices
        self.assertEqual(l[0], 1)
        self.assertEqual(l[-1], 6)

        # append
        l.append(7)
        # pop
        self.assertEqual(l.pop(0), 1)
        self.assertEqual(l.pop(-1), 7)
        self.assertRaises(IndexError, l.pop, 5)
        self.assertRaises(IndexError, l.pop, -6)

        # insert
        l.insert(0, 1)
        self.assertEqual(l[0], 1)

        # remove
        l.remove(3)
        self.assertRaises(ValueError, l.remove, 99)
        
        # native (after remove)
        self.assertEqual(l.native(), [1,2,4,5,6])
        self.assertEqual(sum(l), 18)

    def test_dict(self):
        mydict = {
            "a":1,
            "b":2,
            "c":3,
            "d": {
                "x":10,
                "y":20,
                "z": [
                    { "s": 100, "t": 1, "i": True },
                    { "s": 200, "t": 1, "j": True },
                    { "s": 300, "t": 1, "k": True },
                    { "s": 400, "t": 1, "l": True },
                    { "s": 400, "t": 1, "m": True },
                ]
            }
        }
        self.db['dict'] = mydict
        d = self.db['dict']

        # length
        self.assertEqual(len(d), 4)

        # keys/values/items
        keys = d.keys()
        values = d.values()
        items = d.items()

        self.assertEqual(keys, ['a', 'b', 'c', 'd'])
        self.assertEqual(values[:3], [1,2,3])
        # FIXME: right now there is no compare between Pongo proxy objects,
        # so even if the proxies refer to the same object, their equality
        # cannot be established
        self.assertEqual(items[:3], zip(keys, values)[:3])

        # native and json
        self.assertEqual(d.native(), mydict)
        d2 = json.loads(d.json())
        self.assertEqual(d2, mydict)

        # getitem
        self.assertEqual(d['a'], 1)
        self.assertEqual(d['b'], 2)
        self.assertEqual(d['c'], 3)
        self.assertRaises(KeyError, lambda: d['e']==1)

        # setitem
        d['e'] = 5
        self.assertEqual(d['e'], 5)

        # delitem
        del d['e']
        self.assertRaises(KeyError, lambda: d['e']==1)

        # get
        self.assertEqual(d.get('f'), None)
        self.assertEqual(d.get('f', 1), 1)
        self.assertEqual(d.get('d.x'), 10)
        self.assertEqual(d.get('d/y', sep='/'), 20)

        # set
        d.set('f', 5)
        self.assertEqual(d['f'], 5)

        d.set('d.w', "Hello")
        self.assertEqual(d.get('d.w'), "Hello")

        d.set('d.once', 'foo', fail=True)
        self.assertRaises(KeyError, d.set, key='d.once', value='bar', fail=True)
        self.assertEqual(d.get('d.once'), "foo")

        d.set('d.z.4.m', False)
        self.assertEqual(d.get('d.z.4.m'), False)

        # set with auto-id
        for i in range(8):
            d.set(pongo.id, { "p": True, "q":i })

        # search
        r = d.search("p", "==", True)
        self.assertEqual(len(r), 8)
        r = d.search("q", "<", 4)
        self.assertEqual(len(r), 4)
        r = d.search("q", ">", 8)
        self.assertEqual(len(r), 0)

        # pop
        self.assertEqual(d.pop('f'), 5)
        self.assertEqual(d.pop('g', 6), 6)
        self.assertRaises(KeyError, d.pop, 'g')


    def test_membership(self):
        self.assertTrue('primitive' in self.db)
        self.assertFalse('blurf' in self.db)

        self.assertTrue('true' in self.db['primitive'])
        self.assertFalse('blurf' in self.db['primitive'])

        self.assertTrue(1 in self.db['list'])
        self.assertFalse(99 in self.db['list'])


if __name__ == '__main__':
    unittest.main()






# vim: ts=4 sts=4 sw=4 expandtab:
