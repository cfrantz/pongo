import unittest
import uuid
from datetime import datetime
import _pongo as pongo
import json
import os

class BadType(object):
    pass

class ToPongo(object):
    def __topongo__(self):
        return dict(a=1, b=2, c=3)

class IterMapping(object):
    def __init__(self):
        self.val = dict(a=1,b=2,c=3)
    def __getitem__(self, name):
        return self.val[name]
    def __setitem__(self, name, value):
        self.val[name] = value
    def __delitem__(self, name):
        del self.val[name]
    def __len__(self):
        return len(self.val)
    def __iter__(self):
        for item in self.val.items():
            yield item

# FIXME: how to get a bound method to work?
_key = 0
def _newkey(value):
    global _key
    _key += 1
    ret = 'foo-%d' % _key
    return ret

class TestPongo(unittest.TestCase):
    def setUp(self):
        self.db = pongo.open('test.db')
        # We don't need full 2-phase commit for the test program
        pongo.meta(self.db, '.sync', 0)
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
                "unicode": u"Fuzzy Wuzzy was a bear"
        }
        self.db['primitive'] = self.primitive
        self.db['list'] = [1,2,3,4,5,6]

    def tearDown(self):
        pongo.close(self.db)

    def test_primitive(self):
        p = self.db['primitive']
        for k, v in self.primitive.items():
            self.assertEqual(p[k], v)

    def test_bad_type(self):
        self.assertRaises(TypeError, self.db.set, 'badtype', BadType())

    def test_to_pongo(self):
        self.db['topongo'] = ToPongo()
        self.assertEqual(self.db['topongo'].native(), dict(a=1,b=2,c=3))

    def test_itermapping(self):
        self.db['itermapping'] = IterMapping()
        self.assertEqual(self.db['itermapping'].native(), dict(a=1,b=2,c=3))

    def test_types(self):
        self.assertTrue(isinstance(self.db, pongo.PongoCollection))
        self.assertTrue(isinstance(self.db['primitive'], pongo.PongoDict))
        self.assertTrue(isinstance(self.db['list'], pongo.PongoList))

    def test_meta(self):
        self.assertEqual(pongo.meta(self.db, 'chunksize'), 16*1024*1024)
        self.assertEqual(pongo.meta(self.db, 'id'), "_id")
        # FIXME: .sync, .uuid_class, .uuid_constructor

    def test_newkey(self):
        return
        old = pongo.meta(self.db, '.newkey', _newkey)

        self.assertEqual(self.db.set(pongo.id, {}), 'foo-1')
        self.assertEqual(self.db.set(pongo.id, {}), 'foo-2')
        self.assertEqual(self.db.set(pongo.id, {}), 'foo-3')
        pongo.meta(self.db, '.newkey', old)

    def test_atoms(self):
        atoms = pongo.atoms(self.db)
        self.assertTrue(isinstance(atoms, pongo.PongoCollection))

    def test_pidcache(self):
        # pongo.pidcache is for debug only and should not be used
        pid = os.getpid()
        pc = pongo.pidcache(self.db)
        self.assertTrue(pid in pc)

    def test__object(self):
        # pongo._object is for debug only and should not be used
        pass
    def test__info(self):
        # pongo._info is for debug only and should not be used
        pass

    def test_gc(self):
        # There's nothing to check for here, but it gets the code
        # covered by the coverage analyzer.
        pongo.gc(self.db)

    def test_list(self):
        l = self.db['list']
        # repr
        self.assertTrue(repr(l).startswith('PongoList'))
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
        
        # setitem
        l[4] = 7
        # native (after remove)
        self.assertEqual(l.native(), [1,2,4,5,7])

        # delitem
        del l[0]
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

        # repr
        self.assertTrue(repr(d).startswith('PongoDict'))

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
        # del non-exist item
        def doit():
            del d['zzz']
        self.assertRaises(KeyError, doit)

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

        # Non-str key for get/set
        d.set(55, 66)
        self.assertEqual(d.get(55), 66)

        # set with auto-id
        for i in range(8):
            x = d.set(pongo.id, { "p": True, "q":i })
            self.assertTrue(isinstance(x, uuid.UUID))

        # search
        r = d.search("p", "==", True)
        self.assertEqual(len(r), 8)
        r = d.search("q", "!=", 0)
        self.assertEqual(len(r), 7)
        r = d.search("q", "<", 4)
        self.assertEqual(len(r), 4)
        r = d.search("q", "<=", 4)
        self.assertEqual(len(r), 5)
        r = d.search("q", ">", 8)
        self.assertEqual(len(r), 0)
        r = d.search("q", ">=", 4)
        self.assertEqual(len(r), 4)
        self.assertRaises(ValueError, d.search, "q", "$", 4)
        self.assertRaises(TypeError, d.search, 4, "==", 4)

        # pop
        self.assertEqual(d.pop('f'), 5)
        self.assertEqual(d.pop('g', 6), 6)
        self.assertRaises(KeyError, d.pop, 'g')

        # json 1-arg form
        a = dict(a=1,b=2,c=3)
        d.json(json.dumps(a))
        self.assertEqual(d.native(), a)

        # json 2-arg form
        b = dict(x=7,y=8,z=9)
        d.json('b', json.dumps(b))
        self.assertEqual(d['b'].native(), b)

    def test_collection(self):
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

        # Collections aren't created by simple assignment.
        # They must be explicitly created.
        # Note: the interior dict "d" will get translated
        # into a PongoDict.
        d = pongo.PongoCollection.create(self.db)
        for k, v in mydict.items():
            d[k] = v

        # repr
        self.assertTrue(repr(d).startswith('PongoCollection'))
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
        # del non-exist item
        def doit():
            del d['zzz']
        self.assertRaises(KeyError, doit)

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

        # Non-str key for get/set
        d.set(55, 66)
        self.assertEqual(d.get(55), 66)

        # set with auto-id
        for i in range(8):
            x = d.set(pongo.id, { "p": True, "q":i })
            self.assertTrue(isinstance(x, uuid.UUID))
    
        # search
        r = d.search("p", "==", True)
        self.assertEqual(len(r), 8)
        r = d.search("q", "!=", 0)
        self.assertEqual(len(r), 7)
        r = d.search("q", "<", 4)
        self.assertEqual(len(r), 4)
        r = d.search("q", "<=", 4)
        self.assertEqual(len(r), 5)
        r = d.search("q", ">", 8)
        self.assertEqual(len(r), 0)
        r = d.search("q", ">=", 4)
        self.assertEqual(len(r), 4)
        self.assertRaises(ValueError, d.search, "q", "$", 4)
        self.assertRaises(TypeError, d.search, 4, "==", 4)

        # pop
        self.assertEqual(d.pop('f'), 5)
        self.assertEqual(d.pop('g', 6), 6)
        self.assertRaises(KeyError, d.pop, 'g')

        # json 1-arg form
        a = dict(a=1,b=2,c=3)
        self.assertRaises(NotImplementedError, d.json, json.dumps(a))

        # json 2-arg form
        b = dict(x=7,y=8,z=9)
        d.json('b', json.dumps(b))
        self.assertEqual(d['b'].native(), b)


    def test_membership(self):
        self.assertTrue('primitive' in self.db)
        self.assertFalse('blurf' in self.db)

        self.assertTrue('true' in self.db['primitive'])
        self.assertFalse('blurf' in self.db['primitive'])

        self.assertTrue(1 in self.db['list'])
        self.assertFalse(99 in self.db['list'])

    def test_create(self):
        l = pongo.PongoList.create(self.db)
        self.assertTrue(isinstance(l, pongo.PongoList))
        c = pongo.PongoCollection.create(self.db)
        self.assertTrue(isinstance(c, pongo.PongoCollection))
        d = pongo.PongoDict.create(self.db)
        self.assertTrue(isinstance(d, pongo.PongoDict))

    def test_pongo_check(self):
        p = self.db['primitive']
        self.assertRaises(TypeError, pongo.PongoList.create, p)
        self.assertRaises(TypeError, pongo.PongoList.create, None)



if __name__ == '__main__':
    unittest.main()






# vim: ts=4 sts=4 sw=4 expandtab:
