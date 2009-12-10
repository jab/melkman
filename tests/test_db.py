from helpers import *

def test_get_by_ids():
    from couchdb.schema import IntegerField
    from melkman.db.util import DocumentHelper

    ctx = fresh_context()

    class Foo(DocumentHelper):
        i = IntegerField()
        @classmethod
        def from_doc(cls, doc, ctx):
            instance = super(Foo, cls).from_doc(doc, ctx)
            instance.flag = True
            return instance

    foo1 = Foo.create(ctx, i=1)
    foo1.save()
    foo2 = Foo.create(ctx, i=2)
    foo2.save()

    foo1, foo2 = Foo.get_by_ids((foo1.id, foo2.id), ctx)
    assert foo1.i == 1 and foo2.i == 2
    assert foo1.flag and foo2.flag

def test_db_util_doctest():
    import doctest
    from melkman.db import util
    doctest.testmod(util, raise_on_error=True)

def test_delete_all_in_view():
    from melkman.db.util import delete_all_in_view
    from couchdb.schema import Document
    from couchdb.design import ViewDefinition
    
    
    db = make_db()
    
    view_bad = ViewDefinition('test_daiv', 'bad_items', 
    '''
    function(doc) {
        if (doc.badflag == true) {
            emit(true, null);
        }
    }
    ''')

    
    view_bad.sync(db)
    
    for i in range(10):
        doc = Document('doc_%d' % i)
        doc['foo'] = i
        if i % 2 == 1:
            doc['badflag'] = True
        doc.store(db)
        
    for i in range(10):
        assert 'doc_%d' % i in db
    
    delete_all_in_view(db, view_bad)

    for i in range(10):
        doc_id = 'doc_%d' % i
        if i % 2 == 0:
            assert doc_id in db, 'expected %s in db' % doc_id
        else:
            assert doc_id not in db, 'expected %s not in db' % doc_id

from decimal import Decimal
import doctest
import os
import unittest

from couchdb import client, schema
from melkman.db import MappingField
class MappingFieldTestCase(unittest.TestCase):

    def setUp(self):
        uri = os.environ.get('COUCHDB_URI', 'http://localhost:5984/')
        self.server = client.Server(uri)
        if 'python-tests' in self.server:
            del self.server['python-tests']
        self.db = self.server.create('python-tests')

    def tearDown(self):
        if 'python-tests' in self.server:
            del self.server['python-tests']

    def test_proxy_contains(self):

        class Thing(schema.Document):
            stuff = MappingField(schema.DecimalField, key_field=schema.DecimalField)

        thing = Thing()
        thing.stuff[Decimal('1.0')] = Decimal('2.0')

        assert Decimal('1.0') in thing.stuff
        assert Decimal('2.0') not in thing.stuff


    def test_proxy_delitem(self):
        class Thing(schema.Document):
            stuff = MappingField(schema.DecimalField, key_field=schema.DecimalField)

        thing = Thing()
        thing.stuff[Decimal('1.0')] = Decimal('2.0')

        assert Decimal('1.0') in thing.stuff
        self.assertRaises(KeyError, lambda: thing.stuff.__delitem__(Decimal('2.0')))
        del thing.stuff[Decimal('1.0')]

        assert Decimal('1.0') not in thing.stuff
        assert len(thing.stuff) == 0

    def test_proxy_iter(self):
        class Thing(schema.Document):
            stuff = MappingField(schema.DecimalField, key_field=schema.DecimalField)

        thing = Thing()

        thing.stuff[Decimal('1.0')] = Decimal('2.0')
        thing.stuff[Decimal('3.0')] = Decimal('4.0')

        expected_keys = set([Decimal('1.0'), Decimal('3.0')])        
        got_keys = set()
        for k in thing.stuff:
            assert k in expected_keys
            got_keys.add(k)

        for k in expected_keys:
            assert k in got_keys

    def test_proxy_get_item(self):
        class Thing(schema.Document):
            stuff = MappingField(schema.DecimalField, key_field=schema.DecimalField)

        thing = Thing()
        thing.stuff[Decimal('1.0')] = Decimal('2.0')

        assert thing.stuff[Decimal('1.0')] == Decimal('2.0')
        self.assertRaises(KeyError, lambda: thing.stuff[Decimal('2.0')])

    def test_proxy_set_item(self):
        class Thing(schema.Document):
            stuff = MappingField(schema.FloatField, key_field=schema.FloatField)

        thing = Thing()
        thing.stuff[1.0] = 2.0
        assert thing.stuff[1.0] == 2.0

        self.assertRaises(ValueError, lambda: thing.stuff.__setitem__('zztop', 5.0))
        self.assertRaises(ValueError, lambda: thing.stuff.__setitem__(6.0, 'zztop'))


    def test_proxy_clear(self):
        class Thing(schema.Document):
            stuff = MappingField(schema.DecimalField, key_field=schema.DecimalField)

        thing = Thing()
        thing.stuff[Decimal('1.0')] = Decimal('2.0')
        thing.stuff[Decimal('3.0')] = Decimal('4.0')

        assert Decimal('1.0') in thing.stuff
        assert Decimal('3.0') in thing.stuff
        assert len(thing.stuff) == 2

        thing.stuff.clear()

        assert Decimal('1.0') not in thing.stuff
        assert Decimal('3.0') not in thing.stuff
        assert len(thing.stuff) == 0

    def test_proxy_fromkeys(self):
        self.assertRaises(NotImplementedError, MappingField.Proxy.fromkeys, [1, 2, 3])
        self.assertRaises(NotImplementedError, MappingField.Proxy.fromkeys, [1, 2, 3], 'a')

        class Thing(schema.Document):
            stuff = MappingField(schema.DecimalField)
        thing = Thing()
        self.assertRaises(NotImplementedError, thing.stuff.fromkeys, [1, 2, 3])
        self.assertRaises(NotImplementedError, thing.stuff.fromkeys, [1, 2, 3], 'a')

    def test_proxy_get(self):
        class Thing(schema.Document):
            stuff = MappingField(schema.DecimalField, key_field=schema.DecimalField)

        thing = Thing()
        thing.stuff[Decimal('1.0')] = Decimal('2.0')

        assert thing.stuff.get(Decimal('1.0')) == Decimal('2.0')
        assert thing.stuff.get(Decimal('2.0')) is None
        assert thing.stuff.get(Decimal('2.0'), Decimal('5.0')) == Decimal('5.0')
        assert thing.stuff.get(Decimal('2.0'), 5.0) == 5.0

    def test_proxy_has_key(self):
        class Thing(schema.Document):
            stuff = MappingField(schema.DecimalField, key_field=schema.FloatField)

        thing = Thing()
        thing.stuff[1.0] = Decimal('2.0')

        assert thing.stuff.has_key(1.0)
        assert not thing.stuff.has_key(2.0)
        assert not thing.stuff.has_key('zztop')

    def test_proxy_items(self):
        class Thing(schema.Document):
            stuff = MappingField(schema.DecimalField, key_field=schema.DecimalField)

        thing = Thing()
        thing.stuff[Decimal('1.0')] = Decimal('2.0')
        thing.stuff[Decimal('3.0')] = Decimal('4.0')


        # it should be a list, so it should have a length
        assert len(thing.stuff.items()) == 2

        expected_items = set([(Decimal('1.0'), Decimal('2.0')), 
                              (Decimal('3.0'), Decimal('4.0'))])

        got_items = set()
        for i in thing.stuff.items():
            assert i in expected_items
            got_items.add(i)

        for i in expected_items:
            assert i in got_items

    def test_proxy_iteritems(self):
        class Thing(schema.Document):
            stuff = MappingField(schema.DecimalField, key_field=schema.DecimalField)

        thing = Thing()
        thing.stuff[Decimal('1.0')] = Decimal('2.0')
        thing.stuff[Decimal('3.0')] = Decimal('4.0')

        expected_items = set([(Decimal('1.0'), Decimal('2.0')), 
                              (Decimal('3.0'), Decimal('4.0'))])

        got_items = set()
        for i in thing.stuff.items():
            assert i in expected_items
            got_items.add(i)

        for i in expected_items:
            assert i in got_items

    def test_proxy_iterkeys(self):
        class Thing(schema.Document):
            stuff = MappingField(schema.DecimalField, key_field=schema.DecimalField)

        thing = Thing()

        thing.stuff[Decimal('1.0')] = Decimal('2.0')
        thing.stuff[Decimal('3.0')] = Decimal('4.0')

        expected_keys = set([Decimal('1.0'), Decimal('3.0')])        
        got_keys = set()
        for k in thing.stuff.iterkeys():
            assert k in expected_keys
            got_keys.add(k)

        for k in expected_keys:
            assert k in got_keys


    def test_proxy_keys(self):
        class Thing(schema.Document):
            stuff = MappingField(schema.DecimalField, key_field=schema.DecimalField)

        thing = Thing()

        thing.stuff[Decimal('1.0')] = Decimal('2.0')
        thing.stuff[Decimal('3.0')] = Decimal('4.0')

        # should be a list, so should have a length
        assert len(thing.stuff.keys()) == 2

        expected_keys = set([Decimal('1.0'), Decimal('3.0')])        
        got_keys = set()
        for k in thing.stuff.keys():
            assert k in expected_keys
            got_keys.add(k)

        for k in expected_keys:
            assert k in got_keys

    def test_proxy_pop(self):
        class Thing(schema.Document):
            stuff = MappingField(schema.DecimalField, key_field=schema.DecimalField)

        thing = Thing()

        thing.stuff[Decimal('1.0')] = Decimal('2.0')
        thing.stuff[Decimal('3.0')] = Decimal('4.0')

        # check default return
        assert thing.stuff.pop(Decimal('9.0'), 'elephant') == 'elephant'
        # with no default, it raises KeyError
        self.assertRaises(KeyError, thing.stuff.pop, Decimal('9.0'))

        assert len(thing.stuff) == 2
        assert Decimal('1.0') in thing.stuff
        assert Decimal('3.0') in thing.stuff
        assert thing.stuff.pop(Decimal('1.0')) == Decimal('2.0')
        assert len(thing.stuff) == 1
        assert not Decimal('1.0') in thing.stuff
        assert Decimal('3.0') in thing.stuff

    def test_proxy_popitem(self):
        class Thing(schema.Document):
            stuff = MappingField(schema.DecimalField, key_field=schema.DecimalField)

        thing = Thing()

        thing.stuff[Decimal('1.0')] = Decimal('2.0')
        thing.stuff[Decimal('3.0')] = Decimal('4.0')

        expected_items = [(Decimal('1.0'), Decimal('2.0')), 
                          (Decimal('3.0'), Decimal('4.0'))]

        expected_items.remove(thing.stuff.popitem())
        expected_items.remove(thing.stuff.popitem())

        assert len(expected_items) == 0
        assert len(thing.stuff) == 0
        self.assertRaises(KeyError, thing.stuff.popitem)

    def test_proxy_setdefault(self):
        class Thing(schema.Document):
            stuff = MappingField(schema.FloatField, key_field=schema.FloatField)

        thing = Thing()

        thing.stuff[1.0] = 2.0

        assert thing.stuff.setdefault(1.0, 0.0) == 2.0
        assert thing.stuff.setdefault(2.0, 3.0) == 3.0
        # should come back as the newly set value
        assert thing.stuff.setdefault(2.0, 4.0) == 3.0

        # default to None, can't set a value of None...
        self.assertRaises(ValueError, thing.stuff.setdefault, 3.0)


    def test_proxy_update(self):
        class Thing(schema.Document):
            stuff = MappingField(schema.DecimalField, key_field=schema.DecimalField)

        thing = Thing()
        thing.stuff[Decimal('1.0')] = Decimal('2.0')
        thing.stuff[Decimal('2.0')] = Decimal('2.5')

        thing.stuff.update({Decimal('1.0'): Decimal('1.5'),
                            Decimal('3.0'): Decimal('3.5')})

        assert thing.stuff[Decimal('1.0')] == Decimal('1.5')
        assert thing.stuff[Decimal('2.0')] == Decimal('2.5')
        assert thing.stuff[Decimal('3.0')] == Decimal('3.5')    


    def test_proxy_values(self):
        class Thing(schema.Document):
            stuff = MappingField(schema.DecimalField, key_field=schema.DecimalField)

        thing = Thing()

        thing.stuff[Decimal('1.0')] = Decimal('2.0')
        thing.stuff[Decimal('3.0')] = Decimal('4.0')

        # should be a list, so should have a length
        assert len(thing.stuff.values()) == 2

        expected_vals = set([Decimal('2.0'), Decimal('4.0')])        
        got_vals = set()
        for v in thing.stuff.values():
            assert v in expected_vals
            got_vals.add(v)

        for v in expected_vals:
            assert v in got_vals

    def test_key_equivalence(self):
        """
        tests the equivalence of keys that 
        serialize to the same json value.
        """
        class Thing(schema.Document):
            stuff = MappingField(schema.FloatField, key_field=schema.FloatField)
        thing = Thing()

        thing.stuff[1.0] = 2.0
        assert 1.0 in thing.stuff
        assert '1.0' in thing.stuff
        assert 1 in thing.stuff



    def test_store_load(self):
        from datetime import date

        class Book(schema.Schema):
            isbn = schema.TextField()
            title = schema.TextField()
            authors = schema.ListField(schema.TextField())
            publish_date = schema.DateField()
            pages = schema.IntegerField()

        class Bookshelf(schema.Document):
            # books by isbn
            books = MappingField(schema.DictField(Book))

        shelf = Bookshelf('couchdb_books')

        book_data = [
        {'isbn': 'URN:ISBN:978-059-61-5589-6',
         'title': 'CouchDB: The Definitive Guide', 
         'authors': ['J. Anderson', 'Jan Lehnardt', 'Noah Slater'],
         'publish_date': date(2009, 11, 15),
         'pages': 300},
        {'isbn': 'URN:ISBN:978-059-65-1818-9',
        'title': 'Erlang Programming',
        'authors': ['Francesco Cesarini', 'Simon Thompson'],
        'publish_date': date(2009, 6, 26),
        'pages': 494},
        {'isbn': 'URN:ISBN:978-059-61-0199-2',
        'title': 'Javascript: The Definitive Guide',
        'authors': ['David Flanagan'],
        'publish_date': date(2006, 8, 17),
        'pages': 1018}]

        for b in book_data:
            shelf.books[b['isbn']] = b

        shelf.store(self.db)

        shelf = Bookshelf.load(self.db, 'couchdb_books')
        for b in book_data:
            assert b['isbn'] in shelf.books
            book = shelf.books[b['isbn']]
            for key in b.keys():
                assert getattr(book, key) == b[key]


# def suite():
#     suite = unittest.TestSuite()
#     suite.addTest(doctest.DocTestSuite(schema))
#     suite.addTest(unittest.makeSuite(DocumentTestCase, 'test'))
#     suite.addTest(unittest.makeSuite(ListFieldTestCase, 'test'))
#     suite.addTest(unittest.makeSuite(MappingFieldTestCase, 'test'))
#     return suite
# 
# 
# if __name__ == '__main__':
#     unittest.main(defaultTest='suite')

