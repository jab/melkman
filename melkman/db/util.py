from couchdb import ResourceConflict
from couchdb.schema import Document, Schema, Field
import random
import time
from melk.util.dibject import DibWrap
import logging

log = logging.getLogger(__name__)

class DocumentHelper(Document):
    
    def __init__(self, *args, **kw):
        Document.__init__(self, *args, **kw)
        self._context = None

    def set_context(self, context):
        self._context = context

    @classmethod
    def create(cls, context, *args, **kw):
        instance = cls(*args, **kw)
        instance.set_context(context)
        return instance

    @classmethod
    def from_doc(cls, doc, context):
        instance = cls.wrap(doc)
        instance.set_context(context)
        return instance

    @classmethod
    def get(cls, id, ctx):
        doc = ctx.db.get(id)
        if doc is None:
            return None
        instance = cls.wrap(doc)
        instance.set_context(ctx)
        return instance

    @classmethod
    def get_by_ids(cls, ids, ctx):
        for row in ctx.db.view('_all_docs', keys=ids, include_docs=True):
            instance = cls.from_doc(row.doc, ctx)
            instance.set_context(ctx)
            yield instance

    def load(self, db, id):
        raise NotImplementedError("Use get(id, context) instead.")

    def save(self):
        Document.store(self, self._context.db)

    def store(self, db):
        raise NotImplementedError("Use save(context) instead.")

    def delete(self):
        del self._context.db[self.id]
    
SUPPORTED_BATCH_QUERY_ARGS = set(['startkey', 'endkey', 'skip', 'descending', 'include_docs', 'limit'])
def batched_view_iter(db, view, batch_size, **kw):
    """
    Simple iteration of a view by pulling out batches.
    """
    for key in kw.keys():
        if not key in SUPPORTED_BATCH_QUERY_ARGS:
            raise ValueError("Unsupported query arg: %s" % key)

    if 'limit' in kw:
        max_results = kw.get('limit')
        del kw['limit']
    else:
        max_results = None

    args = dict(kw)
    args['limit'] = batch_size
    start = kw.get('startkey', None)
    skip = kw.get('skip', 0)

    yielded_results = 0
    done = False

    while(not done):
        batch_count = 0

        if start is not None:
            args['startkey'] = start

        if skip > 0:
            args['skip'] = skip

        for r in view(db, **args):
            batch_count += 1

            if r.key == start:
                skip += 1
            else:
                start = r.key
                skip = 1

            yield r

            yielded_results += 1
            if max_results is not None and yielded_results >= max_results:
                done = True
                break

        if batch_count != batch_size:
            done = True

MAX_EXECUTIONS = 6
def backoff_save(saver, pass_count=False):
    """
    This executes 'saver'.  If a ResourceConflict is detected, 
    it is reexecuted with a random exponential backoff.
    """

    backoff = .10;
    executions = 1
    while(True):
        try:
            if pass_count:
                return saver(executions)
            else:
                return saver()
        except ResourceConflict:
            if executions >= MAX_EXECUTIONS:
                log.warn("Too many conflicts! giving up")
                raise
            else:
                if executions >= 3:
                    log.warn("Conflict #%d! retrying in %s seconds" % (executions, backoff))
                else: 
                    log.debug("Conflict #%d! retrying in %s seconds" % (executions, backoff))
                time.sleep(backoff)
                backoff *= random.uniform(1.5, 2)
                executions += 1

def delete_all_in_view(db, view):
    query = {
        'limit': 1000,
        'include_docs': False
    }
    while True:
        batch = [r.id for r in view(db, **query)]
        if len(batch) == 0:
            break

        # XXX this could be batched, but requires pulling out the 
        # doc or doing a head operation... 
        for doc_id in batch:
            del db[doc_id]
            


class DibjectField(Field):
    """
    Field type that is similar to DictField but allows attribute access.
    
    >>> from couchdb import Server
    >>> server = Server('http://localhost:5984/')
    >>> db = server.create('python-tests')

    >>> class Post(Document):
    ...     extra = DibjectField()

    >>> post = Post(
    ...     extra=dict(foo='bar'),
    ... )
    >>> post.store(db) #doctest: +ELLIPSIS
    <Post ...>
    >>> post = Post.load(db, post.id)
    >>> post.extra
    {'foo': 'bar'}
    >>> post.extra.foo
    'bar'
    
    >>> del server['python-tests']
    """
    def __init__(self, name=None, default=None):
        Field.__init__(self, name=name, default=default or {})

    def _to_python(self, value):
        return DibWrap(value)

    def _to_json(self, value):
        return value


class MappingField(Field):
    """Field type for mappings between strings and a Field type. 

    A different Field type for keys may be specified, but it
    must serialize to a string in order to be a valid key in
    a json dictionary. 

    N.B. Keys with the same json representation according to 
    the key schema are considered equal -- alternate key types
    are provided as a convenient and consistent way to map to and from 
    strings only.  If you're nervous about this, use strings as your keys
    instead.
    >>> from datetime import date
    >>> from couchdb import Server
    >>> from couchdb.schema import DateField, DictField, ListField, TextField, Schema
    >>> server = Server('http://localhost:5984/')
    >>> db = server.create('python-tests')

    >>> class Book(Schema):
    ...     title = TextField()
    ...     authors = ListField(TextField())
    ...     publish_date = DateField()
    >>> class Library(Document):
    ...     books = MappingField(DictField(Book))
    >>> couch_book_info = {'title': 'CouchDB: The Definitive Guide', 
    ...                    'authors': ['J. Anderson', 'Jan Lehnardt', 'Noah Slater'],
    ...                    'publish_date': date(2009, 11, 15)}
    >>> isbn = 'URN:ISBN:978-059-61-5589-6'
    >>> library = Library()
    >>> library.books[isbn] = couch_book_info
    >>> len(library.books)
    1
    >>> library.store(db) # doctest: +ELLIPSIS
    <Library ...>
    >>> isbn in library.books
    True
    >>> book = library.books[isbn]
    >>> book.title
    u'CouchDB: The Definitive Guide'
    >>> book.publish_date
    datetime.date(2009, 11, 15)
    >>> 'Jan Lehnardt' in book.authors
    True

    >>> del server['python-tests']
    """

    def __init__(self, field, key_field=Field, name=None, default=None):
        Field.__init__(self, name=name, default=default or {})

        def mkfield(f):
            if type(f) is type:
                if issubclass(f, Field):
                    return f()
                elif issubclass(f, Schema):
                    return DictField(f)
            return f

        self.field = mkfield(field)
        self.key_field = mkfield(key_field)

    def _to_python(self, value):
        return self.Proxy(value, self.field, self.key_field)

    def _to_json(self, value):
        return dict([(self.key_field._to_json(k), 
                      self.field._to_json(v)) for (k, v) in value.items()])


    class Proxy(dict):

        def __init__(self, wdict, field, key_field):
            self.dict = wdict
            self.field = field
            self.key_field = key_field

        def __cmp__(self, other):
            return self.dict.__cmp__(other)

        def __contains__(self, item):
            try:
                jsonk = self.key_field._to_json(item)
            except:
                return False

            return self.dict.__contains__(jsonk)


        # def __delattr__(self, name):

        def __delitem__(self, key):
            try:
                jsonk = self.key_field._to_json(key)
            except ValueError:
                raise KeyError(key)

            return self.dict.__delitem__(jsonk)

        def __eq__(self, other):
            return self.dict.__eq__(other)

        def __ge__(self, other):
            return self.dict.__ge__(other)

        # def __getattribute__(self, name):

        def __getitem__(self, key):
            try:
                jsonk = self.key_field._to_json(key)
            except:
                raise KeyError(key)

            return self.field._to_python(self.dict.__getitem__(jsonk))

        def __gt__(self, other):
            return self.dict.__gt__(other)

        # def __hash__(self):

        def __iter__(self):
            for k in self.dict.__iter__():
                yield self.key_field._to_python(k)

        def __le__(self, other):
            return self.dict.__le__(other)

        def __len__(self):
            return self.dict.__len__()

        def __lt__(self, other):
            return self.dict.__lt__(other)

        def __ne__(self, other):
            return self.dict.__ne__(other)

        # def __reduce__(self):
        # def __reduce_ex__(self):

        def __repr__(self):
            return self.dict.__repr__()

        # def __setattr__(self, name, value)

        def __setitem__(self, key, value):
            self.dict.__setitem__(self.key_field._to_json(key), 
                                  self.field._to_json(value))

        def __str__(self):
            return str(self.dict)

        def __unicode__(self):
            return unicode(self.dict)

        def clear(self):
            return self.dict.clear()

        # def copy():

        @classmethod
        def fromkeys(cls, seq, value=None):
            raise NotImplementedError('Cannot initialize class of type %s without Field type' % cls.__name__)

        def get(self, key, default=None):
            try:
                return self[key]
            except KeyError:
                return default

        def has_key(self, key):
            try:
                return self.dict.has_key(self.key_field._to_json(key))
            except ValueError:
                return False

        def items(self):
            return list(self.iteritems())

        def iteritems(self):
            for (k, v) in self.dict.iteritems():
                yield (self.key_field._to_python(k),
                       self.field._to_python(v))

        def iterkeys(self):
            for k in self.dict.iterkeys():
                yield self.key_field._to_python(k)

        def itervalues(self):
            for v in self.dict.itervalues():
                yield self.field._to_python(v)

        def keys(self):
            return list(self.iterkeys())

        def pop(self, *args):
            if len(args) == 0:
                raise TypeError('pop expected at least 1 arguments, got 0')
            if len(args) > 2:
                raise TypeError('pop expected at most 2 arguments, got %d' % len(args))


            try:
                try:
                    popkey = self.key_field._to_json(args[0])
                except ValueError:
                    raise KeyError(args[0])
                return self.field._to_python(self.dict.pop(popkey))
            except KeyError:
                if len(args) == 1:
                    raise
                else:
                    return args[1]

        def popitem(self):
            k, v = self.dict.popitem()
            return (self.key_field._to_python(k), self.field._to_python(v))

        def setdefault(self, key, default=None):
            try:
                jsonk = self.key_field._to_json(key)
                default_json = self.field._to_json(default)
            except:
                raise ValueError

            v = self.dict.setdefault(jsonk, default_json)

            return self.field._to_python(v)

        def update(self, other):
            for (k, v) in other.iteritems():
                self[k] = v

        def values(self):
            return [self.field._to_python(v) for v in self.dict.values()]

if __name__ == '__main__':
    import doctest
    doctest.testmod()
