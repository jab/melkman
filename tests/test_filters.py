from helpers import dummy_news_item

def test_match_filter():
    from helpers import fresh_context
    from melk.util.dibject import Dibject, dibjectify
    from melkman.filters import NewsItemFilterFactory

    ctx = fresh_context()
    filter_factory = NewsItemFilterFactory(ctx.component_manager)
    
    cfgs = []
    cfgs.append({'field': 'foo.bar', 
                 'match_type': 'exact',
                 'values': ['apple', 'banana']})
    cfgs.append({'field': 'foo.bar', 
                 'values': ['apple', 'banana']})

    for cfg in cfgs:
        filt = filter_factory.create_filter('match_field', cfg)
        assert filt(dummy_news_item({'details': {'foo': {'bar': 'apple'}}}))
        assert filt(dummy_news_item({'details': {'foo': {'bar': 'banana'}}}))
        assert not filt(dummy_news_item({'details': {'foo': {'bar': 'applehead'}}}))
        assert not filt(dummy_news_item({'details': {'foo': {'bar': {'quux': 'zoo'}}}}))
        assert not filt(dummy_news_item({'details': {'foo': 'bar'}}))

    cfg = {'field': 'foo.bar',
           'match_type': 'substring',
           'values': ['ap', 'ban']}
    filt = filter_factory.create_filter('match_field', cfg)
    assert filt(dummy_news_item({'details': {'foo': {'bar': 'crabapple'}}}))
    assert filt(dummy_news_item({'details': {'foo': {'bar': 'nanbana'}}}))
    assert not filt(dummy_news_item({'details': {'foo': {'bar': 'cran'}}}))


    cfg = {'field': 'foo.bar',
           'match_type': 'regex',
           'values': ['^\d+$', '^foo.*bar$']}
    filt = filter_factory.create_filter('match_field', cfg)

    assert filt(dummy_news_item({'details': {'foo': {'bar': '123'}}}))
    assert filt(dummy_news_item({'details': {'foo': {'bar': 'fooqqqbar'}}}))
    assert not filt(dummy_news_item({'details': {'foo': {'bar': '123foo'}}}))
    

def test_or_filter():
    from helpers import fresh_context
    from melk.util.dibject import Dibject, dibjectify
    from melkman.filters import NewsItemFilterFactory

    ctx = fresh_context()
    filter_factory = NewsItemFilterFactory(ctx.component_manager)

    cfg = {
        'filters': 
        [{'op': 'match_field',
         'config': {'field': 'foo',
                    'match_type': 'substring',
                    'values': ['ap', 'ban']}},
        {'op': 'match_field',
         'config': {'field': 'foo',
                    'match_type': 'regex',
                    'values': ['^\d+$', '^foo.*bar$']}}]}
                    
    filt = filter_factory.create_filter('or', cfg)
    assert filt(dummy_news_item({'details': {'foo': 'crabapple'}}))
    assert filt(dummy_news_item({'details': {'foo': '1234'}}))
    assert not filt(dummy_news_item({'details': {'foo': 'abc'}}))

    cfg = {'filters': []}
    filt = filter_factory.create_filter('or', cfg)
    assert not filt(dummy_news_item({}))

def test_and_filter():
    from helpers import fresh_context
    from melk.util.dibject import Dibject, dibjectify
    from melkman.filters import NewsItemFilterFactory

    ctx = fresh_context()
    filter_factory = NewsItemFilterFactory(ctx.component_manager)

    cfg = {
        'filters': 
        [{'op': 'match_field',
         'config': {'field': 'foo',
                    'match_type': 'substring',
                    'values': ['ap', 'ban']}},
        {'op': 'match_field',
         'config': {'field': 'bar',
                    'match_type': 'regex',
                    'values': ['^\d+$', '^foo.*bar$']}}]}
                    
    filt = filter_factory.create_filter('and', cfg)
    assert filt(dummy_news_item({'details': {'foo': 'crabapple', 'bar': '123'}}))
    assert not filt(dummy_news_item({'details': {'foo': 'crabapple', 'bar': 'abc'}}))
    assert not filt(dummy_news_item({'details': {'foo': 'abc', 'bar': '123'}}))
    assert not filt(dummy_news_item({'details': {'foo': 'abc', 'bar': 'abc'}}))

    cfg = {'filters': []}
    filt = filter_factory.create_filter('and', cfg)
    assert not filt(dummy_news_item({}))
    
def test_filter_chain():
    from helpers import fresh_context
    from melk.util.dibject import Dibject, dibjectify
    from melkman.filters import NewsItemFilterFactory, ACCEPT_ITEM, REJECT_ITEM

    ctx = fresh_context()
    filter_factory = NewsItemFilterFactory(ctx.component_manager)
    
    chain = [
        {'op': 'match_author',
         'config': {'values': ['fred']},
         'action': 'reject'},
        {'op': 'and',
        'config': {
            'filters': [
                    {'op': 'match_author',
                    'config': {'values': ['barney']}},
                    {'op': 'match_field',
                     'config': {'field': 'foo',
                                'values': ['bar']}}]},
        'action': 'accept'},
        {'op': 'match_field',
         'config': {'field': 'foo',
                    'values': ['bar']},
         'action': 'reject'},
        {'op': 'match_all',
         'config': {},
         'action': 'accept'}
    ]
    chain = [dibjectify(x) for x in chain]
    chain = filter_factory.create_chain(chain)
    
    assert chain(dummy_news_item({'author': 'fred'})) == REJECT_ITEM
    assert chain(dummy_news_item({})) == ACCEPT_ITEM
    assert chain(dummy_news_item({'details': {'foo': 'bar'}})) == REJECT_ITEM
    assert chain(dummy_news_item({'author': 'barney', 'details': {'foo': 'bar'}})) == ACCEPT_ITEM
    
    
def test_filter_plugin():
    from giblets import Component, implements
    from helpers import fresh_context
    from melkman.filters import NewsItemFilterFactory, INewsItemFilterFactory

    ctx = fresh_context()
    filter_factory = NewsItemFilterFactory(ctx.component_manager)

    def dummy(item):
        return False

    class FilterMaker(Component):
        implements(INewsItemFilterFactory)
        
        def handles_type(self, filter_type):
            if filter_type in ['foo_filt', 'bar_filt']:
                return True
            return False
            
        def create_filter(self, filter_type, config):
            if filter_type in ['foo_filt', 'bar_filt']:
                return dummy
                
    assert filter_factory.create_filter('foo_filt', {}) is not None
    assert filter_factory.create_filter('bar_filt', {}) is not None
    assert filter_factory.create_filter('quux_filt', {}) is None
    
def test_author_filter():
    from helpers import fresh_context
    from melk.util.dibject import Dibject
    from melkman.filters import NewsItemFilterFactory

    ctx = fresh_context()
    filter_factory = NewsItemFilterFactory(ctx.component_manager)

    cfg = {'values': ['Fred', 'Barney']}

    filt = filter_factory.create_filter('match_author', cfg)

    assert filt is not None
    assert filt(dummy_news_item({'author': 'fred'}))
    assert filt(dummy_news_item({'author': 'frED'}))
    assert filt(dummy_news_item({'author': 'barney'}))
    assert filt(dummy_news_item({'author': 'bArnEy'}))
    assert not filt(dummy_news_item({'author': 'blurney'}))

def test_tag_filter():
    from helpers import fresh_context
    from melk.util.dibject import Dibject
    from melkman.filters import NewsItemFilterFactory

    ctx = fresh_context()
    filter_factory = NewsItemFilterFactory(ctx.component_manager)

    cfg = {'values': ['soup', 'nuts']}

    filt = filter_factory.create_filter('match_tag', cfg)

    assert filt is not None
    assert not filt(dummy_news_item({}))
    assert filt(dummy_news_item({'details': {'tags': [{'label': 'soup'}]}}))
    assert filt(dummy_news_item({'details': {'tags': [{'label': 'nuts'}]}}))
    assert filt(dummy_news_item({'details': {'tags': [{'term': 'soup'}]}}))
    assert filt(dummy_news_item({'details': {'tags': [{'term': 'nuts'}]}}))
    assert filt(dummy_news_item({'details': {'tags': [{'label': 'fruit'}, {'label': 'soup'}]}}))
    assert not filt(dummy_news_item({'details': {'tags': [{'label': 'fruit'}]}}))

def test_title_filter():
    from helpers import fresh_context
    from melk.util.dibject import Dibject
    from melkman.filters import NewsItemFilterFactory

    ctx = fresh_context()
    filter_factory = NewsItemFilterFactory(ctx.component_manager)

    cfg = {'values': ['Fred', 'Barney']}

    filt = filter_factory.create_filter('match_title', cfg)

    assert filt is not None
    assert filt(dummy_news_item({'title': 'fred'}))
    assert filt(dummy_news_item({'title': 'frED'}))
    assert filt(dummy_news_item({'title': 'barney'}))
    assert filt(dummy_news_item({'title': 'bArnEy'}))
    assert not filt(dummy_news_item({'title': 'blurney'}))

def test_source_filter():
    from helpers import fresh_context
    from melk.util.dibject import Dibject
    from melkman.filters import NewsItemFilterFactory

    ctx = fresh_context()
    filter_factory = NewsItemFilterFactory(ctx.component_manager)

    cfg = {'values': ['http://example.org/Feed1', 'http://example.org/Feed2']}

    filt = filter_factory.create_filter('match_source', cfg)

    assert filt is not None
    assert filt(dummy_news_item({'source_url': 'http://example.org/Feed1'}))
    assert filt(dummy_news_item({'source_url': 'http://example.org/Feed2'}))
    assert filt(dummy_news_item({'source_url': 'http://example.org:80/FeEd1'}))
    assert not filt(dummy_news_item({'source_url': 'http://foo.example.org/Feed2'}))

    cfg = {'values': ['^http://(.*\.)?example.org/Feed\d$'],
           'match_type': 'regex'}
    filt = filter_factory.create_filter('match_source', cfg)
    assert filt is not None
    for i in range(5):
        assert filt(dummy_news_item({'source_url': 'http://example.org/Feed%d' % i}))
    assert not filt(dummy_news_item({'source_url': 'http://example.org/FeedJ'}))

    for i in range(2):
        for j in range(2):
            assert filt(dummy_news_item({'source_url': 'http://server%d.example.org/Feed%d' % (j,i)}))

def test_content_filter():
    from helpers import fresh_context
    from melk.util.dibject import Dibject
    from melkman.filters import NewsItemFilterFactory

    ctx = fresh_context()
    filter_factory = NewsItemFilterFactory(ctx.component_manager)

    
    test_content = """
    <div>
        Here they are
        <ul>
            <li> Fred </li>
            <li> <a href="http://flintstones.net/br">Bar<i>ney</i></a>... </li>
        </ul>
    </div>
    """
    content_field = {
        'type': 'text/html',
        'value': test_content
    }

    cfg = {'values': ['Fred'],
           'match_type': 'substring'}

    filt = filter_factory.create_filter('match_content', cfg)

    
    assert not filt(dummy_news_item({}))

    assert filt(dummy_news_item({'details': {'content': [content_field]}}))
    assert filt(dummy_news_item({'details': {'summary': '', 'summary_detail': content_field}}))

    cfg = {'values': ['Barney'],
           'match_type': 'substring'}
    filt = filter_factory.create_filter('match_content', cfg)
    assert filt(dummy_news_item({'details': {'content': [content_field]}}))
    assert filt(dummy_news_item({'details': {'summary': '', 'summary_detail': content_field}}))

    cfg = {'values': ['Wilma'],
           'match_type': 'substring'}
    filt = filter_factory.create_filter('match_content', cfg)
    assert not filt(dummy_news_item({'details': {'content': [content_field]}}))
    assert not filt(dummy_news_item({'details': {'summary': '', 'summary_detail': content_field}}))


