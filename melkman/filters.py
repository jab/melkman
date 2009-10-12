from giblets import Component, ExtensionInterface, ExtensionPoint, implements
from melk.util.typecheck import is_dicty, is_listy
from melk.util.urlnorm import canonical_url
from melkman.context import IContextConfigurable
from melkman.parse import stripped_content
import logging
import re
import traceback

__all__ = ['INewsItemFilterFactory', 'NewsItemFilterFactory', 
           'ACCEPT_ITEM', 'REJECT_ITEM', 'BaseMatchFilter', 
           'SimpleFilterPlugin']

log = logging.getLogger(__name__)


class INewsItemFilterFactory(ExtensionInterface):

    def handles_type(filter_type):
        """
        returns True if this factory can produce a filter
        of the type specified.
        """

    def create_filter(filter_type, config):
        """
        produce a "filter" of the type specified 
        given the configuration for the filter.
        
        the filter object is a callable accepting 
        a NewsItemRef and the current context and 
        producing a boolean value representing 
        whether the filter matches the item given.
        """

class NewsItemFilterFactory(Component):
    implements(IContextConfigurable)
    
    filter_types = ExtensionPoint(INewsItemFilterFactory)
    
    def create_filter(self, filter_type, config, negative=False):
        for factory in self.filter_types:
            if factory.handles_type(filter_type):
                try:
                    filt = factory.create_filter(filter_type, config)
                    if negative:
                        filt = Negation(filt)                    
                    return filt
                except:
                    log.warn("Error occurred created filter of type %s: %s" % 
                             (filter_type, traceback.format_exc()))
                    # and keep trying...

        log.warn("Ignoring unsupported filter type: %s" % filter_type)
        return None

    def create_chain(self, filter_chain):
        chain = FilterChain()
        for cfg in filter_chain:
            filt = self.create_filter(cfg.op, cfg.get('config', {}), cfg.get('negative', False))
            if filt is None:
                continue

            chain.append(filt, cfg.action)
        return chain

    def set_context(self, context):
        self.context = context

ACCEPT_ITEM = 'accept'
REJECT_ITEM = 'reject'
class FilterChain(object):
    
    def __init__(self):
        self._default = ACCEPT_ITEM
        self._filters = []

    def __call__(self, news_item):
        for filt, action in self._filters:
            if filt(news_item):
                return action
        return self._default

    def append(self, filt, action):
        self._filters.append((filt, action))

    def set_default_action(self, action):
        self._default = action

class SimpleFilterPlugin(Component):
    implements(INewsItemFilterFactory, IContextConfigurable)
    abstract = True

    def handles_type(self, filter_type):
        return self.FilterType.filter_type == filter_type

    def create_filter(self, filter_type, config):
        return self.FilterType(config, self.context)
    
    def set_context(self, context):
        self.context = context

class Negation(object):
    def __init__(self, filter):
        self.filter = filter
        
    def __call__(self, *args, **kw):
        return not self.filter(*args, **kw)

class MatchNoneFilter(object):
    filter_type = 'match_none'

    def __init__(self, *args, **kw):
        pass
        
    def __call__(self, *args, **kw):
        return False

class MatchNoneFilterPlugin(SimpleFilterPlugin):
    FilterType = MatchNoneFilter

class MatchAllFilter(object):
    filter_type = 'match_all'

    def __init__(self, *args, **kw):
        pass

    def __call__(self, *args, **kw):
        return True

class MatchAllFilterPlugin(SimpleFilterPlugin):
    FilterType = MatchAllFilter

class MultiFilter(object):
    def __init__(self, config, context):
        self._filters = []
        self.context = context

        factory = NewsItemFilterFactory(context.component_manager)
        for cfg in config.get('filters', []):
            filter_type = cfg.get('op')
            filter_cfg = cfg.get('config', {})
            filter_neg = cfg.get('negative', False)
            filt = factory.create_filter(filter_type, filter_cfg, filter_neg)
            if filt is not None:
                self._filters.append(filt)

class OrFilter(MultiFilter):
    filter_type = 'or'

    def __call__(self, news_item):
        for filt in self._filters:
            if filt(news_item):
                return True
        return False

class OrFilterPlugin(SimpleFilterPlugin):
    FilterType = OrFilter

    
class AndFilter(MultiFilter):
    filter_type = 'and'
    
    def __call__(self, news_item):
        if len(self._filters) == 0:
            return False

        for filt in self._filters:
            if not filt(news_item):
                return False
        return True

class AndFilterPlugin(SimpleFilterPlugin):
    FilterType = AndFilter

class BaseMatchFilter(object):
    
    def __init__(self, config, context):
        self.config = config
        self.context = context
        
        pat_str = ''
        match_type = config.get('match_type', 'exact')
        def rescape_char(match):
            return '\%s' % match.group(0)
        def rescape(val):
            return re.sub(r'[\.\^\$\*\+\?\{\}\\\[\]\|\(\)]', rescape_char, val)
        
        if match_type == 'exact':
            for val in config.get('values', []):
                pat_str += '^%s$|' % rescape(val)
            if pat_str.endswith('|'):
                pat_str = pat_str[0:-1]
        elif match_type == 'substring':
            for val in config.get('values', []):
                pat_str += '^.*?%s.*?$|' % rescape(val)
            if pat_str.endswith('|'):
                pat_str = pat_str[0:-1]
        elif match_type == 'regex':
            for val in config.get('values', []):
                pat_str += '%s|' % val
            if pat_str.endswith('|'):
                pat_str = pat_str[0:-1]
        else:
            raise ValueError("unknown match type: %s" % match_type)
        
        if pat_str:
            flags = re.M|re.S
            if config.get('case_sensitive', False) == False:
                flags |= re.I
            self.pat = re.compile(pat_str, flags)
        else:
            self.pat = None
            
    def _match(self, val):
        if not isinstance(val, basestring):
            return False

        if self.pat is None:
            return False
        else:
            return self.pat.match(val) is not None

class AuthorFilter(BaseMatchFilter):

    filter_type = 'match_author'

    def __call__(self, news_item):
        item_author = self._norm_author(news_item.get('author', ''))
        return self._match(item_author)

    def _norm_author(self, author):
        return author.lower().strip()

class AuthorFilterPlugin(SimpleFilterPlugin):
    FilterType = AuthorFilter


class SourceFilter(BaseMatchFilter):
    filter_type = 'match_source'
    
    def __call__(self, news_item):
        return self._match(canonical_url(news_item.get('source_url', '')))

class SourceFilterPlugin(SimpleFilterPlugin):
    FilterType = SourceFilter


class TitleFilter(BaseMatchFilter):

    filter_type = 'match_title'

    def __call__(self, news_item):
        item_title = news_item.get('title', '')
        return self._match(item_title)

class TitleFilterPlugin(SimpleFilterPlugin):
    FilterType = TitleFilter

class TagFilter(BaseMatchFilter):
    filter_type = 'match_tag'
    
    def __call__(self, news_item):
        for t in self._get_tags(news_item):
            if self._match(t):
                return True
        return False

    def _get_tags(self, news_item):
        news_item = news_item.load_full_item(self.context.db)
        item_details = news_item.details
        tags = set()

        for t in item_details.get('tags', []):
            # XXX shady?
            if 'label' in t and t.label:
                tag = self._normalize_tag(t.label)
                if tag is not None:
                    tags.add(tag)
            elif 'term' in t and t.term:
                tag = self._normalize_tag(t.term) 
                if tag is not None:
                    tags.add(tag)

        return tags

    def _normalize_tag(self, tag):
        if tag is not None:
            nt = tag.lower().strip()
            if nt:
                return nt
        return None

class TagFilterPlugin(SimpleFilterPlugin):
    FilterType = TagFilter


class ContentFilter(BaseMatchFilter):

    filter_type = 'match_content'

    def __call__(self, news_item):
        news_item = news_item.load_full_item(self.context.db)
        e = news_item.details
        content = e.get('content', [None])[0]
        if content is None:
            content = e.get('summary_detail', None)

        # strip tags if we can...
        content = stripped_content(content)
        return self._match(content)

class ContentFilterPlugin(SimpleFilterPlugin):
    FilterType = ContentFilter


class MatchFieldFilter(BaseMatchFilter):
    
    filter_type = 'match_field'
    
    def __call__(self, news_item):
        news_item = news_item.load_full_item(self.context.db)
        path = self.config.get('field', None)
        if path is None:
            return False

        path = path.split('.')
        val = news_item.details
        for node in path:
            if is_dicty(val):
                val = val.get(node, None)
            else:
                return False
            
            if val is None:
                break
        if val is None:
            return False
    
        return self._match(val)

class MatchFieldFilterPlugin(SimpleFilterPlugin):
    FilterType = MatchFieldFilter
