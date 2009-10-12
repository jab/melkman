# Copyright (C) 2009 The Open Planning Project
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the
# Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor,
# Boston, MA  02110-1301
# USA

import cgi
from copy import deepcopy
from datetime import datetime
import feedparser
import re

from melk.util.dibject import Dibject, dibjectify
from melk.util.hash import melk_id
from melk.util.urlnorm import canonical_url

#############################
# feed parsing
#############################

class InvalidFeedError(Exception):
    pass

def parse_feed(content, feed_url):
    fake_headers = {
        'content-location': feed_url,
        'content-type': 'text/xml; charset=utf-8',
    }
    ff = feedparser.parse(content, header_defaults=fake_headers)

    # make a clean copy composed of built-in types
    ff = dibjectify(ff)

    if ff is None or not 'feed' in ff:
        raise InvalidFeedError()

    #
    # perform some cleanup...
    #
    source_url = canonical_url(feed_url)

    # make sure the feed has an id...
    if not 'id' in ff.feed:
        ff.feed['id'] = source_url.lower()
    
    # make sure the feed has a self referential link
    has_self_ref = False
    ff.feed.setdefault('links', [])
    for link in ff.feed.links:
        if link.rel == 'self':
            has_self_ref = True
            break
    if not has_self_ref:
        ff.feed.links.append(Dibject(rel='self', href=source_url, title=''))

    # create a structure holding the appropriate source information 
    # from the feed.  This will be copied into each entry.
    source_info = Dibject()
    for k in ['id', 'title', 'title_detail', 'link', 'links', 'icon']:
        try:
            source_info[k] = deepcopy(ff.feed[k])
        except KeyError:
            pass

    out_entries = []
    for e in ff.get('entries', []):
        
        # make sure it has an id
        eid = e.get('id', None)
        if eid is None:
            eid = find_best_entry_id(entry)
            if eid is None:
                # throw this entry out, it has no 
                # id, title, summary or content
                # that is recognizable...
                continue
            e['id'] = eid

        # assign a guid based on the id given and the source url
        e['melk_id'] = melk_id(eid, source_url.lower())

        # build a 'source' entry for each entry which points
        # back to this feed. if there is already a source
        # specified in the entry, we move it aside to 
        # original_source.
        if 'source' in e:
            e['original_source'] = e.source
        
        e.source = deepcopy(source_info)
        out_entries.append(e)

    ff['entries'] = out_entries

    return ff

def find_best_entry_id(entry):
    if entry.has_key('id'):
        return entry['id']
    elif entry.has_key('link') and entry['link']:
        return entry['link']
    elif entry.has_key('title') and entry['title']:
        return (entry.title_detail.base + "/" +
                md5(entry['title']).hexdigest())
    elif entry.has_key('summary') and entry['summary']:
        return (entry['summary_detail']['base'] + "/" +
                md5(entry['summary']).hexdigest())
    elif entry.has_key("content") and entry['content']:
        return (entry['content'][0]['base'] + "/" + 
                md5(entry['content'][0]['value']).hexdigest())
    else:
        return None

def sleep_time(dtime):
    return dtime.timetuple()[0:6]

def wake_time(time_list):
    return datetime(*time_list[0:6])

def find_best_timestamp(thing, default=None):
    """
    return the latest timestamp specified as a datetime. 
    timestamps are returned in this preference order: 
    updated, published, created
    """
    ts = thing.get('updated_parsed', None)
    if ts is None:
        ts = thing.get('published_parsed', None)
    if ts is None:
        ts = thing.get('created_parsed', None)

    if ts is not None:
        return datetime(*ts[0:6])
    else:
        return default

def find_best_permalink(entry, default=''):
    links = entry.get('links', [])
    for link in links:
        rel = link.get('rel', '')
        href = link.get('href', '')
        if href and rel and rel == 'alternate':
            return href
    return default
    
def find_author_name(entry, default=''):
    if 'author_detail' in entry and 'name' in entry.author_detail and entry.author_detail.name:
        return entry.author_detail.name
    elif 'author' in entry and entry.author:
        return cgi.escape(entry.author)
    else:
        return default

def find_source_url(e, default=''):
    links = e.get('links', [])
    for link in links:
        rel = link.get('rel', '')
        href = link.get('href', '')
        if href and rel and rel == 'self':
            return href
    return default
    

HTML_TYPES = ['text/html', 'application/xhtml+xml']
def as_html(content):
    if content is None:
        return ''
    if content.type in HTML_TYPES:
        return content.value
    else:
        return cgi.escape(content.value)
    
def item_trace(e):
    """
    produces a compact subset of the information in a
    news item suitable for summary or preview.  html is 
    stripped and long fields are trimmed.
    """
    trace = Dibject()
    trace.timestamp = find_best_timestamp(e) or datetime.utcnow()

    trace.title = stripped_content(e.get('title_detail', None), 128)
    trace.author = trimmed(find_author_name(e), 128)
    trace.link = find_best_permalink(e)
    
    source = e.get('source', {})
    trace.source_url = find_source_url(source)
    trace.source_title = stripped_content(source.get('title_detail', None), 128)

    content = e.get('content', [None])[0]
    if content is None:
        content = e.get('summary_detail', None)
    trace.summary = stripped_content(content, 256)

    return trace

def stripped_content(content, maxlen=None):
    """
    return the content node given stripped of
    html tags and length limited as specified.
    
    if the content is longer than maxlen, the 
    string is truncated and the final three
    characters of the truncated string are
    replaced with ...
    """
    if content is None:
        return ''

    if content.type in HTML_TYPES:
        try:
            outstr = strip_tags(content.value)
        except:
            # didn't parse, just escape it (gigo)... 
            outstr = cgi.escape(content.value)
    else:
        outstr = cgi.escape(content.value)

    if maxlen:
        return trimmed(outstr, maxlen)
    else:
        return outstr

def trimmed(text, maxlen):
    if text is None:
        return ''
    if len(text) > maxlen:
        return text[0:maxlen-3] + '...'
    else:
        return text

import HTMLParser
class MLStripper(HTMLParser.HTMLParser):
    def __init__(self):
        self.reset()
        self._text = []
    def handle_data(self, d):
        self._text.append(d)
    def handle_charref(self, name):
        self._text.append('&#%s;' % name)
    def handle_entityref(self, name):
        self._text.append('&%s;' % name)

    @property
    def text(self):
        text = ''
        for chunk in self._text:
            if not chunk:
                continue
            if chunk.startswith(' '):
                text += re.sub('^\s+', ' ', chunk)
            else:
                text += chunk
            text.strip()
        return text

def strip_tags(html):
    stripper = MLStripper()
    stripper.feed(html)
    return stripper.text
