from carrot.messaging import Publisher, Consumer
from collections import defaultdict
from couchdb.design import ViewDefinition
from couchdb.schema import *
from datetime import datetime
from eventlet.proc import spawn
from giblets import Component, implements
import logging
from melk.util.hash import melk_id
import re
import traceback
from tweetstream import TweetStream
import urllib

from melkman.context import IRunDuringBootstrap
from melkman.db.bucket import NewsBucket, NewsItem, NewsItemRef, immediate_add
from melkman.green import RWLock

__all__ = ['Tweet', 'TweetRef', 'TweetBucket', 
           'TweetPublisher', 'TweetConsumer', 
           'TwitterStreamConnection', 
           'tweet_trace']

log = logging.getLogger(__name__)

def tweet_trace(tweet):
    tt = {}
    tt['item_id'] = 'tweet:%s' % tweet.get('id')
    tt['title'] = '' # XXX
    tt['author'] = '@%s' % tweet.get('user', {}).get('screen_name')
    tt['summary'] = tweet.get('text', '')
    tt['timestamp'] = tweet.get('created_at')
    tt['source_title'] = 'Twitter'
    # tt['source_url'] = XXX
    return tt

class Tweet(NewsItem):
    document_types = ListField(TextField(), default=['NewsItem', 'Tweet'])

    @classmethod
    def create_from_tweet(cls, tweet, context):
        tt = tweet_trace(tweet)
        tt['details'] = tweet
        tid = tt.pop('item_id')
        return cls.create(context, tid, **tt)
        #return 'tweet:%s' % tweet_id # XXX

class TweetRef(NewsItemRef):
    document_types = ListField(TextField(), default=['NewsItemRef', 'TweetRef'])

    def load_full_item(self):
        return Tweet.get(self.item_id, self._context)

class TweetBucket(NewsBucket):

    document_types = ListField(TextField(), default=['NewsBucket', 'TweetBucket'])

    # these should not be changed, they are the characteristic
    # of this bucket -- only set during initialization
    filter_type = TextField()
    filter_value = TextField()

    def save(self):
        is_new = self.rev is None
        NewsBucket.save(self)
        if is_new:
            twitter_filters_changed(self._context)

    def delete(self):
        NewsBucket.delete(self)
        twitter_filters_changed(self._context)

    @classmethod
    def create_from_constraint(cls, filter_type, value, ctx):
        bid = cls.dbid(filter_type, value)
        instance = cls.create(ctx, bid)
        instance.filter_type = filter_type
        instance.filter_value = value
        return instance

    @classmethod
    def create_from_topic(cls, topic, ctx):
        return cls.create_from_constraint('track', topic, ctx)

    @classmethod
    def create_from_follow(cls, userid, ctx):
        return cls.create_from_constraint('follow', userid, ctx)

    @classmethod
    def get_by_constraint(cls, filter_type, value, ctx):
        bid = cls.dbid(filter_type, value)
        return cls.get(bid, ctx)
        
    @classmethod
    def get_by_topic(cls, topic, ctx):
        return cls.get_by_constraint('track', topic, ctx)

    @classmethod
    def get_by_follow(cls, userid, ctx):
        return cls.get_by_constraint('follow', userid, ctx)

    @classmethod
    def dbid(cls, filter_type, val):
        return melk_id('tweets:%s/%s' % (filter_type, val))

view_all_twitter_filters = ViewDefinition('twitter', 'all_twitter_filters', 
'''
function(doc) {
    if (doc.document_types && doc.document_types.indexOf("TweetBucket") != -1) {
        emit([doc.filter_type, doc.filter_value], null);
    }
}
''')

#############################

class MelkmanTweetStream(TweetStream):
    
    SUPPORTED_FILTERS = ['follow', 'track']

    def __init__(self, context):
        self.context = context
        username = context.config.twitter.username
        password = context.config.twitter.password
        filter_url = context.config.twitter.filter_url
        TweetStream.__init__(self, username=username,
                             password=password,
                             url=filter_url)

    def _get_post_data(self):
        filters = defaultdict(list)
        for r in view_all_twitter_filters(self.context.db):
            ftype, val = r.key
            filters[ftype].append(val)
        
        post_data = {}
        for ftype in self.SUPPORTED_FILTERS:
            vals = filters.get(ftype, [])
            if len(vals):
                post_data[ftype] = ','.join(vals)

        return urllib.urlencode(post_data)

TWEET_EXCHANGE = 'melkman.direct'
GOT_TWEET_KEY = 'tweet_received'
TWEET_SORTER_QUEUE = 'tweet_sorter'

class TweetPublisher(Publisher):
    exchange = TWEET_EXCHANGE
    routing_key = GOT_TWEET_KEY
    delivery_mode = 2
    mandatory = True

class TweetConsumer(Consumer):
    exchange = TWEET_EXCHANGE
    routing_key = GOT_TWEET_KEY
    durable = True

TWITTER_CHANNEL = 'twitter'
FILTERS_CHANGED = 'filters_changed'
def twitter_filters_changed(context):
    context.event_bus.send(TWITTER_CHANNEL, {'type': FILTERS_CHANGED})

class TwitterSetup(Component):
    implements(IRunDuringBootstrap)

    def bootstrap(self, context, purge=False):
        view_all_twitter_filters.sync(context.db)
        c = TweetSorterConsumer(context)
        c.close()

        if purge == True:
            log.info("Clearing twitter sorting queues...")
            cnx = context.broker
            backend = cnx.create_backend()
            backend.queue_purge(TWEET_SORTER_QUEUE)

        context.broker.close()

def received_tweet(tweet_data, context):
    publisher = TweetPublisher(context.broker)
    publisher.send(tweet_data)
    publisher.close()

class BasicSorter(object):
    def __init__(self, bucket_id):
        self.bucket_id = bucket_id
        self.bucket = None

    def matches(self, tweet):
        raise NotImplementedError

    def apply(self, tweet_data, item, context):
        if self.bucket is None:
            self.bucket = NewsBucket.get(self.bucket_id, context)
            if self.bucket is None:
                return

        if self.matches(tweet_data):
            immediate_add(self.bucket, item, context)

PUNC = re.compile('[^a-zA-Z0-9]+')
def no_punc(x):
    return re.sub(PUNC, ' ', x).strip()

class TrackSorter(BasicSorter):
    """
    From: http://apiwiki.twitter.com/Streaming-API-Documentation#track

    Terms are exact-matched, and also exact-matched ignoring punctuation. Phrases,
    keywords with spaces, are not supported. Keywords containing punctuation will
    only exact match tokens.

    Track examples: The keyword Twitter will match all public statuses with the
    following comma delimited tokens in their text field: TWITTER, twitter, "Twitter", 
    twitter., #twitter and @twitter. The following tokens will not be matched: 
    TwitterTracker and http://www.twitter.com,  The phrase, excluding quotes, 
    "hard alee" won't match anything.  The keyword "helm's-alee" will match helm's-alee
    but not #helm's-alee.
    
    >>> ts = TrackSorter('Twitter', 'test')
    >>> ts.matches({'text': 'I use teh TWITTER all day'})
    True
    >>> ts.matches({'text': 'I use teh twitter all day'})
    True
    >>> ts.matches({'text': 'I use teh "Twitter" all day'})
    True
    >>> ts.matches({'text': 'I use teh twitter. all day'})
    True
    >>> ts.matches({'text': 'I use teh #twitter all day'})
    True
    >>> ts.matches({'text': 'I use teh @twitter all day'})
    True
    >>> ts.matches({'text': 'I use teh TwitterTracker all day'})
    False
    >>> ts.matches({'text': 'I use teh http://www.twitter.com all day'})
    False
    
    >>> ts =  TrackSorter('hard alee', 'test')
    >>> ts.matches({'text': 'hard alee'})
    False
    >>> ts.matches({'text': 'hardalee'})
    False
    >>> ts.matches({'text': 'hard-alee'})
    False
    
    >>> ts =  TrackSorter("helm's-alee", 'test')
    >>> ts.matches({'text': "Quick capn, helm's-alee gyarr!"})
    True
    >>> ts.matches({'text': "Quick capn, #helm's-alee gyarr!"})
    False
    """
    def __init__(self, keyword, bucket_id):
        BasicSorter.__init__(self, bucket_id)
        self.keyword = keyword.lower()
        self.kw_has_whitespace = len(keyword.split()) > 1
        self.kw_has_punc = no_punc(self.keyword) != self.keyword
        
    def matches(self, tweet):
        if self.kw_has_whitespace:
            return False

        words = [x.lower() for x in tweet.get('text', '').split()]
        if self.keyword in words:
            return True

        if not self.kw_has_punc:
            return self.keyword in [no_punc(x) for x in words]
        else:
            return False

class FollowSorter(BasicSorter):
    """
    From: http://apiwiki.twitter.com/Streaming-API-Documentation#follow
    References matched are statuses that were:
    * Created by a specified user
    * Explicitly in-reply-to a status created by a specified user 
      (pressed reply "swoosh" button)
    * Explicitly retweeted by a specified user (pressed retweet button)
    * Created by a specified user and subsequently explicitly retweed by any user

    References unmatched are statuses that were:
    * Mentions ("Hello @user!")
    * Implicit replies ("@user Hello!", created without pressing a reply "swoosh" 
      button to set the in_reply_to field)
    * Implicit retweets ("RT @user Says Helloes" without pressing a retweet button)
    
    >>> fs = FollowSorter('123', 'test')
    >>> fs.matches({'user': {'id': '123'}})
    True
    >>> fs.matches({'in_reply_to_user_id': '123'})
    True
    >>> fs.matches({'retweet_details': {'retweeting_user': {'id': '123'}}})
    True
    >>> fs.matches({'text': 'yo @123'})
    False
    >>> fs.matches({'text': '@123 yo!!!'})
    False
    >>> fs.matches({'text': 'RT @123 I am the balm'})
    False
    """
    def __init__(self, userid, bucket_id):
        BasicSorter.__init__(self, bucket_id)
        self.userid = userid

    def matches(self, tweet):
        if not self.userid:
            return False

        if (str(tweet.get('user', {}).get('id')) == self.userid or
            str(tweet.get('in_reply_to_user_id')) == self.userid or
            str(tweet.get('retweet_details', {}).get('retweeting_user', {}).get('id')) == self.userid):
            return True
        
        return False

class TweetSorter(object):
    def __init__(self, context):
        self.context = context
        self._rwlock = RWLock()
        self._sorters = []
        self.refresh()
        
        
    def create_sorter(self, filt_type, value, bucket_id):
        # ? Extensible...
        if filt_type == 'track':
            return TrackSorter(value, bucket_id)
        elif filt_type == 'follow':
            return FollowSorter(value, bucket_id)
        else:
            return None

    def refresh(self):
        self._rwlock.write_acquire()
        try:
            new_sorters = []
            for r in view_all_twitter_filters(self.context.db):
                filt_type, value = r.key
                sorter = self.create_sorter(filt_type, value, r.id)
                if sorter is not None:
                    new_sorters.append(sorter)
            self._sorters = new_sorters
        finally:
            self._rwlock.write_release()

    def sort(self, tweet_data, item):
        self._rwlock.read_acquire()
        try:
            for sorter in self._sorters:
                try:
                    sorter.apply(tweet_data, item, self.context)
                except:
                    log.error("Error sorting tweet: %s: %s" % (tweet_data, traceback.format_exc()))
        finally:
            self._rwlock.read_release()

class TweetSorterConsumer(TweetConsumer):

    queue = TWEET_SORTER_QUEUE

    def __init__(self, context):
        TweetConsumer.__init__(self, context.broker)
        self.context = context
        self.sorter = TweetSorter(context)
        self.context.event_bus.add_listener(TWITTER_CHANNEL, self.twitter_event)

    def receive(self, message_data, message):
        spawn(self.handle_message, message_data, message)

    def handle_message(self, message_data, message):
        try:
            item = Tweet.create_from_tweet(message_data, self.context)
            item.save()
            self.sorter.sort(message_data, item)
        finally:
            message.ack()

    def twitter_event(self, message):
        if message.get('type') == FILTERS_CHANGED:
            self.sorter.refresh()

    def close(self):
        TweetConsumer.close(self)
        # un-register callback from eventbus
        self.context.event_bus.remove_listener(TWITTER_CHANNEL, self.twitter_event)

#################################################
class TwitterStreamConnection(object):

    def __init__(self, context):
        self.context = context
        self._reader = None

    def run(self):
        try:
            self.context.event_bus.add_listener(TWITTER_CHANNEL, self.twitter_event)
            while(True):
                self._reader = spawn(self.read_stream)
                self._reader.wait()
        except ProcExit:
            if self._reader is not None:
                self._reader.kill()
                self._reader.wait()
            self.context.event_bus.remove_listener(TWITTER_CHANNEL, self.twitter_event)
            return

    def twitter_event(self, message):
        if message.get('type') == FILTERS_CHANGED:
            if self._reader is not None:
                self._reader.kill()

    def read_stream(self):
        tcp_failures = 0
        other_failures = 0
        successes = 0
        stream = None
        while True:
            try:
                stream = MelkmanTweetStream(self.context)
                while True:
                    received_tweet(stream.next(), self.context)
                    if tcp_failures > 0 or other_failures > 0:
                        successes += 1
                        if successes > 100:
                            tcp_failures = 0
                            other_failures = 0
            except ProcExit:
                if stream is not None:
                    stream.close()
                return
            except ConnectionError, e:
                if stream is not None:
                    stream.close()

                # backoff as suggested in
                # http://apiwiki.twitter.com/Streaming-API-Documentation#Connecting
                log.error('Error in twitter connection: %s' % traceback.format_exc())
                tcp_failures += 1
                successes = 0
                if tcp_failures < 64:
                    sleep(250 * tcp_failures)
                else:
                    sleep(16000)
            except:
                if stream is not None:
                    stream.close()
                # backoff as suggested in
                # http://apiwiki.twitter.com/Streaming-API-Documentation#Connecting
                log.error('Error handling twitter stream: %s' % traceback.format_exc())
                other_failures += 1
                successes = 0
                if other_failures < 15:
                    sleep(max(10*(2**other_failures)))
                else:
                    sleep(240000)
#           
# back-searching / following by sub-api?
# as feeds... (?)
#

if __name__ == '__main__':
    import doctest
    doctest.testmod()
