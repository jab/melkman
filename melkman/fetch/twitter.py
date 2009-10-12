from carrot.messaging import Publisher, Consumer
from couchdb.design import ViewDefinition
from couchdb.schema import *
from giblets import Component, implements
import logging
from melk.util.hash import melk_id
from tweetstream import TweetStream
import urllib

from melkman.context import IRunDuringBootstrap
from melkman.db import NewsBucket, NewsItemRef

log = logging.getLogger(__name__)

class Tweet(NewsItemRef):
    document_types = ListField(TextField(), default=['NewsItemRef', 'Tweet'])
    
    @property
    def details(self):
        return {}

    def load_full_item(self):
        return self

class TweetBucket(NewsBucket):

    document_types = ListField(TextField(), default=['NewsBucket', 'TweetBucket'])

    filter_type = TextField()
    filter_value = TextField()

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
        bid = dbid(filter_type, value)
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
        filters = {}
        for r in view_all_twitter_filters(self.context.db):
            ftype, val = r.key
            filters.setdefault(ftype, [])
            filters[ftype].append(val)
        
        post_data = {}
        for ftype in self.SUPPORTED_FILTERS:
            vals = filters.get(ftype, [])
            if len(vals):
                post_data[ftype] = ','.join(vals)

        return urllib.urlencode(post_data)

TWEET_EXCHANGE = 'melkman.direct'
GOT_TWEET_KEY = 'tweet_recieved'
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

def recieved_tweet(self, tweet_data, context):
    publisher = TweetPublisher(self.context.broker)
    publisher.send(tweet_data)
    publisher.close()

class BasicSorter(object):
    def __init__(self, bucket_id):
        self.bucket_id = bucket_id
    
    def apply(self, tweet):
        if self.matches(tweet):
            pass

class KeywordSorter(BasicSorter):

    def __init__(self, keyword, bucket_id):
        BasicSorter.__init__(self, bucket_id)
        self.keyword = keyword
        
    def matches(self, tweet):
        pass

class FollowSorter(BasicSorter):
    def __init__(self, userid, bucket_id):
        BasicSorter.__init__(self, bucket_id)
        self.userid = userid

    def matches(self, tweet):
        pass

class TweetSorter(object):
    
    def __init__(self, context):
        self.context = context
        self.refresh()
        self._sorters = []
        self.refresh()
        
    def create_sorter(self, filt_type, value, id):
        # ? Extensible...
        if filt_type == 'track':
            return KeywordSorter(value, r.id)
        elif filt_type == 'follow':
            return FollowSorter(value, r.id)
        else:
            return None

    def refresh(self):
        self._sorters = []
        for r in view_all_twitter_filters(self.context.db):
            filt_type, value = r.key()
            sorter = self._create_sorter(filt_type, value, r.id)
            if sort is not None:
                self._sorters.append(sorter)

    def sort(self, tweet):
        for sorter in self._sorters:
            sorter.apply(tweet)
        
class TweetSorterConsumer(TweetConsumer):
    
    queue = TWEET_SORTER_QUEUE

    def __init__(self, context):
        TweetConsumer.__init__(self, context.broker)
        self.context = context

    def receive(self, message_data, message):
        spawn(self.handle_message, message_data, message)

    def handle_message(self, message_data, message):
        author_id = message_data.user.id
        
        follow_bucket = TweetBucket.get_by_follow(author_id)
        if follow_bucket is not None:
            follow_bucket.add_news_item(item)