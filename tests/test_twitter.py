from melkman.green import green_init
green_init()

from helpers import *

def test_filter_collection():
    from melkman.fetch.twitter import MelkmanTweetStream, TweetBucket    
    from cgi import parse_qs
    
    ctx = fresh_context()
    
    tids = {
    'joe': '1',
    'sue': '32523512',
    'joop': '21307'
    }

    stream = MelkmanTweetStream(ctx)

    buckets = []
    buckets.append(TweetBucket.create_from_follow(tids['joe'], ctx))
    buckets.append(TweetBucket.create_from_follow(tids['sue'], ctx))
    for b in buckets:
        b.save()
    
    pd = stream._get_post_data()
    filts = parse_qs(pd)

    assert len(filts) == 1
    assert 'follow' in filts
    assert len(filts['follow']) == 1
    fids = filts['follow'][0].split(',')
    assert len(fids) == 2
    assert tids['joe'] in fids
    assert tids['sue'] in fids
    assert tids['joop'] not in fids

    assert 'track' not in filts

    buckets = []
    buckets.append(TweetBucket.create_from_topic('soup', ctx))
    buckets.append(TweetBucket.create_from_topic('nuts', ctx))
    for b in buckets:
        b.save()
    
    pd = stream._get_post_data()
    filts = parse_qs(pd)
    
    assert len(filts) == 2
    
    assert 'follow' in filts
    assert len(filts['follow']) == 1
    fids = filts['follow'][0].split(',')
    assert len(fids) == 2
    assert tids['joe'] in fids
    assert tids['sue'] in fids
    assert tids['joop'] not in fids
    
    assert 'track' in filts
    assert len(filts['track']) == 1
    tracks = filts['track'][0].split(',')
    assert len(tracks) == 2
    assert 'soup' in tracks
    assert 'nuts' in tracks
    assert 'tacos' not in tracks

def test_tweet_sorting():
    from eventlet.api import sleep
    from eventlet.proc import spawn
    from melkman.fetch.twitter import TweetBucket, received_tweet
    from melkman.fetch.twitter import TweetSorterConsumer, tweet_trace
    from melkman.green import consumer_loop

    ctx = fresh_context()
    sorter = spawn(consumer_loop, TweetSorterConsumer, ctx)

    foo_bucket = TweetBucket.create_from_follow('12', ctx)
    foo_bucket.save()
    
    bar_bucket = TweetBucket.create_from_topic('bar', ctx)
    bar_bucket.save()

    tweets = [{'id': 1747474, 'user': {'id': 12}, 'text': 'bbzt blatt'},
              {'id': 1747475, 'user': {'id': 11}, 'text': 'bbzt #bar blatt'},
              {'id': 1747476, 'user': {'id': 12}, 'text': 'bbzt @bar blatt'}]

    for i in range(len(tweets)):
        received_tweet(tweets[i], ctx)

    sleep(.5)

    tts = [tweet_trace(t) for t in tweets]
    tids = [tt['item_id'] for tt in tts]
    
    foo_bucket = TweetBucket.get_by_follow('12', ctx)
    bar_bucket = TweetBucket.get_by_topic('bar', ctx)

    assert tids[0] in foo_bucket.entries
    assert tids[1] not in foo_bucket.entries
    assert tids[2] in foo_bucket.entries

    assert tids[0] not in bar_bucket.entries
    assert tids[1] in bar_bucket.entries
    assert tids[2] in bar_bucket.entries
    
    sorter.kill()

def test_tweet_sorting_reset():
    pass
    
def test_tweet_stream():
    pass
    
def test_tweet_stream_reset():
    pass