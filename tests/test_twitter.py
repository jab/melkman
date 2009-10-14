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
    pass
    
def test_tweet_sorting_reset():
    pass
    
def test_tweet_stream():
    pass
    
def test_tweet_stream_reset():
    pass