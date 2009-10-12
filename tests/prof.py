from melkman.green import green_init, GreenContext, consumer_loop
green_init()

from helpers import *
from melkman.db import NewsBucket, RemoteFeed


def profile_bucket_saves():
    ctx = fresh_context()
    
    import hotshot
    prof = hotshot.Profile("bucket_saves.prof")
    prof.runcall(_profile_bucket_saves, ctx)
    prof.close()

def _profile_bucket_saves(context):
    bucket = NewsBucket.create(context)
    for i in range(100):
        for i in range(100):
            bucket.add_news_item(dummy_news_item({}))
        bucket.save()
        
def profile_push_index(context):
    pass
    
if __name__ == '__main__':
    import sys
    import logging
    logging.basicConfig(level=logging.DEBUG)
    
    profile_bucket_saves()

