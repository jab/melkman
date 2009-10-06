
if __name__ == "__main__":
    """
    a simple main-line that runs a FeedIndexer process
    """
    from melkman.green import green_init, GreenContext, consumer_loop
    green_init()
    
    import sys
    import logging
    logging.basicConfig(level=logging.DEBUG)

    from melkman.fetch import request_feed_index

    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print "usage: %s <config.ini> [request_url]" % sys.argv[0]
        sys.exit(0)

    ini_file = sys.argv[1]
    context = GreenContext.from_ini(ini_file)

    if len(sys.argv) == 3:
        url = sys.argv[2]
        request_feed_index(url, context)
        sys.exit(0)
    else:
        from melkman.fetch.worker import FeedIndexer
        consumer_loop(FeedIndexer, context)