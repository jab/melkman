if __name__ == "__main__":
    """
    a simple main-line that runs an Aggregator process
    """
    from melkman.green import green_init, GreenContext, resilient_consumer_loop
    green_init()
    
    from eventlet.proc import spawn, waitall
    import sys
    import logging
    logging.basicConfig(level=logging.DEBUG)

    from melkman.fetch import request_feed_index

    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print "usage: %s <config.yaml> [request_url]" % sys.argv[0]
        sys.exit(0)

    yaml_file = sys.argv[1]
    context = GreenContext.from_yaml(yaml_file)


    from melkman.aggregator.worker import CompositeUpdater, CompositeUpdateDispatcher
    updater = spawn(resilient_consumer_loop, CompositeUpdater, context)
    dispatcher = spawn(resilient_consumer_loop, CompositeUpdateDispatcher, context)
    waitall([updater, dispatcher])
