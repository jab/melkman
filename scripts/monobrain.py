if __name__ == '__main__':
    """
    A simple main-line that runs a bail of all the services
    in a single process...
    """
    from melkman.green import green_init
    green_init()
    
    import sys
    from eventlet.proc import spawn, waitall
    from melkman.green import GreenContext, consumer_loop, resilient_consumer_loop


    import logging
    logging.basicConfig(level=logging.DEBUG)
    
    
    if len(sys.argv) != 2:
        print "usage: %s <config.yaml>" % sys.argv[0]
        sys.exit(0)

    yaml_file = sys.argv[1]
    context = GreenContext.from_yaml(yaml_file)

    procs = []

    # scheduled message service
    from melkman.scheduler.worker import ScheduledMessageService
    def run_sched(context):
        sms = ScheduledMessageService(context)
        sms.run()
    sched = spawn(run_sched, context)
    procs.append(sched)

    # fetcher
    from melkman.fetch.worker import FeedIndexer
    fetcher = spawn(resilient_consumer_loop, FeedIndexer, context)
    procs.append(fetcher)
    
    # aggregator 
    from melkman.aggregator.worker import CompositeUpdater, CompositeUpdateDispatcher
    updater = spawn(resilient_consumer_loop, CompositeUpdater, context)
    dispatcher = spawn(resilient_consumer_loop, CompositeUpdateDispatcher, context)
    procs.append(updater)
    procs.append(dispatcher)
    
    waitall(procs)
