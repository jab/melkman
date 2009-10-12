
if __name__ == '__main__':
    """
    A simple main-line that runs the scheduled message
    service.
    """
    
    import sys
    from melkman.green import green_init, GreenContext
    green_init()
    
    from melkman.scheduler.worker import ScheduledMessageService
    import logging
    logging.basicConfig(level=logging.DEBUG)
    
    
    if len(sys.argv) != 2:
        print "usage: %s <config.yaml>" % sys.argv[0]
        sys.exit(0)

    yaml_file = sys.argv[1]
    context = GreenContext.from_yaml(yaml_file)

    sms = ScheduledMessageService(context)
    sms.run()
