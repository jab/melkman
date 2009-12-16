if __name__ == '__main__':
    """
    This is a simple mainline that runs all worker processes
    configured in the context yaml file given.
    """

    from melkman.green import green_init
    green_init()
    
    import sys
    from giblets import Component, ExtensionPoint
    from eventlet.proc import spawn, waitall
    from melkman.green import GreenContext, consumer_loop, resilient_consumer_loop
    from melkman.worker import IWorkerProcess

    import logging
    log = logging.getLogger(__name__)
    logging.basicConfig(level=logging.DEBUG)
        
    if len(sys.argv) != 2:
        print "usage: %s <config.yaml>" % sys.argv[0]
        sys.exit(0)

    yaml_file = sys.argv[1]
    context = GreenContext.from_yaml(yaml_file)

    class ConfiguredWorkers(Component):
        workers = ExtensionPoint(IWorkerProcess)

        def run_all(self):
            procs = []
            if not self.workers:
                log.error("No workers are configured in the specified context.")
            
            for worker in self.workers:
                proc = spawn(worker.run, context)
                procs.append(proc)
            waitall(procs)
            
    ConfiguredWorkers(context.component_manager).run_all()