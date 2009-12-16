from giblets import Component, ExtensionInterface, ExtensionPoint, implements
from melkman.green import green_init
green_init()    
import sys

def print_usage(message=None):
    print "usage: %s <command|help> <config.yaml> [...]" % sys.argv[0]
    if message:
        print '\n%s' % message

class IRunnerCommand(ExtensionInterface):
    
    def __call__(context):
        """
        """

    def name():
        """
        """

    def description():
        """
        """

class AvailableCommands(Component):
    
    commands = ExtensionPoint(IRunnerCommand)
    
    def lookup(self, name):
        for command in self.commands:
            if command.name() == name:
                return command
        return None

def main():
    """
    This is a simple mainline that runs all worker processes
    configured in the context yaml file given.
    """

    from melkman.green import GreenContext

    if len(sys.argv) != 3:
        print_usage()
        sys.exit(0)
        
    command_name = sys.argv[1]
    yaml_file = sys.argv[2]

    import logging
    log = logging.getLogger(__name__)
    logging.basicConfig(level=logging.DEBUG)
    context = GreenContext.from_yaml(yaml_file)

    all_commands = AvailableCommands(context.component_manager)
    command = all_commands.lookup(command_name)
    
    if command_name is None:
        print_usage('uknown command')
        sys.exit(0)


    return command(context)


class BootstrapCommand(Component):
    implements(IRunnerCommand)

    def name(self):
        return 'bootstrap'

    def description(self):
        return 'bootstrap the database and related components'

    def __call__(self, context):
        context.bootstrap()

class ShellCommand(Component):
    implements(IRunnerCommand)

    def name(self):
        return 'shell'

    def description(self):
        return 'run an interactive shell'

    def __call__(self, context):
        # hijacked from pylons
        locs = {'ctx': context}
        banner_header = 'Melkman Interactive Shell\n'
        banner_footer = '\n\nYou may access the current context as "ctx"'
        try:
            # try to use IPython if possible
            from IPython.Shell import IPShellEmbed
            shell = IPShellEmbed(argv=sys.argv)
            banner = banner_header + shell.IP.BANNER + banner_footer
            shell.set_banner(banner)
            shell(local_ns=locs, global_ns={})
        except ImportError:
            import code
            pyver = 'Python %s' % sys.version
            banner = banner_header +  pyver + banner_footer

            shell = code.InteractiveConsole(locals=locs)
            try:
                import readline
            except ImportError:
                pass
            try:
                shell.interact(banner)
            finally:
                pass

class ServeCommand(Component):
    implements(IRunnerCommand)

    def name(self):
        return 'serve'

    def description(self):
        return 'run the configured melkman backend comonents'

    def __call__(self, context):
        from giblets import Component, ExtensionPoint
        from eventlet.proc import spawn, waitall
        from melkman.worker import IWorkerProcess

        class ConfiguredWorkers(Component):
            workers = ExtensionPoint(IWorkerProcess)

            def run_all(self):
                procs = []
                if not self.workers:
                    log.error("No workers are configured in the specified context")

                for worker in self.workers:
                    proc = spawn(worker.run, context)
                    procs.append(proc)
                waitall(procs)

        ConfiguredWorkers(context.component_manager).run_all()    

class HelpCommand(Component):
    implements(IRunnerCommand)

    def name(self):
        return 'help'

    def description(self):
        return 'display help'

    def __call__(self, context):
        all_commands = AvailableCommands(context.component_manager)
        commands_by_name = []
        for command in all_commands.commands:
            commands_by_name.append((command.name(), command))
        commands_by_name.sort()
        
        print_usage()
        for command in commands_by_name:
            print "%s: %s" % (command[1].name(), command[1].description())
        

if __name__ == '__main__':
    main()