from giblets import Attribute, Component, ExtensionInterface, ExtensionPoint, implements
from melkman.green import green_init
green_init()    
import optparse
import signal
import sys

def print_usage(message=None):
    print "\nusage: %s <command|help> [...] [config.yaml]" % sys.argv[0]
    if message:
        print '\n%s' % message

class IRunnerCommand(ExtensionInterface):

    name = Attribute('name used to invoke this command')
    
    def __call__(context, args):
        """
        execute the runner command in this context given 
        with the arguments given. class docstring is used for
        built in help.
        """

class AvailableCommands(Component):
    
    commands = ExtensionPoint(IRunnerCommand)
    
    def lookup(self, name):
        for command in self.commands:
            if command.name == name:
                return command
        return None

def main():
    """
    This is a simple mainline that runs all worker processes
    configured in the context yaml file given.
    """

    from melkman.green import GreenContext

    if len(sys.argv) < 3:
        print_usage()
        sys.exit(0)
        
    command_name = sys.argv[1]
    yaml_file = sys.argv[-1]

    import logging
    log = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)
    context = GreenContext.from_yaml(yaml_file)


    all_commands = AvailableCommands(context.component_manager)
    command = all_commands.lookup(command_name)
    
    if command is None:
        print_usage('uknown command: %s' % command_name)
        sys.exit(0)

    def user_trap(*args, **kw):
        cmd = all_commands.lookup('shell')(context, [])
    signal.signal(signal.SIGUSR1, user_trap)


    return command(context, sys.argv[2:-1])



def assert_no_args(orig_func):
    def new_func(self, context, args):
        if len(args) > 0:
            print 'got unexpected argument(s) to command %s: "%s"' % (self.name, ' '.join(args))
            sys.exit(0)
        else:
            return orig_func(self, context, args)
    return new_func

class BootstrapCommand(Component):
    "bootstrap the database and related components"
    implements(IRunnerCommand)

    name = 'bootstrap'

    @assert_no_args
    def __call__(self, context, args):
        context.bootstrap()

class ShellCommand(Component):
    "run an interactive shell"

    implements(IRunnerCommand)
    
    name = 'shell'

    @assert_no_args
    def __call__(self, context, args):
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
    "run all configured melkman backend components"
    implements(IRunnerCommand)

    name = 'serve'

    @assert_no_args
    def __call__(self, context, args):
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
    """display built in help"""
    implements(IRunnerCommand)

    name = 'help'

    def __call__(self, context, args):
        all_commands = AvailableCommands(context.component_manager)
        
        if len(args) > 1:
            print "usage: help [command] <config.yaml>"
            print "help takes at most one argument."
        elif len(args) == 1:
            command = all_commands.lookup(args[0])
            if command is None:
                print "unknown command %s" % args[0]
            else:
                print "%s: %s" % (command.name, command.__doc__)
        else:
            commands_by_name = []
            for command in all_commands.commands:
                commands_by_name.append((command.name, command))
            commands_by_name.sort()
        
        
            print_usage()
            for command in commands_by_name:
                print "%s:  %s%s" % (command[1].name, ' '*(15-len(command[1].name)), command[1].__doc__)
            print '\n'

if __name__ == '__main__':
    main()