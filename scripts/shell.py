if __name__ == '__main__':
    import sys
    from melkman.context import Context
    
    if len(sys.argv) < 2:
        print "Usage: %s <context.yaml>" % sys.argv[0]
        sys.exit(0)

    yaml_file = sys.argv.pop(1)

    context = Context.from_yaml(yaml_file)

    # hijacked from pylons
    locs = {'ctx': context}
    banner = 'You may access the current context as "ctx"'
    try:
        # try to use IPython if possible
        from IPython.Shell import IPShellEmbed

        shell = IPShellEmbed(argv=sys.argv)
        shell.set_banner(shell.IP.BANNER + '\n\n' + banner)
        try:
            shell(local_ns=locs, global_ns={})
        finally:
            import paste
            paste.registry.restorer.restoration_end()
    except ImportError:
        import code
        newbanner = "Melkman Interactive Shell\nPython %s\n\n" % sys.version
        banner = newbanner + banner
        shell = code.InteractiveConsole(locals=locs)
        try:
            import readline
        except ImportError:
            pass
        try:
            shell.interact(banner)
        finally:
            pass