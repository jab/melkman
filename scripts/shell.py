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