if __name__ == '__main__':
    import sys
    from melkman.context import Context

    if len(sys.argv) < 2:
        print "Usage: %s <context.yaml>" % sys.argv[0]
        sys.exit(0)

    yaml_file = sys.argv.pop(1)

    context = Context.from_yaml(yaml_file)
    print 'bootstrapping database and plugins...',
    context.bootstrap()
    print 'done'
