Overview
========

Melkman is a feed aggregating, sorting, and filtering engine based on `CouchDB
<http://couchdb.apache.org/>`_, `RabbitMQ <http://www.rabbitmq.com/>`_, and
`eventlet <http://eventlet.net>`_.

This is mostly a pre-alpha work-in-progress at the moment.


Requirements
============

- `Python <http://www.python.org/>`_ 2.6
- `CouchDB <http://couchdb.apache.org/>`_ 0.9 or higher
- `RabbitMQ <http://www.rabbitmq.com/>`_ 1.6 or higher


Installation
============

It is recommended that you install melkman into a virtualenv. So if you have
not already, first install `virtualenv <http://virtualenv.openplans.org/>`_
version 1.4.3 or higher (comes with `pip <http://pip.openplans.org/>`_),
either via your system's package manager::

    $ sudo port install py26-virtualenv

or via `distribute
<http://pypi.python.org/pypi/distribute>`_ / `setuptools
<http://pypi.python.org/pypi/setuptools>`_::

    $ sudo easy_install virtualenv

Now install melkman and its dependencies into a new virtualenv::

    $ virtualenv melkenv
    ...
    $ cd melkenv
    $ source bin/activate
    (melkenv)$ pip install -r http://github.com/ltucker/melkman/raw/master/requirements.txt
    ...

This will install melkman as an editable package in melkenv/src/melkman.


Quick Start
===========

There isn't much of an interface on top of things right now, but there are
some ways to poke around...

Make sure sure couchdb and rabbitmq are running before doing the following:

Set up rabbitmq users / virtual hosts 
-------------------------------------

A few rabbit specific commands need to be run to set things up::

    $ sudo melkman/scripts/setup_rabbit.sh
    ...

If you adjust the defaults, be sure to also change your dev.yaml configuration
file as well as tests/test.yaml to reflect the changes.

Running tests
-------------

Running the tests requires `nose
<http://somethingaboutorange.com/mrl/projects/nose/>`_. (Run ``easy_install
nose`` inside your virtualenv to install.) Then::

    (melkenv)$ cd melkman/tests
    (melkenv)$ nosetests -s
    ................................................
    ----------------------------------------------------------------------
    Ran ... tests in ...s

    OK


Bootstrap database and plugins
------------------------------

To create the necessary databases, views, and rabbit queues, you can run
the bootstrap operation on the current setup defined in your configuration
yaml. The package contains some sensible defaults::

    (melkenv)$ melkman bootstrap ../dev.yaml
    ...

Now you're ready to start doing something, hooray!


Doing Something
===============

Running the development engine
------------------------------

The development engine runs all of the engine's services in one
monolithic process::

    (melkenv)$ melkman serve ../dev.yaml
    ...


Interacting with the engine
---------------------------

Basically this means a python prompt at the moment (though you can also try
an `experimental frontend <http://github.com/jab/beereader>`_)::

    (melkenv)$ melkman shell ../dev.yaml

    Melkman Interactive Shell
    ...
    You may access the current context as "ctx"


 First let's start indexing a feed::

    >>> url = "http://rss.slashdot.org/Slashdot/slashdot"

    >>> from melkman.fetch import request_feed_index
    >>> request_feed_index(url, ctx)

    >>> from melkman.db import RemoteFeed
    >>> slashdot = RemoteFeed.get_by_url(url, ctx)

    >>> slashdot.entries.values()[0].title
    u"Something disastrous is happening to the internets"

Now let's create a composite, an aggregation of one or more sources::

    >>> from melkman.db import Composite
    >>> my_feeds = Composite.create(ctx)
    >>> my_feeds.subscribe(slashdot)
    >>> my_feeds.save()

    >>> my_feeds = Composite.get(my_feeds.id, ctx)
    >>> my_feeds.entries.values()[0].title
    u"Something disastrous is happening to the internets"

    >>> url = "http://www.nytimes.com/services/xml/rss/nyt/HomePage.xml" 
    >>> nytimes = RemoteFeed.create_from_url(url, ctx)
    >>> nytimes.save()
    >>> my_feeds.subscribe(nytimes)
    >>> my_feeds.save()

    >>> my_feeds = Composite.get(my_feeds.id, ctx)
    >>> my_feeds.entries.values()[0].title
    u"Something disastrous is happening to the earths"
