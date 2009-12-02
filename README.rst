Melkman is a feed aggregating, sorting and filtering 
engine based on CouchDB, RabbitMQ and eventlet.

This is mostly a pre-alpha work-in-progress at the moment.

Requirements: 

Python
CouchDB  - http://couchdb.apache.org/
RabbitMQ - http://www.rabbitmq.com/

==================
Install
==================

It is recommended that you install melkman into a virtualenv. So if you 
have not already, first install virtualenv and pip:
$ easy_install virtualenv
...
$ easy_install pip
...

Great, now install melkman and dependencies into a new env:

$ virtualenv melkenv
$ pip -E melkenv install -r http://github.com/ltucker/melkman/raw/master/requirements.txt
...

This will install melkman as an editable package in melkenv/src/melkman.

==================
QuickStart
==================

There isn't much of an interface on top of things right now, 
but there are some ways to poke around...

Setting up
==========

Starting Other stuff
--------------------
make sure couchdb and rabbitmq are running...


Set up rabbitmq users / virtual hosts 
--------------------------------------
A few rabbit specific commands need to be run to 
set things up.  If you adjust the defaults, 
be sure to also change your configuration yaml 
file to reflect the changes as well as 
tests/test.yaml

$ cd melkman/scripts
$ sudo ./setup_rabbit.sh
...


Running tests
-------------
if you don't have nose, easy_install nose. Then:

(melkenv)$ cd melkman/tests
(melkenv)$ nosetests -s
.........................


Bootstrap database and plugins
------------------------------
To create the necessary databases, views and 
rabbit queues, you can run the bootstrap operation on the 
current setup defined in your configuration yaml.  
The package contains some sensible defaults.


(melkenv)$ cd melkman/scripts
(melkenv)$ python ./bootstrap.py ../dev.yaml
...

Now you're ready to start doing something. Hooray!


Doing Something
===============

Running the test engine
-----------------------
The test engine runs all of the engine's 
services in one monolithic process.

(melkenv)$ cd melkman/scripts
(melkenv)$ python ./monobrain.py ../dev.yaml
...


Interacting with the engine
---------------------------
Basically this means a python prompt atm :/ 

(melkenv)$ cd melkman/scripts
(melkenv)$ python ./shell.py ../dev.yaml

Melkman Interactive Shell
...
You may access the current context as "ctx"

>>> url = "http://rss.slashdot.org/Slashdot/slashdot"

>>> from melkman.fetch import request_feed_index
>>> request_feed_index(url, ctx)

>>> from melkman.db import RemoteFeed
>>> slashdot = RemoteFeed.get_by_url(url, ctx)

>>> slashdot.entries.values()[0].title
u"Something disastrous is happening to the internets"

Creating an aggregation
-----------------------

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
