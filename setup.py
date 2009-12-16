# Copyright (C) 2009 The Open Planning Project
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the
# Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor,
# Boston, MA  02110-1301
# USA

try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

setup(
    name='melkman',
    version="0.1",
    #description="",
    license="GPLv2 or any later version",
    author="Luke Tucker",
    author_email="ltucker@openplans.org",
    url="http://github.com/ltucker/melkman",
    install_requires=['couchdb',
                      'carrot>=0.6',
                      'eventlet>=0.9',
                      'giblets',
                      'greenamqp',
                      'httplib2<0.5',
                      'melk.util',
                      'simplejson',
                      'PyYAML',
                      'WebOb'
    ],
    dependency_links=[
    ],
    packages=find_packages(exclude=['ez_setup']),
    include_package_data=True,
    test_suite='nose.collector',
    entry_points="""
    [melkman_plugins]
    scheduler = melkman.scheduler.api
    scheduler_proc = melkman.scheduler.worker
    fetch = melkman.fetch.api
    fetch_proc = melkman.fetch.worker
    aggregator = melkman.aggregator.api
    aggregator_worker = melkman.aggregator.worker
    filters = melkman.filters
    pubsub = melkman.fetch.pubsubhubbub
    
    [console_scripts]
    melkman=melkman.runner:main
    """,
)

