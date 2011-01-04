#!/usr/bin/env python

"""
@file res/scripts/anf.py
@author Dave Foster <dfoster@asascience.com>
@brief main module for bootstrapping ANF test (services only)
"""
from twisted.internet import defer

from ion.core import ioninit
from ion.core import bootstrap
from ion.core.cc.shell import control

CONF = ioninit.config('startup.pubsub')

# Static definition of message queues
ion_messaging = ioninit.get_config('messaging_cfg', CONF)

anf_services = [
    {'name':'attributestore',
     'module':'ion.services.coi.attributestore',
     'class':'AttributeStoreService'},
    {'name':'app_controller',
     'module': 'ion.play.app_controller_service',
     'class': 'AppControllerService'}
]

@defer.inlineCallbacks
def start():
    """
    Main function of bootstrap.
    """
    startsvcs = []
    startsvcs.extend(anf_services)
    sup = yield bootstrap.bootstrap(ion_messaging, startsvcs)

start()
