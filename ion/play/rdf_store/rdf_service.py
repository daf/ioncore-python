#!/usr/bin/env python

"""
@file ion/play/rdf_store/rdf_service.py
@package ion.play.rdf_store.rdf_service 
@author David Stuebe
@brief A service that provides git symantics for push pull commit and diff to
the rdf workspace composed of associations and objects.
The associations can be walked to find content.
"""

import logging
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient

class RdfService(BaseService):
    """
    Example service interface
    """
    # Declaration of service
    declare = BaseService.service_declare(name='RdfService',
                                          version='0.1.0',
                                          dependencies=[])

    def __init__(self, receiver, spawnArgs=None):
        # Service class initializer. Basic config, but no yields allowed.
        BaseService.__init__(self, receiver, spawnArgs)
        logging.info('RdfService.__init__()')

    def slc_init(self):
        # Service life cycle state. Initialize service here. Can use yields.
        pass

    @defer.inlineCallbacks
    def op_push(self, content, headers, msg):
        logging.info('op_push: '+str(content))

        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'value':'Hello there, '+str(content)}, {})

    @defer.inlineCallbacks
    def op_pull(self, content, headers, msg):
        logging.info('op_pull: '+str(content))

        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'value':'Hello there, '+str(content)}, {})



class RdfServiceClient(BaseServiceClient):
    """
    This is an exemplar service client that calls the hello service. It
    makes service calls RPC style.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "RdfService"
        BaseServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def push(self, text='Hi there push'):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('push', text)
        logging.info('Service reply: '+str(content))
        defer.returnValue(str(content))

    @defer.inlineCallbacks
    def pull(self, text='Hi there pull'):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('pull', text)
        logging.info('Service reply: '+str(content))
        defer.returnValue(str(content))



# Spawn of the process using the module name
factory = ProtocolFactory(RdfService)



"""
from ion.play import hello_service as h
spawn(h)
send(1, {'op':'hello','content':'Hello you there!'})

from ion.play.hello_service import HelloServiceClient
hc = HelloServiceClient(1)
hc.hello()
"""
