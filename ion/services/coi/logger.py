#!/usr/bin/env python

"""
@file ion/services/coi/logger.py
@author Michael Meisinger
@brief service backend for logging. Plays nicely with logging package
"""

import logging.config
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient

logserv = logging.getLogger('logServer')

class LoggerService(BaseService):
    """Logger service interface
    """

    # Declaration of service
    declare = BaseService.service_declare(name='logger', version='0.1.0', dependencies=[])

    def slc_init(self):
        logging.info("LoggingService initialized")

    def op_config(self, content, headers, msg):
        pass

    @defer.inlineCallbacks
    def op_logmsg(self, content, headers, msg):
        level = content.get('level','info')
        logmsg = content.get('msg','#NO MESSAGE#')
        lfrom = headers.get('sender','')
        ltime = content.get('logtime')

        if level=='debug':
            logserv.debug(logmsg)
        elif level=='info':
            logserv.info(logmsg)
        elif level=='warn':
            logserv.warn(logmsg)
        elif level=='error':
            logserv.error(logmsg)
        elif level=='critical':
            logserv.critical(logmsg)
        else:
            logging.error('Invalid log level: '+str(level))
        yield self.reply(msg, 'result', 'Ok')


class LoggerClient(BaseServiceClient):
    """
    Class for client to sent log message to service
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "logger"
        BaseServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def logmsg(self, level, msg, sender, logtime):
        yield self._check_init()
        
        # do we or don't we trust/care about client timestamps?  If not,
        # the logtime isn't needed. 

        cont={'level':level,'msg':msg,'sender':sender,'logtime':logtime}            
        (content, headers, msg) = yield self.rpc_send('logmsg', cont)
        logging.info('Service reply: '+str(content))
        
        defer.returnValue(str(content))


# Spawn of the process using the module name
factory = ProtocolFactory(LoggerService)

"""
from ion.services.coi import logger
spawn(logger)
send(1,{'op':'logmsg','sender':'snd.1','content':{'level':'info','logtime':'120202112','msg':'Test logging'}})
"""


