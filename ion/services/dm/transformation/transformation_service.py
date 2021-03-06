#!/usr/bin/env python

"""
@file ion/services/dm/transformation/transformation_service.py
@author Michael Meisinger
@brief service for transforming information
"""

import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient

class TransformationService(BaseService):
    """Transformation service interface
    """

    # Declaration of service
    declare = BaseService.service_declare(name='transformation_service', version='0.1.0', dependencies=[])
 
    def op_transform(self, content, headers, msg):
        """Service operation: TBD
        """
        
# Spawn of the process using the module name
factory = ProtocolFactory(TransformationService)

class TransformationClient(BaseServiceClient):
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = 'transformation_service'
        BaseServiceClient.__init__(self, proc, **kwargs)

    def transform(self, object):
        '''
        @Brief transform an object to a different type
        @param what?
        '''