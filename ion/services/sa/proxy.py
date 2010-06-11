#!/usr/bin/env python
"""
@file ion/services/sa/proxy.py
@author Paul Hubbard
@date 5/25/10
@package ion.services.sa.proxy http->ooi proxy for user DAP access
Porting from LCO implementation to new LCA arch - complete rewrite.
"""

import logging
logging = logging.getLogger(__name__)

from ion.core.base_process import ProtocolFactory

from ion.services.base_service import BaseService
from ion.services.sa.tinyproxy import ProxyHandler, ThreadingHTTPServer

class ProxyService(BaseService):
    """
    Proxy service. Stub, really, since the proxy listens on a plain tcp port.
    """
    # Declaration of service
    declare = BaseService.service_declare(name='proxy',
                                          version='0.1.0',
                                          dependencies=['controller'])

    def __init__(self, receiver, spawnArgs=None):
        # Service class initializer. Basic config, but no yields allowed.
        BaseService.__init__(self, receiver, spawnArgs)
        logging.info('ProxyService.__init__()')

    def slc_init(self):
        """
        Use this hook to bind to listener TCP port and setup modified
        proxy stack.
        @todo Move tcp port to DX configuration file
        """
        tcp_port = 8000
        logging.debug('Setting up proxy on port %d...' % tcp_port)
        """
        based on BaseHTTPServer.test and tweaked a bit.
        """
        server_address = ('', tcp_port)
        ProxyHandler.protocol_version = 'HTTP/1.0'
        httpd = ThreadingHTTPServer(server_address, ProxyHandler)
#        httpd.serve_forever()
#        logging.debug('Proxy listener running.')

factory = ProtocolFactory(ProxyService)
