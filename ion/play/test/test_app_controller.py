#!/usr/bin/env python

"""
@file ion/play/test/test_app_controller.py
@test ion.play.app_controller_service End to end automated tests for ANF Application Controller 
@author Dave Foster <dfoster@asascience.com>
"""
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

from ion.play import app_controller_service as app_controller_module
from ion.play.app_controller_service import AppControllerService, AppAgent
from ion.test.iontest import IonTestCase

from ion.core.process.process import ProcessFactory, Process, ProcessClient, ProcessDesc, processes
from ion.core.process.service_process import ServiceProcess
from ion.core import bootstrap, ioninit

import atexit

class FakeEPUControllerService(ServiceProcess):
    """
    Fake EPUControllerService instance that is used for testing.

    Imitates the EPUControllerService and starts processes within the same container.
    """

    declare = ServiceProcess.service_declare(name="epu_controller", version="0.88.0", dependencies=[])

    def slc_init(self):
        ServiceProcess.slc_init(self)

        self._conf = {}
        self._agentconfs = {}
        self._agents = {}

    @defer.inlineCallbacks
    def op_reconfigure(self, content, headers, msg):
        log.debug("HI FAKE GOT A RECONFIGURE %s %s" % (content.__class__, str(content)))
        self.reply_ok(msg)

        if content.has_key("unique_instances"):
            for k,v in content["unique_instances"].items():
                if not self._agents.has_key(k):

                    # build spawn args
                    sa = { 'opunit_id' : k,
                           'sqlstreams' : str(v['sqlstreams']),
                           'sqlt_vars' : content['provisioner_vars']['sqlt_vars'] }
                    log.debug("SPAWNING %s WITH %s" % (k, str(sa)))
                    self._agentconfs[k] = sa

                    #desc = ProcessDesc(name="AppAgent-%s" % k, module="ion.play.app_controller_service", procclass="AppAgent", spawnargs=sa)
                    #yield ioninit.container_instance.spawn_process(desc, self)
                    aa = AppAgent(spawnargs=sa)
                    yield aa.spawn()

                    self._agents[k] = aa

        self._conf = content



class AppControllerTest(IonTestCase):
    """
    """

    @defer.inlineCallbacks
    def setUp(self):
        self.timeout = 10
        services = [
            {'name' : 'attributestore', 'module' : 'ion.services.coi.attributestore',
             'class' : 'AttributeStoreService'},
        ]

        yield self._start_container()
        self.sup = yield self._spawn_processes(services)


    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_stuff(self):
        pass

# rewire the internals so that we can be the epu controller
# THIS IS A TEST CLASS, DONT IMPORT IT INTO IMPORTANT THINGS
oldepu = processes.pop('epu_controller', None)
factory = ProcessFactory(FakeEPUControllerService)

def bla():
    print "*********** HI IM RESTORING"
    processes['epu_controller'] = oldepu

#atexit.register(bla)

