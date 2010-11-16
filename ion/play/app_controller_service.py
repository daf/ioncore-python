#!/usr/bin/env/python

"""
@file ion/play/app_controller_service.py
@author Dave Foster <dfoster@asascience.com>
@brief Application Controller for load balancing
"""

import os
import string

from ion.core import ioninit

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer, reactor, protocol

from ion.core.process.process import ProcessFactory, Process
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.core.messaging import messaging
from ion.core.messaging.receiver import Receiver

try:
    import json
except:
    import simplejson as json

STATIONS_PER_QUEUE = 2
CORES_PER_SQLSTREAM = 2     # SQLStream instances use 2 cores each: a 4 core machine can handle 2 instances
EXCHANGE_NAME = "magnet.topic"
DETECTION_TOPIC = "anf.detections"

class AppControllerService(ServiceProcess):
    """
    Defines an application controller service to perform load balancing.
    """

    declare = ServiceProcess.service_declare(name = "app_controller",
                                             version = "0.1.0",
                                             dependencies=[])

    def __init__(self, *args, **kwargs):
        ServiceProcess.__init__(self, *args, **kwargs)

        self.routing = {}   # mapping of queues to a list of bindings (station ids/sensor ids)
        self.workers = {}   # mapping of known worker vms to info about those vms (cores / running instances)
        self.unboundqueues = [] # list of queues waiting to be assigned to sqlstreams (op unit is starting up)
        self.sqldefs = None     # cached copy of SQLStream SQL definition templates from disk

    @defer.inlineCallbacks
    def slc_init(self):
        # Service life cycle state.
        
        # instrument announcement queue name
        self.announce_queue = "instrument_announce"

        # TODO: can use receiver just initialized, not activated - will create a queue
        #       consider that?
        #amsgs = {self.announce_queue: {'name_type':'worker', 'args':{'scope':'global'}}}
        #yield ioninit.container_instance.declare_messaging(amsgs)

        # consume the announcement queue
        self.announce_recv = TopicWorkerReceiver(name=self.announce_queue,
                                            scope='global',
                                            process=self,
                                            handler=self._recv_announce)
        
        # declares queue and starts listening on it
        yield self.announce_recv.attach()

        # get topic based routing to all sensor data (for anything missed on the announcement queue)
        self.all_data_recv = TopicWorkerReceiver(name="ta_alldata",
                                                 scope='global',
                                                 binding_key = "ta.*.BHZ",
                                                 process=self,
                                                 handler=self._recv_data)

        #yield self.all_data_recv.attach()
        #yield self.all_data_recv.initialize()
        #self.counter = 0

    @defer.inlineCallbacks
    def _recv_announce(self, data, msg):
        """
        Received an instrument announcement. Set up a binding for it.
        """
        jsdata = json.loads(data)
        station_name = jsdata['content']

        log.info("Instrument Station Announce: " + station_name)

        found = self.has_station_binding(station_name)
        
        if found:
            log.error("Duplicate announcement")
        else:
            yield self.bind_station(station_name)

        yield msg.ack()
    
    @defer.inlineCallbacks
    def bind_station(self, station_name, queue_name = None):
        """
        Binds a station to a queue. Typically you do not specify the queue name, this method
        will find a queue with room. If a queue name is given, no checking will be done - it 
        will simply be added.
        """
        if queue_name == None:
            queue_name = "W%s" % (len(self.routing.keys()) + 1)
             
            # find a queue with enough room
            added = False
            for queues in self.routing.keys():
                qlen = len(self.routing[queues])
                if qlen < STATIONS_PER_QUEUE:
                    queue_name = queues
                    break

        binding_key = 'ta.%s' % station_name

        yield self._create_queue(queue_name, binding_key)

        if not self.routing.has_key(queue_name):
            self.routing[queue_name] = []
            yield self.request_sqlstream(queue_name)
            
        self.routing[queue_name].append(station_name)
        
        log.info("Created binding %s to queue %s" % (binding_key, queue_name))

    @defer.inlineCallbacks
    def _create_queue(self, queue_name, binding_key):
        """
        Creates a queue and/or binding to a queue (just the binding if the queue exists).
        TODO: replace this with proper method of doing so.
        """
        recv = TopicWorkerReceiver(name=queue_name,
                                   scope='global',
                                   binding_key = binding_key,
                                   process=self)
        yield recv.initialize()   # creates queue but does not listen

    @defer.inlineCallbacks
    def request_sqlstream(self, queue_name, op_unit_id=None):
        """
        Requests a SQLStream operational unit to be created, or an additional SQLStream on an exiting operational unit.
        @param queue_name   The queue the SQL Stream unit should consume from.
        @param op_unit_id   The operational unit id that should be used to create a SQL Stream instance. If specified, will always create on that op unit. Otherwise, it will find available space on an existing VM or create a new VM.
        """
        if op_unit_id != None and not self.workers.has_key(op_unit_id):
            log.error("request_sqlstream: op_unit (%s) requested but unknown" % op_unit_id)
        
        # TODO: assign op_unit_id to it as well
        self.unboundqueues.insert(0, queue_name)

        if op_unit_id == None:
            # find an available op unit
            for (worker,info) in self.workers.items():
                availcores = info['cores'] - (len(info['sqlstreams']) * CORES_PER_SQLSTREAM)
                if availcores >= CORES_PER_SQLSTREAM:
                    log.info("request_sqlstream - asking existing operational unit (%s) to spawn new SQLStream" % worker)
                    # Request spawn new sqlstream instance on this worker
                    # wait for rpc message to app controller that says sqlstream is up
                    op_unit_id = worker

                    # record the fact we are using this worker now
                    info['sqlstreams'].append({'sqlstreamid':-1}) # 'sqlstreamid' will be updated when sqlstream actually comes up
                    break

        if op_unit_id == None:
            log.info("request_sqlstream - requesting new operational unit")
            # request spawn new VM
            # wait for rpc message to app controller that says vm is up, then request sqlstream
            defer.returnValue(None)
        else:
            yield self._start_sqlstream(worker, queue_name)
         
    @defer.inlineCallbacks
    def _recv_data(self, data, msg):
        #log.info("<-- data packet" + msg.headers.__str__())
        log.info("data " + self.counter.__str__())
        self.counter += 1
        yield msg.ack()

    def has_station_binding(self, station_name):
        """
        Returns true if we know about this station.
        """
        for queues in self.routing.keys():
            found = station_name in self.routing[queues]
            if found:
                return True

        return False

    @defer.inlineCallbacks
    def op_opunit_ready(self, content, headers, msg):
        """
        An op unit has started and reported in. It has no running SQLStream instances at
        this time.
        When we receive this message, if we have a queue waiting to be consumed,
        we tell it to start a SQLStream that will read from that queue.
        """
        log.info("Op Unit reporting ready " + content.__str__())
        if self.workers.has_key(content['id']):
            log.error("Duplicate worker ID just reported in")

        self.workers[content['id']] = {'cores':content['cores'],
                                       'sqlstreams':[]}

        yield self.reply_ok(msg, {'value': 'ok'}, {})

        # TODO: pump sql stream config over here
        queue_name = None
        if len(self.unboundqueues):
            queue_name = self.unboundqueues.pop()

        if queue_name != None:
            yield self._start_sqlstream(content['id'], queue_name)
            # processing continues when agent reports sqlstream is launched/configured/initialized
        else:
            log.info("Op Unit reported ready but no queue for it to work with")

        # if we did not find a queue_name from the unboundqueues, it's ok, it'll just be
        # in the list used by request_sqlstream

    @defer.inlineCallbacks
    def _start_sqlstream(self, id, queue_name):
        """
        Tells an op unit with the given id to start a SQLStream instance.
        """

        params = {'queue_name':queue_name,
                  'sql_defs':self._get_sql_def(inp_queue=queue_name, 
                                               inp_exchange=EXCHANGE_NAME, 
                                               det_topic=DETECTION_TOPIC, 
                                               det_exchange=EXCHANGE_NAME) }

        yield self.rpc_send(id, 'start_sqlstream', params)

    def _get_sql_def(self, **kwargs):
        """
        Gets SQLStream detection application SQL definitions, either from
        disk or in memory. SQL files stored on disk are loaded once and stored
        in memory after they have been translated through string.Template.

        You may override the SQL defs by sending an RPC message ("set_sql_defs") to
        the Application Controller. These defs will take the place of the current
        in memory defs. They are expected to be templates, in which certain vars will be
        updated. See op_set_sql_defs for more information.
        """
        #inp_queue, inp_exchange, out_queue, out_exchange, nostore=False):
        nostore = kwargs.pop('nostore', False)

        if self.sqldefs != None:
            fulltemplate = self.sqldefs
        else:
            fulltemplatelist = []
            for filename in ["catalog.sqlt", "detections.sqlt", "validate.sqlt"]:
                f = open(os.path.join(os.path.dirname(__file__), "app_controller_service", "catalog.sqlt"), "r")
                fulltemplatelist.extend(f.readlines())
                f.close()

            fulltemplate = "\n".join(fulltemplatelist)

            if not nostore:
                self.sqldefs = fulltemplate

        # template replace fulltemplates with kwargs
        t = string.Template(fulltemplate)
        defs = t.substitute(kwargs)
        return defs

    @defer.inlineCallbacks
    def op_sqlstream_ready(self, content, headers, msg):
        """
        A worker has indicated a SQLStream is ready.
        When we receive this message, we tell the SQLStream to turn its pumps on.
        """
        log.info("SQLStream reports as ready: %s" % str(content))
        yield self.reply_ok(msg, {'value': 'ok'}, {})

        # save knowledge of this sqlstream on this worker
        self.workers[content['id']]['sqlstreams'].append({'sqlstreamid':content['sqlstreamid'],
                                                          'queue_name':content['queue_name']})

        # tell it to turn the pumps on
        yield self.rpc_send(content['id'], 'ctl_sqlstream', {'sqlstreamid':content['sqlstreamid'],'action':'pumps_on'})

#
#
# ########################################################
#
#


class AppAgent(Process):
    """
    Application Agent - lives on the opunit, communicates status with app controller, recieves
    instructions.
    """

    #@defer.inlineCallbacks
    def plc_init(self):
        self.target = self.get_scoped_name('system', "app_controller")
        self.sqlstreams = {}

    @defer.inlineCallbacks
    def opunit_ready(self):
        """
        Declares to the app controller that this op unit is ready. This indicates a freshly
        started op unit with no running SQLStream instances.
        """
        (content, headers, msg) = yield self.rpc_send('opunit_ready', {'id':self.id.full, 'cores':2})
        defer.returnValue(str(content))

    @defer.inlineCallbacks
    def op_start_sqlstream(self, content, headers, msg):
        """
        Begins the process of starting and configuring a SQLStream instance on this op unit.
        The app controller calls here when it determines the op unit should spawn a new
        processing SQLStream.
        """
        ssid = len(self.sqlstreams.keys()) + 1
        self.sqlstreams[ssid] = {'id':ssid,
                                 'state':'starting',
                                 'queue_name':content['queue_name'],
                                 'sql_defs':content['sql_defs']}

        log.info("op_start_sqlstream") # : %s" % str(self.sqlstreams[ssid]))
        print str(self.sqlstreams[ssid])

        reactor.callLater(5, self._pretend_sqlstream_started, None, **{'sqlstreamid':ssid})

        yield self.reply_ok(msg, {'value':'ok'}, {})

    @defer.inlineCallbacks
    def _pretend_sqlstream_started(self, *args, **kwargs):
        """
        @TODO: remove, this is to simulate a SQLStream instance starting
        """
        ssid = kwargs.get('sqlstreamid')
        log.info("called later : sqlstream ready announce %s" % ssid)

        self.sqlstreams[ssid]['state'] = 'stopped'
        yield self.rpc_send('sqlstream_ready', {'sqlstreamid':ssid,
                                                'queue_name':self.sqlstreams[ssid]['queue_name']})

    @defer.inlineCallbacks
    def op_ctl_sqlstream(self, content, headers, msg):
        """
        Instructs a SQLStream to perform an operation like start or stop its pumps.
        The app controller calls here.
        """
        log.info("ctl_sqlstream received " + content['action'])

        if content['action'] == 'pumps_on':
            self.pumps_on(content['sqlstreamid'])
        elif content['action'] == 'pumps_off':
            self.pumps_off(content['sqlstreamid'])

        yield self.reply_ok(msg, {'value':'ok'}, {})

    #@defer.inlineCallbacks
    def pumps_on(self, sqlstreamid):
        """
        Instructs the given SQLStream worker to turn its pumps on.
        It will begin or resume reading from its queue.
        """
        log.info("pumps_on: %d" % sqlstreamid)
        self.sqlstreams[sqlstreamid]['state'] = 'running'

        sspp = SSProcessProtocol()
        #args = ['/Users/asadeveloper/tmp/seismic/drain_queue.py', '--queue', self.sqlstreams[sqlstreamid]['queue_name']] 
        processname = '/Users/asadeveloper/tmp/seismic/drain_queue.py'
        theargs = [processname, '--queue', self.sqlstreams[sqlstreamid]['queue_name']] 
        #args = ['/Users/asadeveloper/tmp/seismic/trad.py']
        self.sqlstreams[sqlstreamid]['proc'] = reactor.spawnProcess(sspp, processname, args=theargs, env=None, usePTY=True)

    #@defer.inlineCallbacks
    def pumps_off(self, sqlstreamid):
        """
        Instructs the given SQLStream worker to turn its pumps off.
        It will no longer read from its queue.
        """
        log.info("pumps_off: %d" % sqlstreamid)
        self.sqlstreams[sqlstreamid]['state'] = 'stopped'
        if self.sqlstreams[sqlstreamid].has_key('proc'):
            self.sqlstreams[sqlstreamid]['proc'].transport.signalProcess('KILL')

    @defer.inlineCallbacks
    def rpc_send(self, operation, content, headers=None, **kwargs):
        """
        Wrapper to make rpc calls a bit easier. Builds standard info into content message like
        our id.
        """
        content.update({'id':self.id.full})
        content.update(kwargs)

        ret = yield Process.rpc_send(self, self.target, operation, content, headers, **kwargs)
        defer.returnValue(ret)

    def __del__(self):
        """
        Destructor - kills any running consumers cleanly.
        TODO: this is temporary, only works with drain script. Replace with real comms to
        SQLStream.
        """
        for (sqlstreamid, info) in self.sqlstreams:
            if info.has_key('state') and info['state'] == 'running':
                if info.has_key['proc']:
                    info['proc'].transport.signalProcess('KILL')
#
#
# ########################################################
#
#

class TopicWorkerReceiver(Receiver):
    """
    A TopicWorkerReceiver is a Receiver from a worker queue that pays attention to its binding_key property. It also turns auto_delete off so consumers can detach without destroying the queues.

    TODO: This should be replaced by appropriate pubsub arch stuff.
    """

    def __init__(self, *args, **kwargs):
        """
        @param binding_key The binding key to use. By default, uses the computed xname, but can take a topic based string with wildcards.
        """
        binding_key = kwargs.pop("binding_key", None)
        Receiver.__init__(self, *args, **kwargs)
        if binding_key == None:
            binding_key = self.xname
        
        self.binding_key = binding_key

    @defer.inlineCallbacks
    def on_initialize(self, *args, **kwargs):
        """
        @retval Deferred
        """
        assert self.xname, "Receiver must have a name"

        name_config = messaging.worker(self.xname)
        #TODO: needs routing_key or it doesn't bind to the binding key - find where that occurs
        #TODO: auto_delete gets clobbered in Consumer.name by exchange space dict config - rewrite - maybe not possible if exchange is set to auto_delete always
        name_config.update({'name_type':'worker', 'binding_key':self.binding_key, 'routing_key':self.binding_key, 'auto_delete':False})

        #log.info("CONF IN " + name_config.__str__())

        yield self._init_receiver(name_config, store_config=True)

#
#
# ########################################################
#
#

class SSProcessProtocol(protocol.ProcessProtocol):
    """
    TODO: Class for connecting to SQLStream through Twisted
    """
    def connectionMade(self):
        log.info("CONNECT")

    def outReceived(self, data):
        log.debug(data)

    def processEnded(self, reason):
        log.info("PROC ENDED: %s" % reason.value.exitCode)


# Spawn of the process using the module name
factory = ProcessFactory(AppControllerService)

