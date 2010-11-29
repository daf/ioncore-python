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
from ion.services.cei.epucontroller import PROVISIONER_VARS_KEY

import uuid
import zlib
import base64

try:
    import multiprocessing  # python 2.6 only
    NO_MULTIPROCESSING = False
except:
    NO_MULTIPROCESSING = True

try:
    import json
except:
    import simplejson as json

STATIONS_PER_QUEUE = 2
CORES_PER_SQLSTREAM = 2     # SQLStream instances use 2 cores each: a 4 core machine can handle 2 instances
EXCHANGE_NAME = "magnet.topic"
DETECTION_TOPIC = "anf.detections"
SSD_READY_STRING = "Server ready; enter"
SSD_BIN = "/usr/local/sqlstream/SQLstream-2.5/bin/SQLstreamd"  # TODO: path search ourselves?
SSC_BIN = "/usr/local/sqlstream/SQLstream-2.5/bin/sqllineClient" 

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

        # provisioner vars are common vars for all worker instances
        self.prov_vars = { 'sqldefs'   : None,        # cached copy of SQLStream SQL definition templates from disk
                           'sqlt_vars' : { 'server_host'  : 'localhost',
                                           'inp_exchange' : EXCHANGE_NAME,
                                           'det_topic'    : DETECTION_TOPIC,
                                           'det_exchange' : EXCHANGE_NAME } }

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
            try:
                yield self.request_sqlstream(queue_name)
            except Exception,e:
                log.error("seomthing dyde %s" % str(e))
            
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

        # if this var is true, at the end of this method, instead of reconfiguring via
        # the decision engine, we will directly ask the agent on op_unit_id to spawn the 
        # sqlstream engine. This will hopefully be taken out when we can reconfigure 
        # workers on the fly.
        direct_request = False

        if op_unit_id != None and not self.workers.has_key(op_unit_id):
            log.error("request_sqlstream: op_unit (%s) requested but unknown" % op_unit_id)
        
        if op_unit_id == None:
            # find an available op unit
            for (worker,info) in self.workers.items():
                availcores = info['metrics']['cores'] - (len(info['sqlstreams']) * CORES_PER_SQLSTREAM)
                if availcores >= CORES_PER_SQLSTREAM:
                    log.info("request_sqlstream - asking existing operational unit (%s) to spawn new SQLStream" % worker)
                    # Request spawn new sqlstream instance on this worker
                    # wait for rpc message to app controller that says sqlstream is up
                    op_unit_id = worker

                    direct_request = True

                    # record the fact we are using this worker now
                    # TODO : needs to be an integer to indicate number of starting up, or a
                    # unique key per each starter
                    #info['sqlstreams']['spawning'] = True
                    break

        if op_unit_id == None:
            op_unit_id = str(uuid.uuid4())[:8]
            log.info("request_sqlstream - requesting new operational unit %s" % op_unit_id)

        # now we have an op_unit_id, update the config
        if not self.workers.has_key(op_unit_id):
            self.workers[op_unit_id] = {'metrics' : {'cores':2}, # all workers should have at least two, will be updated when status is updated
                                        'state' : {},
                                        'sqlstreams' : []}
            streamcount = 0
        else:
            streamcount = len(self.workers[op_unit_id]['sqlstreams'])

        stream_conf = { 'sqlt_vars' : { 'inp_queue' : queue_name },
                        'port'      : 9000 + streamcount + 1 }

        self.workers[op_unit_id]['sqlstreams'].append( { 'conf' : stream_conf,
                                                         'state': {} })

        if direct_request == True:
            yield self._start_sqlstream(op_unit_id, stream_conf)
        else:
            yield self.request_reconfigure()

            # TODO: maybe assign op_unit_id to it as well?
            # we're requesting an op unit start up, so put it in the available unbound queues
            # next opunit to come available will get it.
            #self.unboundqueues.insert(0, queue_name)

            # request spawn new VM
            # wait for rpc message to app controller that says vm is up, then request sqlstream
            #defer.returnValue(None)
        #else:
            #yield self._start_sqlstream(worker, queue_name)

    @defer.inlineCallbacks
    def request_reconfigure(self):
        """
        Requests a reconfiguration from the Decision Engine. This takes care of provisioning
        workers.

        This method builds the JSON required to reconfigure/configure the decision engine.
        """

        # TODO: likely does not need to send prov vars every time as this is reconfigure

        # update the sqldefs just in case they have changed via operation
        self._get_sql_def()

        provvars = self.prov_vars.copy()
        #provvars['sqldefs'] = provvars['sqldefs'].replace("$", "$$")    # escape template vars once so it doesn't get clobbered in provisioner replacement

        conf = { 'preserve_n'         : len(self.workers),
                 PROVISIONER_VARS_KEY : self.prov_vars,
                 'unique_instances'   : {} }

        for (wid, winfo) in self.workers.items():
            conf['unique_instances'][wid] = { 'sqlstreams' : [] }
            ssdefs = conf['unique_instances'][wid]['sqlstreams']
            for ssinfo in winfo['sqlstreams']:
                ssdefs.append( { 'port'      : ssinfo['conf']['port'],
                                 'sqlt_vars' : ssinfo['conf']['sqlt_vars'] } )

        print json.dumps(conf)

        # TODO: actually request
        defer.returnValue(None)
         
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

        TODO: do away with this, just replace it with a full agent status update
        """
        log.info("Op Unit reporting ready " + content.__str__())
        if self.workers.has_key(content['id']):
            log.error("Duplicate worker ID just reported in")

        if not self.workers.has_key(content['id']):
            self.workers[content['id']] = {}

        self.workers[content['id']].update({'metrics':content['metrics'], 
                                            'sqlstreams':{}})

        yield self.reply_ok(msg, {'value': 'ok'}, {})

    @defer.inlineCallbacks
    def _start_sqlstream(self, worker_id, conf):
        """
        Tells an op unit to start a SQLStream instance.
        """

        yield self.rpc_send(worker_id, 'start_sqlstream', conf)

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

        if self.prov_vars['sqldefs'] != None:
            fulltemplate = self.prov_vars['sqldefs']
        else:
            fulltemplatelist = []
            for filename in ["catalog.sqlt", "funcs.sqlt", "detections.sqlt", "validate.sqlt"]:
                f = open(os.path.join(os.path.dirname(__file__), "app_controller_service", filename), "r")
                fulltemplatelist.extend(f.readlines())
                f.close()

            fulltemplate = "\n".join(fulltemplatelist)

            if not nostore:
                fullcompressed = zlib.compress(fulltemplate)
                self.prov_vars['sqldefs'] = base64.b64encode(fullcompressed)

        # NO LONGER SUBSTITUTES - THE AGENT DOES THIS NOW
        return fulltemplate

        # template replace fulltemplates with kwargs
        #t = string.Template(fulltemplate)
        #defs = t.substitute(kwargs)
        #return defs

    @defer.inlineCallbacks
    def op_set_sql_defs(self, content, headers, msg):
        """
        Updates the current cached SQL defs for the SQLStream detection application.
        This overrides what is found on the disk.

        Note it does not update the SQL files on disk, so if the AppControllerService is
        restarted, it will need to be updated with the current defs again.

        This method expects that the only key in content, also named content, is a full 
        SQL definition (the concatenation of "catalog.sqlt", "detections.sqlt", and 
        "validate.sqlt") with Python string.Template vars as substitution points for the
        following variables:

        * inp_queue     - The input queue name to read messages from.
        * inp_exchange  - The exchange where the input queue resides.
        * det_topic     - The topic string that should be used for detections.
        * det_exchange  - The exchange where detections should be published.

        If these variables are not present, no error is thrown - it will use whatever you
        gave it. So your updated SQL definitions may hardcode the variables above.

        Variable substitution is done by calling self._get_sql_def and passing appropriate
        replacement variables via keyword arguments to the method. self._start_sqlstream
        is an example user of that replacement method.
        """
        self.prov_vars['sqldefs'] = content['content']
        yield self.reply_ok(msg, {'value':'ok'}, {})

    @defer.inlineCallbacks
    def op_sqlstream_ready(self, content, headers, msg):
        """
        A worker has indicated a SQLStream is ready.
        When we receive this message, we tell the SQLStream to turn its pumps on.
        """
        log.info("SQLStream reports as ready: %s" % str(content))
        yield self.reply_ok(msg, {'value': 'ok'}, {})

        # save knowledge of this sqlstream on this worker
        self.workers[content['id']]['sqlstreams'][content['sqlstreamid']] = {'queue_name':content['queue_name'], 'status':'ready'}

        # tell it to turn the pumps on
        yield self.rpc_send(content['id'], 'ctl_sqlstream', {'sqlstreamid':content['sqlstreamid'],'action':'pumps_on'})

    @defer.inlineCallbacks
    def op_sqlstream_status(self, content, headers, msg):
        """
        Status message recieved from agent to indicate what's happening with a SQLStream instance.
        """
        log.info("SQLStream status report: %s for SS# %d on %s" % (content['status'], content['sqlstreamid'], content['id']))

        self.workers[content['id']]['sqlstreams'][content['sqlstreamid']]['status'] = content['status']
        yield self.reply_ok(msg, {'value': 'ok'})

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

    def __init__(self, receiver=None, spawnargs=None, **kwargs):
        """
        Constructor.
        Gathers information about the system this agent is running on.
        """
        Process.__init__(self, receiver=receiver, spawnargs=spawnargs, **kwargs)

        self.metrics = { 'cores' : self._get_cores() }
        self.sqlstreams = {}

    #@defer.inlineCallbacks
    def plc_init(self):
        self.target = self.get_scoped_name('system', "app_controller")

        # check spawn args for sqlstreams, start them up as appropriate
        # expect a stringify'd python array of dicts
        if self.spawn_args.has_key('sqlstreams'):
            sqlstreams = eval(self.spawn_args['sqlstreams'])

            for ssinfo in sqlstreams:
                port = ssinfo['port']
                inp_queue = ssinfo['sqlt_vars']['inp_queue']
                defs = self._get_sql_defs(self.spawn_args['sqldefs'], ssinfo['sqlt_vars'])

                self.start_sqlstream(port, inp_queue, defs)

    def kill_sqlstreams(self):
        dl = []
        for sinfo in self.sqlstreams.values():
            if sinfo.has_key('serverproc') and sinfo['serverproc'] != None:
                dl.append(sinfo['serverproc'].close(timeout=120))   # needs a high timeout, takes a while to close!
        
        deflist = defer.DeferredList(dl)
        return deflist

    def kill_sqlstream_clients(self):
        dl = []
        for sinfo in self.sqlstreams.values():
            if sinfo.has_key('proc') and sinfo['proc'] != None:
                dl.append(sinfo['proc'].close())

        deflist = defer.DeferredList(dl)
        return deflist

    def _get_sql_defs(self, sqldefs, uconf, **kwargs):
        """
        Returns a fully substituted SQLStream SQL definition string.
        Using keyword arguments, you can update the default params passed in to spawn args.
        """
        conf = self.spawn_args['sqlt_vars'].copy()
        conf.update(uconf)
        conf.update(kwargs)

        # uncompress defs
        compdefs = base64.b64decode(sqldefs)
        defs = zlib.decompress(compdefs)

        template = string.Template(defs)
        return template.substitute(conf)

    def _get_cores(self):
        """
        Gets the number of processors/cores on the current system.
        Adapted from http://codeliberates.blogspot.com/2008/05/detecting-cpuscores-in-python.html
        """
        if NO_MULTIPROCESSING:
            if hasattr(os, "sysconf"):
                if os.sysconf_names.has_key("SC_NPROCESSORS_ONLN"):
                    # linux + unix
                    ncpus = os.sysconf("SC_NPROCESSORS_ONLN")
                    if isinstance(ncpus, int) and ncpus > 0:
                        return ncpus
                else:
                    # osx
                    return int(os.popen2("sysctl -n hw.ncpu")[1].read())

            return 1
        else:
            return multiprocessing.cpu_count()

    @defer.inlineCallbacks
    def opunit_ready(self):
        """
        Declares to the app controller that this op unit is ready. This indicates a freshly
        started op unit with no running SQLStream instances.
        """
        (content, headers, msg) = yield self.send_controller_rpc('opunit_ready', id=self.id.full, metrics=self.metrics)
        defer.returnValue(str(content))

    @defer.inlineCallbacks
    def op_start_sqlstream(self, content, headers, msg):
        """
        Begins the process of starting and configuring a SQLStream instance on this op unit.
        The app controller calls here when it determines the op unit should spawn a new
        processing SQLStream.
        """
        log.info("op_start_sqlstream") # : %s" % str(self.sqlstreams[ssid]))

        port = content['port']
        sqlt_vars = content['sqlt_vars']
        defs = self._get_sql_defs(sqlt_vars)

        self.start_sqlstream(port, content['inp_queue'], defs)

        yield self.reply_ok(msg, {'value':'ok'}, {})

    #@defer.inlineCallbacks
    def start_sqlstream(self, ssid, inp_queue, sql_defs):

        # pick a new ssid/port if None passed in (which is atypical)
        if ssid == None:
            start = 9001
            while start in self.sqlstreams.keys():
                start += 1
            ssid = start

        self.sqlstreams[ssid] = {'port':ssid,
                                 'state':'starting',
                                 'inp_queue':inp_queue,
                                 'sql_defs':sql_defs}

        log.debug("Starting SQLStream")

        # TODO: when we learn how to do this, we can fix it
        if len(self.sqlstreams) > 1:
            log.error("Cannot currently start more than one SQLStream")
        else:
            sspp = SSServerProcessProtocol(self, ready_callback=self._sqlstream_started, ready_callbackargs={'sqlstreamid':ssid})
            sspp.addCallback(self._sqlstream_ended, sqlstreamid=ssid)

            sspp.spawn()
            self.sqlstreams[ssid]['serverproc'] = sspp

    #@defer.inlineCallbacks
    def _sqlstream_ended(self, result, *args):
        """
        SQLStream daemon has ended.
        """
        ssid = args[0].get('sqlstreamid')
        log.debug("SQLStream (%s) has ended" % ssid)
        self.sqlstreams[ssid].pop('serverproc', None)

        # TODO: update appcontroller

        #defer.returnValue(None)

    #@defer.inlineCallbacks
    def _sqlstream_started(self, *args, **kwargs):
        """
        SQLStream daemon has started.
        Calls _load_sqlstream_defs.
        """
        ssid = kwargs.get('sqlstreamid')
        log.debug("SQLStream server (%s) has started" % ssid)
        self._load_sqlstream_defs(ssid)

    def _load_sqlstream_defs(self, ssid):
        """
        Spawns a SQLStream client to load the definitions of seismic app into the SQLStream instance.
        """
        sspp = SSClientProcessProtocol(self, sqlcommands=self.sqlstreams[ssid]['sql_defs'])
        sspp.addCallback(self._sqlstream_defs_loaded, sqlstreamid=ssid)

        sspp.spawn()
        self.sqlstreams[ssid]['proc'] = sspp

    #@defer.inlineCallbacks
    def _sqlstream_defs_loaded(self, result, *args):
        """
        SQLStream defs have finished loading.
        Begins starting pumps.
        """
        ssid = args[0].get('sqlstreamid')
        log.debug("SQLStream server (%s) has loaded defs" % ssid)

        # remove ref to proc
        self.sqlstreams[ssid].pop('proc', None)

        if result['exitcode'] == 0:
            self.sqlstreams[ssid]['state'] = 'stopped'
            self.pumps_on(ssid)
        else:
            log.warning("Loading defs failed, SS # %d" % ssid)
            # TODO: inform app controller?

        #defer.returnValue(None)

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

        sql_cmd = """
                  ALTER PUMP "SignalsPump" START;
                  ALTER PUMP "DetectionsPump" START;
                  ALTER PUMP "DetectionMessagesPump" START;
                  """
        sspp = SSClientProcessProtocol(self, sqlcommands=sql_cmd)
        sspp.addCallback(self._pumps_on_callback, sqlstreamid=sqlstreamid)
        
        sspp.spawn()
        self.sqlstreams[sqlstreamid]['proc'] = sspp

    @defer.inlineCallbacks
    def _pumps_on_callback(self, result, *args):
        log.debug("CALLBACK for pumps on: %d" % result['exitcode'])
        
        sqlstreamid = args[0].get('sqlstreamid')

        # remove ref to proc
        self.sqlstreams[sqlstreamid].pop('proc', None)

        if result['exitcode'] == 0:
            self.sqlstreams[sqlstreamid]['state'] = 'running'

            # call app controller to let it know status
            yield self.send_controller_rpc('sqlstream_status', sqlstreamid=sqlstreamid, status='running')
        else:
            log.warning("Could not turn pumps on for %s, SS # %d" % (self.id.full, sqlstreamid))
            # TODO: inform app controller?

    #@defer.inlineCallbacks
    def pumps_off(self, sqlstreamid):
        """
        Instructs the given SQLStream worker to turn its pumps off.
        It will no longer read from its queue.
        """
        log.info("pumps_off: %d" % sqlstreamid)

        sql_cmd = """
                  ALTER PUMP "SignalsPump" STOP;
                  ALTER PUMP "DetectionsPump" STOP;
                  ALTER PUMP "DetectionMessagesPump" STOP;
                  """
        self.sqlstreams[sqlstreamid]['state'] = 'stopped'

        sspp = SSClientProcessProtocol(self, sqlcommands=sql_cmd)
        sspp.addCallback(self._pumps_off_callback, sqlstreamid=sqlstreamid)

        sspp.spawn()
        self.sqlstreams[sqlstreamid]['proc'] = sspp
    
    @defer.inlineCallbacks
    def _pumps_off_callback(self, result, *args):
        log.debug("CALLBACK 2 for pumps OFF: %d" % result['exitcode'])

        sqlstreamid = args[0].get('sqlstreamid')

        # remove ref to proc
        self.sqlstreams[sqlstreamid].pop('proc', None)
        
        if result['exitcode'] == 0:
            self.sqlstreams[sqlstreamid]['state'] = 'stopped'

            # let app controller know status
            yield self.send_controller_rpc('sqlstream_status', sqlstreamid=sqlstreamid, status='stopped')
        else:
            log.warning("Could not turn pumps off for %s, SS # %d" % (self.id.full, sqlstreamid))

    @defer.inlineCallbacks
    def send_controller_rpc(self, operation, **kwargs):
        """
        Wrapper to make rpc calls a bit easier. Builds standard info into content message like
        our id.
        """
        content = {'id':self.id.full}
        content.update(kwargs)

        ret = yield self.rpc_send(self.target, operation, content, {}, **kwargs)
        defer.returnValue(ret)

    def __del__(self):
        """
        Destructor - kills any running consumers cleanly.
        TODO: this is temporary, only works with drain script. Replace with real comms to
        SQLStream.
        """
        #for (sqlstreamid, info) in self.sqlstreams:
        #    if info.has_key('state') and info['state'] == 'running':
        #        if info.has_key['proc']:
        #            info['proc'].transport.signalProcess('KILL')
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
    def __init__(self, app_agent, **kwargs):
        """
        Initializes a process protocol for a SQLStream client or server.

        @param app_agent    The application agent that is running this SQLStream client command.

        Additional parameters specified through keyword arguments are passed back to the
        callback method as keyword arguments.
        """
        self.app_agent = app_agent

        self.errlines = []
        self.outlines = []

        self.binary = None      # binary to run; set this in a derived class
        self.deferred_exited = defer.Deferred()   # is called back on process end
        self.used = False       # do not allow anyone to use again
        self.close_timeout = None

    def spawn(self, binary=None, args=[]):
        """
        Spawns an OS process via twisted's reactor.

        @returns    A deferred that is called back on process ending.
                    WARNING: it is not safe to yield on this deferred as the process
                    may never terminate! Use the close method to safely close a
                    process. You may yield on the deferred returned by that.
        """
        if binary == None:
            binary = self.binary

        if binary == None:
            log.error("No binary specified")
            raise ValueError("No binary specified")     # TODO: not this type

        theargs = [binary]
        theargs.extend(args)

        reactor.spawnProcess(self, binary, theargs, env=None)
        self.used = True

        return self.deferred_exited

    def addCallback(self, callback, **kwargs):
        """
        Adds a callback to be called when the process exits.
        You may call this method any time.

        @param  callback    Method to be called when the process exits. This callback should
                            take two arguments, a dict named result containing the status
                            of the process (exitcode, outlines, errlines) and then a *args param
                            for the callback args specified to this method.

                            This callback will be executed for any reason that the process
                            ends - whether it ended on its own, ended as a result of close, or
                            was killed abnormally. It is your responsibility to handle all these
                            cases.

        @param  **kwargs    Additional arguments to return to the callback.
        """
        self.deferred_exited.addCallback(callback, kwargs)

    def close(self, force=False, timeout=5):
        """
        Instructs the opened process to close down.

        @param force    If true, it severs all pipes and sends a KILL signal.
                        At this point, twisted essentially forgets about the
                        process.

        @param timeout  The amount of time allowed by the process to signal it has
                        closed. Default is 5 seconds. If the process does not shut down
                        within this time, close is called again with the force param
                        set to True.
        """

        def anon_timeout():
            self._close_impl(True)

        # we have to save this so we can cancel the timeout in processEnded
        self.close_timeout = reactor.callLater(timeout, anon_timeout)
        self._close_impl(force)

        # with the timeout in place, the processEnded will always be called, so its safe
        # to yield on this deferred now
        return self.deferred_exited

    def _close_impl(self, force):
        """
        Default implementation of close. Override this in your derived class.
        """
        if force:
            self.transport.loseConnection()
            self.transport.signalProcess("KILL")
        else:
            self.transport.closeStdin()
            self.transport.signalProcess("TERM")

    def connectionMade(self):
        log.debug("SSProcessProtocol: process started")

    def outReceived(self, data):
        self.outlines.append(data)

    def errReceived(self, data):
        self.errlines.append(data)

    #@defer.inlineCallbacks
    def processEnded(self, reason):
        log.debug("SSProcessProtocol: process ended (exitcode: %d)" % reason.value.exitCode)

        # call method in App Agent we set up when constructing
        #yield self.callback(exitcode=reason.value.exitCode, outlines=self.outlines, errlines=self.errlines, **self.callbackargs)

        # if this was called as a result of a close() call, we need to cancel the timeout so
        # it won't try to kill again
        if self.close_timeout != None and self.close_timeout.active():
            self.close_timeout.cancel()

        # form a dict of status to be passed as the result
        cba = { 'exitcode' : reason.value.exitCode,
                'outlines' : self.outlines,
                'errlines' : self.errlines }

        self.deferred_exited.callback(cba)

#
#
# ########################################################
#
#

class SSClientProcessProtocol(SSProcessProtocol):
    """
    SQLStream client process protocol.
    Upon construction, looks for a sqlcommands keyword argument. If that parameter exists,
    it will execute the commands upon launching the client process.
    """
    def __init__(self, app_agent, **kwargs):
        """
        @param sqlcommands  SQL commands to run in the client. May be None, which leaves stdin open.
        """
        self.sqlcommands = kwargs.pop('sqlcommands', None)
        SSProcessProtocol.__init__(self, app_agent, **kwargs)

        self.binary = SSC_BIN

    def connectionMade(self):
        SSProcessProtocol.connectionMade(self)

        # pump the contents of sqlcommands into stdin
        if self.sqlcommands != None:
            try:
                self.transport.write(self.sqlcommands.encode('ascii', 'ignore'))
            finally:
                self.transport.closeStdin()

#
#
# ########################################################
#
#

class SSServerProcessProtocol(SSProcessProtocol):
    def __init__(self, app_agent, **kwargs):
        """
        @param ready_callback   Callback that is called when the server reports it is ready. That callback gets any additional kwargs passed here.
        """
        self.ready_callback = kwargs.pop('ready_callback', None)
        self.ready_callbackargs = kwargs.pop('ready_callbackargs', {})
        SSProcessProtocol.__init__(self, app_agent, **kwargs)

        self.binary = SSD_BIN

    def _close_impl(self, force=False):
        """
        Safely exit SQLstream daemon.
        Make sure you use a long timeout when calling close, it takes a while.
        """
        if force:
            self.transport.signalProcess("INT") # sends ctrl-c which should abort sqlstreamd
            self.transport.loseConnection()
        else:
            self.transport.write('!quit\n') 

    @defer.inlineCallbacks
    def outReceived(self, data):
        SSProcessProtocol.outReceived(self, data)
        if (SSD_READY_STRING in data):
            yield self.ready_callback(**self.ready_callbackargs)
 

# Spawn of the process using the module name
factory = ProcessFactory(AppControllerService)

