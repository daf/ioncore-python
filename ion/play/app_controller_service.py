#!/usr/bin/env/python

"""
@file ion/play/app_controller_service.py
@author Dave Foster <dfoster@asascience.com>
@brief Application Controller for load balancing
"""

import os, string, tempfile, shutil
#from collections import MutableSequence        # MUTABLESEQUENCE

from ion.core import ioninit

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer, reactor, protocol

from ion.core.process.process import ProcessFactory, Process
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.core.messaging import messaging
from ion.core.messaging.receiver import Receiver
from ion.services.cei.epucontroller import PROVISIONER_VARS_KEY
from ion.services.cei.epu_reconfigure import EPUControllerClient
from ion.util.task_chain import TaskChain
from ion.util.os_process import OSProcess

import uuid
import zlib
import base64
import pprint

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
SSD_BIN = "bin/SQLstreamd"
SSC_BIN = "bin/sqllineClient" 

# TODO: deployables
SS_INSTALLER_BIN = "/home/daf/Downloads/SQLstream-2.5.0.6080-opto-x86_64.bin"
SS_SEISMIC_JAR = "/usr/local/seismic/lib/ucsd-seismic.jar"

# TODO: obv
SS_FIXED_DAEMON = "/home/daf/tmp/sqlstream/SQLstreamd"
SS_FIXED_CLIENT = "/home/daf/tmp/sqlstream/sqllineClient"

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

        self.epu_controller_client = EPUControllerClient()

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
                                        'state' : '',
                                        'sqlstreams' : {}}
            streamcount = 0
        else:
            streamcount = len(self.workers[op_unit_id]['sqlstreams'])

        ssid = streamcount + 1

        stream_conf = { 'sqlt_vars' : { 'inp_queue' : queue_name },
                        'ssid'      : ssid }

        self.workers[op_unit_id]['sqlstreams']['ssid'] = { 'conf' : stream_conf,
                                                           'state': {} }

        if direct_request == True:
            yield self._start_sqlstream(op_unit_id, stream_conf)
        else:
            yield self.request_reconfigure()

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
                ssdefs.append( { 'ssid'      : ssinfo['conf']['ssid'],
                                 'sqlt_vars' : ssinfo['conf']['sqlt_vars'] } )

        print json.dumps(conf)

        yield self.epu_controller_client.reconfigure(conf)

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

    def op_opunit_status(self, content, headers, msg):
        """
        Handles an application agent reporting an operational unit's status.
        Details include its current state, metrics about the system, status of
        SQLstream instances.
        """
        opunit_id   = content['id']
        proc_id     = content['proc_id']
        state       = content['state']
        metrics     = content['metrics']
        sqlstreams  = content['sqlstreams']

        log.info("Op Unit (%s) status update: state (%s), sqlstreams (%d)" % (opunit_id, state, len(sqlstreams)))

        if not self.workers.has_key(content['id']):
            self.workers[content['id']] = {}

        self.workers[opunit_id].update({'metrics':metrics,
                                        'state': state,
                                        'proc_id': proc_id,
                                        'sqlstreams':sqlstreams})

        self.reply_ok(msg, {'value': 'ok'}, {})

    @defer.inlineCallbacks
    def _start_sqlstream(self, op_unit_id, conf):
        """
        Tells an op unit to start a SQLStream instance.
        """
        proc_id = self.workers[op_unit_id]['proc_id']
        yield self.rpc_send(proc_id, 'start_sqlstream', conf)

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

        sa = spawnargs
        if not sa:
            sa = {}

        self._opunit_id = sa.get("opunit_id", str(uuid.uuid4())[:8]) # if one didn't get assigned, make one up to report in to the app controller

        self.metrics = { 'cores' : self._get_cores() }
        self.sqlstreams = {}

    #@defer.inlineCallbacks
    def plc_init(self):
        self.target = self.get_scoped_name('system', "app_controller")

        defs = self.spawn_args.get('sqldefs')   # SHOULD ALWAYS HAVE THIS IN SPAWN ARGS
        if defs:
            # uncompress defs
            compdefs = base64.b64decode(defs)
            uncompresseddefs = zlib.decompress(compdefs)

            self._sqldefs = uncompresseddefs

        # check spawn args for sqlstreams, start them up as appropriate
        # expect a stringify'd python array of dicts
        if self.spawn_args.has_key('sqlstreams'):
            sqlstreams = eval(self.spawn_args['sqlstreams'])

            for ssinfo in sqlstreams:
                ssid = ssinfo['ssid']
                inp_queue = ssinfo['sqlt_vars']['inp_queue']
                defs = self._get_sql_defs(uconf=ssinfo['sqlt_vars'])

                self.start_sqlstream(ssid, inp_queue, defs)

        # let controller know we're starting and have some sqlstreams starting, possibly
        self.opunit_status()

    @defer.inlineCallbacks
    def plc_terminate(self):
        """
        Termination of this App Agent process.
        Attempts to shut down sqlstream clients and sqlstream daemon instances cleanly. If
        they exceed a timeout, they are shut down forcefully.
        """
        yield self.kill_sqlstream_clients()  # kill clients first, they prevent sqlstream daemons from shutting down
        yield self.kill_sqlstreams()

    def kill_sqlstreams(self):
        dl = []
        for sinfo in self.sqlstreams.values():
            if sinfo.has_key('serverproc') and sinfo['serverproc'] != None:
                dl.append(self.kill_sqlstream(sinfo['ssid']))
        
        deflist = defer.DeferredList(dl)
        return deflist

    def kill_sqlstream(self, ssid):
        """
        Shuts down and deletes a single SQLstream instance.
        @return A deferred which will be called back when the steps to stop and delete a SQLstream
                instance are complete.
        """
        chain = TaskChain()
        if self.sqlstreams[ssid]['state'] == 'running':
            chain.append(self.get_pumps_off_proc(ssid).spawn)
        chain.append((self.sqlstreams[ssid]['serverproc'].close, [], { 'timeout':30 }))
        chain.append((shutil.rmtree, [self.sqlstreams[ssid]['dirname']]))
        return chain.run()

    def kill_sqlstream_clients(self):
        dl = []
        for sinfo in self.sqlstreams.values():
            if sinfo.has_key('taskchain') and sinfo['taskchain'] != None:
                dl.append(sinfo['taskchain'].close())

        deflist = defer.DeferredList(dl)
        return deflist

    def _get_sql_defs(self, sqldefs=None, uconf={}, **kwargs):
        """
        Returns a fully substituted SQLStream SQL definition string.
        Using keyword arguments, you can update the default params passed in to spawn args.
        """
        assert self.spawn_args.has_key('sqlt_vars') and self._sqldefs, "Required SQL definitions and substitution vars have not been set yet."

        conf = self.spawn_args['sqlt_vars'].copy()
        conf.update(uconf)
        conf.update(kwargs)

        defs = sqldefs
        if defs == None:
            defs = self._sqldefs

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
    def opunit_status(self):
        """
        Sends the current status of this Agent/Op Unit.
        """
        content = { 'id' : self._opunit_id,
                    'metrics' : self.metrics,
                    'sqlstreams' : self.sqlstreams,
                    'state' : 'temp' }

        yield self.send_controller_rpc('opunit_status', **content)

    @defer.inlineCallbacks
    def op_start_sqlstream(self, content, headers, msg):
        """
        Begins the process of starting and configuring a SQLStream instance on this op unit.
        The app controller calls here when it determines the op unit should spawn a new
        processing SQLStream.
        """
        log.info("op_start_sqlstream") # : %s" % str(self.sqlstreams[ssid]))

        ssid        = content['ssid']
        sqlt_vars   = content['sqlt_vars']
        defs        = self._get_sql_defs(uconf=sqlt_vars)
        failed      = False
        ex          = None

        try:
            self.start_sqlstream(ssid, sqlt_vars['inp_queue'], defs)
        except ValueError,e:
            failed = True
            ex = e

        if failed:
            resp = { 'response':'failed',
                     'exception':ex }
        else:
            resp = { 'response':'ok' }

        yield self.reply_ok(msg, resp, {})

    #@defer.inlineCallbacks
    def start_sqlstream(self, ssid, inp_queue, sql_defs):
        """
        Returns a deferred you can yield on. When finished, the sqlstream should be up
        and running.
        """

        if ssid == None:
            raise ValueError("ssid is None")

        if ssid in self.sqlstreams.keys():
            log.error("Duplicate SSID requested")
            raise ValueError("Duplicate SSID requested (%s)" % ssid)

        procs = []
        chain = TaskChain()

        # vars needed for params
        sdpport     = str(5575 + int(ssid))
        hsqldbport  = str(9000 + int(ssid))
        dirname     = os.path.join(tempfile.gettempdir(), 'sqlstream.%s.%s' % (sdpport, hsqldbport))
        installerbin= os.path.join(os.path.dirname(__file__), "app_controller_service", "install_sqlstream.sh")

        log.debug("Starting SQLStream (sdp=%s, hsqldb=%s, install dir=%s)" % (sdpport, hsqldbport, dirname))

        # record state
        self.sqlstreams[ssid] = {'ssid'         : ssid,
                                 'hsqldbport'   : hsqldbport,
                                 'sdpport'      : sdpport,
                                 'state'        : 'starting',
                                 'inp_queue'    : inp_queue,
                                 'dirname'      : dirname,
                                 'sql_defs'     : sql_defs}

        # 1. Install SQLstream daemon
        proc_installer = OSProcess(binary=installerbin, spawnargs=[sdpport, hsqldbport, dirname])
        chain.append(proc_installer.spawn)

        # 2. SQLStream daemon process
        proc_server = OSSSServerProcess(binroot=dirname)
        proc_server.addCallback(self._sqlstream_ended, sqlstreamid=ssid)
        proc_server.addReadyCallback(self._sqlstream_started, sqlstreamid=ssid)
        self.sqlstreams[ssid]['serverproc'] = proc_server   # store it here, it still runs
        chain.append(proc_server.spawn)

        # 3. Load definitions
        proc_loaddefs = OSSSClientProcess(spawnargs=[sdpport], sqlcommands=self.sqlstreams[ssid]['sql_defs'], binroot=dirname)
        #proc_loaddefs.addCallback(self._sqlstream_defs_loaded, sqlstreamid=ssid)
        chain.append(proc_loaddefs.spawn)

        # 4. Turn pumps on
        proc_pumpson = self.get_pumps_on_proc(ssid)
        #proc_pumpson.addCallback(self._pumps_on_callback, sqlstreamid=ssid)
        chain.append(proc_pumpson.spawn)

        # run chain
        chaindef = chain.run()
        chaindef.addCallback(self._sqlstream_start_chain_success, sqlstreamid=ssid)
        chaindef.addErrback(self._sqlstream_start_chain_failure, sqlstreamid=ssid)
        return chaindef

    def _sqlstream_start_chain_success(self, result, **kwargs):
        ssid = kwargs.get("sqlstreamid")
        log.info(" EVERYTHING WENT BETTER THAN EXPECTED %s" % ssid)
        # inform service

    def _sqlstream_start_chain_failure(self, failure, **kwargs):
        ssid = kwargs.get("sqlstreamid")
        log.error('this is jut the pits %s' % ssid)
        failure.trap(StandardError)
        # inform service

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
    def _sqlstream_started(self, result, *args, **kwargs):
        """
        SQLStream daemon has started.
        """
        ssid = args[0].get('sqlstreamid')
        log.debug("SQLStream server (%s) has started" % ssid)

        return result   # pass result down the chain

    #@defer.inlineCallbacks
    # TODO: probably remove
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

        #defer.returnValue(None)

    @defer.inlineCallbacks
    def op_ctl_sqlstream(self, content, headers, msg):
        """
        Instructs a SQLStream to perform an operation like start or stop its pumps.
        The app controller calls here.
        """
        log.info("ctl_sqlstream received " + content['action'])

        yield self.reply_ok(msg, {'value':'ok'}, {})

        if content['action'] == 'pumps_on':
            #self.pumps_on(content['sqlstreamid'])
            proc_pumpson = self.get_pumps_on_proc(content['sqlstreamid'])
            proc_pumpson.addCallback(self._pumps_on_callback, sqlstreamid=content['sqlstreamid'])
            yield proc_pumpson.spawn()  # TODO: could never return!

        elif content['action'] == 'pumps_off':
            proc_pumpsoff = self.get_pumps_off_proc(content['sqlstreamid'])
            proc_pumpsoff.addCallback(self._pumps_off_callback, sqlstreamid=content['sqlstreamid'])
            yield proc_pumpsoff.spawn()  # TODO: could never return!

    def get_pumps_on_proc(self, sqlstreamid):
        """
        Instructs the given SQLStream worker to turn its pumps on.
        It will begin or resume reading from its queue.
        """

        sql_cmd = """
                  ALTER PUMP "SignalsPump" START;
                  ALTER PUMP "DetectionsPump" START;
                  ALTER PUMP "DetectionMessagesPump" START;
                  """
        sdpport = self.sqlstreams[sqlstreamid]['sdpport']
        binroot = self.sqlstreams[sqlstreamid]['dirname']
        proc_pumpson = OSSSClientProcess(spawnargs=[sdpport], binroot=binroot, sqlcommands=sql_cmd)

        return proc_pumpson

    @defer.inlineCallbacks
    # TODO: DELETE ?
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
    def get_pumps_off_proc(self, sqlstreamid):
        """
        Builds an OSProcess to nstructs the given SQLStream worker to turn its pumps off.
        It will no longer read from its queue after this process is spawned.
        """

        sql_cmd = """
                  ALTER PUMP "SignalsPump" STOP;
                  ALTER PUMP "DetectionsPump" STOP;
                  ALTER PUMP "DetectionMessagesPump" STOP;
                  """
        self.sqlstreams[sqlstreamid]['state'] = 'stopped'

        sdpport = self.sqlstreams[sqlstreamid]['sdpport']
        binroot = self.sqlstreams[sqlstreamid]['dirname']
        proc_pumpsoff = OSSSClientProcess(spawnargs=[sdpport], binroot=binroot, sqlcommands=sql_cmd)

        return proc_pumpsoff
    
    @defer.inlineCallbacks
    # TODO: DELETE ?
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
        content = {'proc_id':self.id.full}
        content.update(kwargs)

        ret = yield self.rpc_send(self.target, operation, content, {}, **kwargs)
        defer.returnValue(ret)

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

class OSSSClientProcess(OSProcess):
    """
    SQLStream client process protocol.
    Upon construction, looks for a sqlcommands keyword argument. If that parameter exists,
    it will execute the commands upon launching the client process.
    """
    def __init__(self, binary=None, spawnargs=[], **kwargs):
        """
        @param sqlcommands  SQL commands to run in the client. May be None, which leaves stdin open.
        @param binroot      Root path to prepend on the binary name.
        """
        self.sqlcommands = kwargs.pop('sqlcommands', None)
        binroot = kwargs.pop("binroot", None)

        if binroot != None:
            binary = os.path.join(binroot, SSC_BIN)

        OSProcess.__init__(self, binary=binary, spawnargs=spawnargs, **kwargs)

        self.temp_file  = None
    
    def spawn(self, binary=None, args=[]):
        """
        Spawns the sqllineClient process.
        This override is to dump our sqlcommands into a file and tell sqllineClient to
        run that file. We modify the args keyword arg and send it up to the baseclass
        spawn implementation.
        """
        newargs = args
        if len(args) == 0:
            newargs.extend(self.spawnargs)

        if self.sqlcommands != None:

            # dump sqlcommands into a temporary file
            f = tempfile.NamedTemporaryFile(delete=False)   # TODO: requires python 2.6
            f.write(self.sqlcommands)
            f.close()

            self.temp_file = f.name

            newargs.append("--run=%s" % self.temp_file)

        return OSProcess.spawn(self, binary, newargs)

    def processEnded(self, reason):
        OSProcess.processEnded(self, reason)

        # remove temp file if we created one earlier
        if self.temp_file != None:
            os.unlink(self.temp_file)

#
#
# ########################################################
#
#

class OSSSServerProcess(OSProcess):
    """
    OSProcess for starting a SQLstream daemon.
    
    Note the spawn method is overridden here to return a deferred which is called back
    when the server reports it is ready instead of when it exits. The deferred for when
    it exits can still be accessed by the deferred_exited attr, or you can simply add
    callbacks to it using the addCallback method. This override is so it can be used in a
    TaskChain.
    """
    def __init__(self, binary=None, spawnargs=[], **kwargs):

        binroot = kwargs.pop("binroot", None)

        if binroot != None:
            binary = os.path.join(binroot, SSD_BIN)

        OSProcess.__init__(self, binary=binary, spawnargs=spawnargs, **kwargs)
        self.ready_deferred = defer.Deferred()

    def spawn(self, binary=None, args=[]):
        """
        Calls the baseclass spawn but returns a deferred that's fired when the daemon
        announces it is ready.
        Then it can be used in a chain.
        """

        OSProcess.spawn(self, binary, args)

        return self.ready_deferred

    def addReadyCallback(self, callback, **kwargs):
        """
        Adds a callback to the deferred returned by spawn in this server override.
        Similar to addCallback, just for a different internal deferred.
        """
        self.ready_deferred.addCallback(callback, kwargs)

    def _close_impl(self, force):
        """
        Safely exit SQLstream daemon.
        Make sure you use a long timeout when calling close, it takes a while.
        """

        # will do the quick shutdown if force, or if we never became ready
        if force or not self.ready_deferred.called:
            self.transport.signalProcess("KILL") # TODO: INT does not interrupt foreground process in a bash script!
                                                 # need to get PID of java process so we can shut it down nicer than this!
            self.transport.loseConnection()
        else:
            self.transport.write('!kill\n')

    #@defer.inlineCallbacks
    def outReceived(self, data):
        OSProcess.outReceived(self, data)
        if (SSD_READY_STRING in data):
            
            # must simulate processEnded callback value
            cba = { 'exitcode' : 0,
                    'outlines' : self.outlines,
                    'errlines' : self.errlines }

            self.ready_deferred.callback(cba)
            #yield self.ready_callback(**self.ready_callbackargs)

    def processEnded(self, reason):
        # Process ended override.
        # This override is to signal ready deferred if we never did, just in case.
        if self.ready_deferred and not self.ready_deferred.called:
            
            # must simulate processEnded callback value
            cba = { 'exitcode' : 0,
                    'outlines' : self.outlines,
                    'errlines' : self.errlines }

            self.ready_deferred.callback(cba)

        OSProcess.processEnded(self, reason)

# Spawn of the process using the module name
factory = ProcessFactory(AppControllerService)

