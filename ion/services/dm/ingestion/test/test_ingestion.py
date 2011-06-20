#!/usr/bin/env python

"""
@file ion/services/dm/ingestion/test/test_ingestion.py
@author David Stuebe
@brief test for eoi ingestion demo
"""
from ion.core.exception import ReceivedApplicationError, ReceivedContainerError
from ion.services.dm.distribution.publisher_subscriber import Subscriber

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer
from twisted.trial import unittest

from ion.core import ioninit
from ion.util import procutils as pu
from ion.services.coi.datastore_bootstrap.ion_preload_config import PRELOAD_CFG, ION_DATASETS_CFG, SAMPLE_PROFILE_DATASET_ID, SAMPLE_PROFILE_DATA_SOURCE_ID, TYPE_CFG, NAME_CFG, DESCRIPTION_CFG, CONTENT_CFG, CONTENT_ARGS_CFG, ID_CFG

from ion.services.dm.distribution.events import DatasourceUnavailableEventSubscriber, DatasetSupplementAddedEventSubscriber


from ion.core.process import process
from ion.services.dm.ingestion.ingestion import IngestionClient, SUPPLEMENT_MSG_TYPE, CDM_DATASET_TYPE, DAQ_COMPLETE_MSG_TYPE, PERFORM_INGEST_MSG_TYPE, CREATE_DATASET_TOPICS_MSG_TYPE, EM_URL, EM_ERROR, EM_TITLE, EM_DATASET, EM_END_DATE, EM_START_DATE, EM_TIMESTEPS, EM_DATA_SOURCE
from ion.test.iontest import IonTestCase

from ion.services.coi.datastore_bootstrap.dataset_bootstrap import bootstrap_profile_dataset, BOUNDED_ARRAY_TYPE, FLOAT32ARRAY_TYPE, bootstrap_byte_array_dataset

from ion.services.dm.ingestion.ingestion import CREATE_DATASET_TOPICS_MSG_TYPE

from ion.core.object.object_utils import create_type_identifier


DATASET_TYPE = create_type_identifier(object_id=10001, version=1)
DATASOURCE_TYPE = create_type_identifier(object_id=4503, version=1)
GROUP_TYPE = create_type_identifier(object_id=10020, version=1)


CONF = ioninit.config(__name__)


class FakeDelayedCall(object):

    def cancel(self):
        pass

    def delay(self, int):
        pass

class IngestionTest(IonTestCase):
    """
    Testing service operations of the ingestion service.
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        services = [
            {   'name':'ds1',
                'module':'ion.services.coi.datastore',
                'class':'DataStoreService',
                'spawnargs':
                        {PRELOAD_CFG:
                                 {ION_DATASETS_CFG:True}
                        }
            },

            {
                'name':'resource_registry1',
                'module':'ion.services.coi.resource_registry.resource_registry',
                'class':'ResourceRegistryService',
                    'spawnargs':{'datastore_service':'datastore'}
            },

            {
                'name':'exchange_management',
                'module':'ion.services.coi.exchange.exchange_management',
                'class':'ExchangeManagementService',
            },

            {
                'name':'association_service',
                'module':'ion.services.dm.inventory.association_service',
                'class':'AssociationService'
            },
            {
                'name':'pubsub_service',
                'module':'ion.services.dm.distribution.pubsub_service',
                'class':'PubSubService'
            },

            {   'name':'ingestion1',
                'module':'ion.services.dm.ingestion.ingestion',
                'class':'IngestionService'
            },

            ]

        # ADD PUBSUB AND EMS

        self.sup = yield self._spawn_processes(services)

        self.proc = process.Process(spawnargs={'proc-name':'test_ingestion_proc'})
        yield self.proc.spawn()

        self._ic = IngestionClient(proc=self.proc)

        ingestion1 = yield self.sup.get_child_id('ingestion1')
        log.debug('Process ID:' + str(ingestion1))
        self.ingest= self._get_procinstance(ingestion1)

        ds1 = yield self.sup.get_child_id('ds1')
        log.debug('Process ID:' + str(ds1))
        self.datastore= self._get_procinstance(ds1)


    class fake_msg(object):

        def ack(self):
            return True


    @defer.inlineCallbacks
    def tearDown(self):
        # You must explicitly clear the registry in case cassandra is used as a back end!
        yield self._stop_container()


    @defer.inlineCallbacks
    def test_create_dataset_topics(self):
        """
        """

        msg = yield self.proc.message_client.create_instance(CREATE_DATASET_TOPICS_MSG_TYPE)

        msg.dataset_id = 'ABC'

        result = yield self._ic.create_dataset_topics(msg)

        result.MessageResponseCode = result.ResponseCodes.OK


    @defer.inlineCallbacks
    def test_recv_dataset(self):
        """
        This is a test method for the recv dataset operation of the ingestion service
        """
        #print '\n\n\n Starting Test \n\n\n\n'
        # Reach into the ingestion service and fake the receipt of a perform ingest method - so we can test recv_dataset

        content = yield self.ingest.mc.create_instance(PERFORM_INGEST_MSG_TYPE)
        content.dataset_id = SAMPLE_PROFILE_DATASET_ID

        yield self.ingest._prepare_ingest(content)

        self.ingest.timeoutcb = FakeDelayedCall()

        #print '\n\n\n Got Dataset in Ingest \n\n\n\n'

        # Now fake the receipt of the dataset message
        cdm_dset_msg = yield self.ingest.mc.create_instance(CDM_DATASET_TYPE)
        yield bootstrap_profile_dataset(cdm_dset_msg, supplement_number=1, random_initialization=True)

        #print '\n\n\n Filled out message with a dataset \n\n\n\n'

        # Call the op of the ingest process directly
        yield self.ingest._ingest_op_recv_dataset(cdm_dset_msg, '', self.fake_msg())

        # ==========
        # Can't use messaging and client because the send returns before the op is complete so the result is untestable.
        #yield self._ic.send_dataset(SAMPLE_PROFILE_DATASET_ID,cdm_dset_msg)
        #yield pu.asleep(1)
        # ==========

        self.assertEqual(self.ingest.dataset.ResourceLifeCycleState, self.ingest.dataset.UPDATE)



    @defer.inlineCallbacks
    def test_recv_chunk(self):
        """
        This is a test method for the recv dataset operation of the ingestion service
        """

        #print '\n\n\n Starting Test \n\n\n\n'
        # Reach into the ingestion service and fake the receipt of a perform ingest method - so we can test recv_dataset

        content = yield self.ingest.mc.create_instance(PERFORM_INGEST_MSG_TYPE)
        content.dataset_id = SAMPLE_PROFILE_DATASET_ID

        yield self.ingest._prepare_ingest(content)

        self.ingest.timeoutcb = FakeDelayedCall()

        self.ingest.dataset.CreateUpdateBranch()

        #print '\n\n\n Got Dataset in Ingest \n\n\n\n'

        # Pick a few variables to 'update'
        var_list = ['time', 'depth', 'lat', 'lon', 'salinity']

        for var in var_list:

            yield self.create_and_test_variable_chunk(var)


    @defer.inlineCallbacks
    def create_and_test_variable_chunk(self, var_name):

        group = self.ingest.dataset.root_group
        var = group.FindVariableByName(var_name)
        starting_bounded_arrays  = var.content.bounded_arrays[:]

        supplement_msg = yield self.ingest.mc.create_instance(SUPPLEMENT_MSG_TYPE)
        supplement_msg.dataset_id = SAMPLE_PROFILE_DATASET_ID
        supplement_msg.variable_name = var_name

        self.create_chunk(supplement_msg)

        # Call the op of the ingest process directly
        yield self.ingest._ingest_op_recv_chunk(supplement_msg, '', self.fake_msg())

        updated_bounded_arrays = var.content.bounded_arrays[:]

        # This is all we really need to do - make sure that the bounded array has been added.
        self.assertEqual(len(updated_bounded_arrays), len(starting_bounded_arrays)+1)

        # The bounded array but not the ndarray should be in the ingestion service dataset
        self.assertIn(supplement_msg.bounded_array.MyId, self.ingest.dataset.Repository.index_hash)
        self.assertNotIn(supplement_msg.bounded_array.ndarray.MyId, self.ingest.dataset.Repository.index_hash)

        # The datastore should now have this ndarray
        self.failUnless(self.datastore.b_store.has_key(supplement_msg.bounded_array.ndarray.MyId))


    def create_chunk(self, supplement_msg):
        """
        This method is specialized to create bounded arrays for the Sample profile dataset.
        """



        supplement_msg.bounded_array = supplement_msg.CreateObject(BOUNDED_ARRAY_TYPE)
        supplement_msg.bounded_array.ndarray = supplement_msg.CreateObject(FLOAT32ARRAY_TYPE)

        if supplement_msg.variable_name == 'time':

            tsteps = 3
            tstart = 1280106120
            delt = 3600
            supplement_msg.bounded_array.ndarray.value.extend([tstart + delt*n for n in range(tsteps)])

            supplement_msg.bounded_array.bounds.add()
            supplement_msg.bounded_array.bounds[0].origin = 0
            supplement_msg.bounded_array.bounds[0].size = tsteps

        elif supplement_msg.variable_name == 'depth':
            supplement_msg.bounded_array.ndarray.value.extend([0.0, 0.1, 0.2])
            supplement_msg.bounded_array.bounds.add()
            supplement_msg.bounded_array.bounds[0].origin = 0
            supplement_msg.bounded_array.bounds[0].size = 3

        elif supplement_msg.variable_name == 'salinity':
            supplement_msg.bounded_array.ndarray.value.extend([29.84, 29.76, 29.87, 30.16, 30.55, 30.87])
            supplement_msg.bounded_array.bounds.add()
            supplement_msg.bounded_array.bounds[0].origin = 0
            supplement_msg.bounded_array.bounds[0].size = 2
            supplement_msg.bounded_array.bounds.add()
            supplement_msg.bounded_array.bounds[1].origin = 0
            supplement_msg.bounded_array.bounds[1].size = 3


        supplement_msg.Repository.commit('Commit before fake send...')


    @defer.inlineCallbacks
    def test_recv_done(self):
        """
        This is a test method for the recv dataset operation of the ingestion service
        """

        # Receive a dataset to get setup...
        content = yield self.ingest.mc.create_instance(PERFORM_INGEST_MSG_TYPE)
        content.dataset_id = SAMPLE_PROFILE_DATASET_ID

        yield self.ingest._prepare_ingest(content)

        self.ingest.timeoutcb = FakeDelayedCall()

        # Now fake the receipt of the dataset message
        cdm_dset_msg = yield self.ingest.mc.create_instance(CDM_DATASET_TYPE)
        yield bootstrap_profile_dataset(cdm_dset_msg, supplement_number=1, random_initialization=True)

        # Call the op of the ingest process directly
        yield self.ingest._ingest_op_recv_dataset(cdm_dset_msg, '', self.fake_msg())


        complete_msg = yield self.ingest.mc.create_instance(DAQ_COMPLETE_MSG_TYPE)

        complete_msg.status = complete_msg.StatusCode.OK
        yield self.ingest._ingest_op_recv_done(complete_msg, '', self.fake_msg())



    @defer.inlineCallbacks
    def test_ingest_on_new_dataset(self):
        """
        This is a test method for the recv dataset operation of the ingestion service
        """

        new_dataset_id = 'C37A2796-E44C-47BF-BBFB-637339CE81D0'

        def create_dataset(dataset, *args, **kwargs):
            """
            Create an empty dataset
            """
            group = dataset.CreateObject(GROUP_TYPE)
            dataset.root_group = group
            return True

        data_set_description = {ID_CFG:new_dataset_id,
                      TYPE_CFG:DATASET_TYPE,
                      NAME_CFG:'Blank dataset for testing ingestion',
                      DESCRIPTION_CFG:'An example of a station dataset',
                      CONTENT_CFG:create_dataset,
                      }

        self.datastore._create_resource(data_set_description)

        ds_res = self.datastore.workbench.get_repository(new_dataset_id)


        yield self.datastore.workbench.flush_repo_to_backend(ds_res)

        new_datasource_id = '0B1B4D49-6C64-452F-989A-2CDB02561BBE'
        # ============================================
        # Don't need a real data source at this time!
        # ============================================

        log.info('Created Dataset Resource for test.')

        # Receive a dataset to get setup...
        content = yield self.ingest.mc.create_instance(PERFORM_INGEST_MSG_TYPE)
        content.dataset_id = new_dataset_id
        content.datasource_id = new_datasource_id

        yield self.ingest._prepare_ingest(content)

        self.ingest.timeoutcb = FakeDelayedCall()

        # Now fake the receipt of the dataset message
        cdm_dset_msg = yield self.ingest.mc.create_instance(CDM_DATASET_TYPE)
        yield bootstrap_profile_dataset(cdm_dset_msg, supplement_number=1, random_initialization=True)

        log.info('Calling Receive Dataset')

        # Call the op of the ingest process directly
        yield self.ingest._ingest_op_recv_dataset(cdm_dset_msg, '', self.fake_msg())

        log.info('Calling Receive Dataset: Complete')

        complete_msg = yield self.ingest.mc.create_instance(DAQ_COMPLETE_MSG_TYPE)

        log.info('Calling Receive Done')

        complete_msg.status = complete_msg.StatusCode.OK
        yield self.ingest._ingest_op_recv_done(complete_msg, '', self.fake_msg())

        log.info('Calling Receive Done: Complete!')





    @defer.inlineCallbacks
    def test_notify(self):

        ### Test the unavailable notification
        sub_unavailable = DatasourceUnavailableEventSubscriber(process=self.proc, origin=SAMPLE_PROFILE_DATA_SOURCE_ID)
        yield sub_unavailable.initialize()
        yield sub_unavailable.activate()

        test_deferred = defer.Deferred()

        sub_unavailable.ondata = lambda msg: test_deferred.callback( msg['content'].additional_data.error_explanation)

        data_details = {EM_TITLE:'title',
                       EM_URL:'references',
                       EM_DATA_SOURCE:SAMPLE_PROFILE_DATA_SOURCE_ID,
                       EM_DATASET:SAMPLE_PROFILE_DATASET_ID,
                       EM_ERROR:'ERROR # 1',
                       }
        yield self.ingest._notify_ingest(data_details)
        errors_received = yield test_deferred

        self.assertEqual(errors_received, 'ERROR # 1')


        test_deferred = defer.Deferred()

        data_details = {EM_TITLE:'title',
                       EM_URL:'references',
                       EM_DATA_SOURCE:SAMPLE_PROFILE_DATA_SOURCE_ID,
                       EM_DATASET:SAMPLE_PROFILE_DATASET_ID,
                       EM_ERROR:'ERROR # 2',
                       }
        yield self.ingest._notify_ingest(data_details)
        errors_received = yield test_deferred

        self.assertEqual(errors_received, 'ERROR # 2')


        ### Test the Data Supplement notification
        sub_added = DatasetSupplementAddedEventSubscriber(process=self.proc, origin=SAMPLE_PROFILE_DATASET_ID)
        yield sub_added.initialize()
        yield sub_added.activate()

        sub_added.ondata = lambda msg: test_deferred.callback( msg['content'].additional_data.number_of_timesteps)

        test_deferred = defer.Deferred()

        data_details = {EM_TITLE:'title',
                        EM_URL:'references',
                        EM_DATA_SOURCE:SAMPLE_PROFILE_DATA_SOURCE_ID,
                        EM_DATASET:SAMPLE_PROFILE_DATASET_ID,
                        EM_START_DATE:59,
                        EM_END_DATE:69,
                        EM_TIMESTEPS:7
                        }
        yield self.ingest._notify_ingest(data_details)
        nsteps = yield test_deferred

        self.assertEqual(nsteps, 7)

    @defer.inlineCallbacks
    def test_error_in_ingest(self):
        """
        Attempts to raise an error during the ingestion process to ensure they are trapped and
        reported properly.  We are simulating JAW/DatasetAgent interaction and do a simple "incorrect message type"
        to the first sub-ingestion method.
        """

        # first, create the dataset
        new_dataset_id = 'C37A2796-E44C-47BF-BBFB-637339CE81D0'

        def create_dataset(dataset, *args, **kwargs):
            """
            Create an empty dataset
            """
            group = dataset.CreateObject(GROUP_TYPE)
            dataset.root_group = group
            return True

        data_set_description = {ID_CFG:new_dataset_id,
                      TYPE_CFG:DATASET_TYPE,
                      NAME_CFG:'Blank dataset for testing ingestion',
                      DESCRIPTION_CFG:'An example of a station dataset',
                      CONTENT_CFG:create_dataset,
                      }

        self.datastore._create_resource(data_set_description)

        ds_res = self.datastore.workbench.get_repository(new_dataset_id)


        yield self.datastore.workbench.flush_repo_to_backend(ds_res)

        new_datasource_id = '0B1B4D49-6C64-452F-989A-2CDB02561BBE'

        # ============================================
        # Don't need a real data source at this time!
        # ============================================

        log.info('Created Dataset Resource for test.')

        # now, start ingestion on this fake dataset
        msg = yield self.proc.message_client.create_instance(PERFORM_INGEST_MSG_TYPE)
        msg.dataset_id = new_dataset_id
        msg.reply_to = "fake.respond"
        msg.ingest_service_timeout = 45
        msg.datasource_id = new_datasource_id

        # get a subscriber going for the ingestion ready message
        def_ready = defer.Deferred()
        def readyrecv(data):
            def_ready.callback(True)

        readysub = Subscriber(xp_name="magnet.topic",
                              binding_key="fake.respond",
                              process=self.proc)
        readysub.ondata = readyrecv
        yield readysub.initialize()
        yield readysub.activate()

        # start ingestion, hold its deferred as we need to do something with it in a bit
        ingestdef = self._ic.ingest(msg)

        # wait for ready response from ingestion
        yield def_ready

        log.info("Ready response from ingestion, proceeding to give it an incorrect message type to recv_chunk")

        # now send it an incorrect message, make sure we get an error back
        badmsg = yield self.proc.message_client.create_instance(SUPPLEMENT_MSG_TYPE)
        yield self.proc.send(new_dataset_id, 'recv_dataset', badmsg)

        yield self.failUnlessFailure(ingestdef, ReceivedApplicationError)

        # check called back thing
        self.failUnless("Expected message type" in ingestdef.result.msg_content.MessageResponseBody)
        self.failUnless(ingestdef.result.msg_content.MessageResponseCode, msg.ResponseCodes.BAD_REQUEST)
