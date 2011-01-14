#!/usr/bin/env python

"""
@file ion/services/coi/resource_registry_beta/test/test_resource_client.py
@author David Stuebe
@brief test service for registering resources and client classes
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer
from twisted.trial import unittest

from net.ooici.core.type import type_pb2
from net.ooici.play import addressbook_pb2
from ion.core.object import gpb_wrapper
from ion.core.object import workbench
from ion.core.object import object_utils

from ion.services.coi.resource_registry_beta.resource_registry import ResourceRegistryClient
from ion.services.coi.resource_registry_beta.resource_client import ResourceClient, ResourceInstance
from ion.services.coi.resource_registry_beta.resource_client import ResourceClientError, ResourceInstanceError
from ion.test.iontest import IonTestCase


addresslink_type = object_utils.create_type_identifier(object_id=20003, version=1)
person_type = object_utils.create_type_identifier(object_id=20001, version=1)

class ResourceClientTest(IonTestCase):
    """
    Testing service classes of resource registry
    """
        
        
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        #self.sup = yield self._start_core_services()
        services = [
            {'name':'ds1','module':'ion.services.coi.datastore','class':'DataStoreService',
             'spawnargs':{'servicename':'datastore'}},
            {'name':'resource_registry1','module':'ion.services.coi.resource_registry_beta.resource_registry','class':'ResourceRegistryService',
             'spawnargs':{'datastore_service':'datastore'}}]
        sup = yield self._spawn_processes(services)
            
        self.rrc = ResourceRegistryClient(proc=sup)
        self.rc = ResourceClient(proc=sup)
        self.sup = sup

    @defer.inlineCallbacks
    def tearDown(self):
        # You must explicitly clear the registry in case cassandra is used as a back end!
        yield self._stop_container()
        
    @defer.inlineCallbacks
    def test_create_resource(self):
                
        resource = yield self.rc.create_instance(addresslink_type, name='Test AddressLink Resource', description='A test resource')
        
        self.assertIsInstance(resource, ResourceInstance)
        self.assertEqual(resource.ResourceLifeCycleState, resource.NEW)
        self.assertEqual(resource.ResourceName, 'Test AddressLink Resource')
        self.assertEqual(resource.ResourceDescription, 'A test resource')
        
    @defer.inlineCallbacks
    def test_get_resource(self):
                        
        resource = yield self.rc.create_instance(addresslink_type, name='Test AddressLink Resource', description='A test resource')
            
        res_id = resource.ResourceIdentity
            
        # Spawn a completely separate resource client and see if we can retrieve the resource...
        services = [
            {'name':'my_process','module':'ion.core.process.process','class':'Process'}]
        
        sup = yield self._spawn_processes(services)
        
        child_ps1 = yield self.sup.get_child_id('my_process')
        log.debug('Process ID:' + str(child_ps1))
        proc_ps1 = self._get_procinstance(child_ps1)
        
        my_rc = ResourceClient(proc=proc_ps1)
            
        my_resource = yield my_rc.get_instance(res_id)
            
        self.assertEqual(my_resource.ResourceName, 'Test AddressLink Resource')
        
    
    @defer.inlineCallbacks
    def test_read_your_writes(self):
            
        resource = yield self.rc.create_instance(addresslink_type, name='Test AddressLink Resource', description='A test resource')
            
        self.assertEqual(resource.ResourceType.GPBMessage, addresslink_type)
            
        
        person = resource.CreateObject(person_type)
        resource.person.add()
        resource.person[0] = person
            
        resource.owner = person
            
        person.id=5
        person.name='David'
        
        self.assertEqual(resource.person[0].name, 'David')
        
        yield self.rc.put_instance(resource, 'Testing write...')
            
        res_id = resource.ResourceIdentity
        
        # Spawn a completely separate resource client and see if we can retrieve the resource...
        services = [
            {'name':'my_process','module':'ion.core.process.process','class':'Process'}]
        
        sup = yield self._spawn_processes(services)
        
        child_ps1 = yield self.sup.get_child_id('my_process')
        log.debug('Process ID:' + str(child_ps1))
        proc_ps1 = self._get_procinstance(child_ps1)
        
        my_rc = ResourceClient(proc=proc_ps1)
            
        my_resource = yield my_rc.get_instance(res_id)
            
        self.assertEqual(my_resource.ResourceName, 'Test AddressLink Resource')
        
        my_resource._repository.log_commits('master')
            
        self.assertEqual(my_resource.person[0].name, 'David')
        
        
    @defer.inlineCallbacks
    def test_version_resource(self):
            
        
        # Create the resource object    
        resource = yield self.rc.create_instance(addresslink_type, name='Test AddressLink Resource', description='A test resource')
                
        person = resource.CreateObject(person_type)
        
        resource.person.add()
        person.id=5
        person.name='David'
        
        resource.person[0] = person
        
        yield self.rc.put_instance(resource, 'Testing write...')
        
        first_version = self.rc.reference_instance(resource)
        
        resource.VersionResource()
        
        person.name = 'Paul'
        
        # The resource must be committed before it can be referenced
        self.assertRaises(workbench.WorkBenchError, self.rc.reference_instance, resource, current_state=True)
        
        yield self.rc.put_instance(resource, 'Testing version!')
        
        second_version = self.rc.reference_instance(resource)
        
        # Spawn a completely separate resource client and see if we can retrieve the resource...
        services = [
            {'name':'my_process','module':'ion.core.process.process','class':'Process'}]
        
        sup = yield self._spawn_processes(services)
        
        child_ps1 = yield self.sup.get_child_id('my_process')
        log.debug('Process ID:' + str(child_ps1))
        proc_ps1 = self._get_procinstance(child_ps1)
        
        my_rc = ResourceClient(proc=proc_ps1)
            
        my_resource_1 = yield my_rc.get_instance(first_version)
            
        self.assertEqual(my_resource_1.ResourceName, 'Test AddressLink Resource')
        
        self.assertEqual(my_resource_1.person[0].name, 'David')
        
        my_resource_2 = yield my_rc.get_instance(second_version)
            
        self.assertEqual(my_resource_2.ResourceName, 'Test AddressLink Resource')
        
        self.assertEqual(my_resource_2.person[0].name, 'Paul')
        
        self.assertRaises(gpb_wrapper.OOIObjectError, getattr, my_resource_1, 'person') 
        
        