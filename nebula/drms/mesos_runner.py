
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import os
import json
import time
import logging
import threading

import mesos.interface
import mesos.native
from mesos.interface import mesos_pb2

import nebula.drms
import nebula.service

class MesosService(nebula.service.Service):
    def __init__(self, service):
        self.service = service
        super(MesosService, self).__init__("MesosService(%s)" % (service.name))
        self.name = "mesos:%s" % (service.name)

class MesosDRMS(nebula.drms.DRMSWrapper):

    def __init__(self, scheduler, config):
        super(MesosDRMS, self).__init__(scheduler, config)

        if "mesos" not in self.config:
            logging.error("Mesos not configured")
            return
        self.driver_thread = None
        self.sched = NebularMesos(scheduler, config)
        self.framework = mesos_pb2.FrameworkInfo()
        self.framework.user = "" # Have Mesos fill in the current user.
        self.framework.name = "Nebula"
        ## additional authentication stuff would go here
        self.driver = mesos.native.MesosSchedulerDriver(self.sched, self.framework, self.config['mesos'])

    def start(self):
        logging.info("Starting Mesos Thread")
        self.driver_thread = DriverThread(self.driver)
        self.driver_thread.start()

    def stop(self):
        logging.info("Stoping Mesos Thread")
        if self.driver_thread is not None:
            self.driver_thread.stop()

    def deploy_service(self, service):
        mesos_service = MesosService(service)
        self.sched.queue_service(mesos_service)
        return mesos_service


class DriverThread(threading.Thread):
    def __init__(self, driver):
        threading.Thread.__init__(self)
        self.driver = driver

    def run(self):
        self.driver.run() #this doesn't return until stop is called by another thread

    def stop(self):
        self.driver.stop()

class NebulaMesos(mesos.interface.Scheduler):
    """
    The GridScheduler is responsible for deploying and managing child Galaxy instances using Mesos
    """
    def __init__(self, scheduler):
        mesos.interface.Scheduler.__init__(self)
        self.scheduler = scheduler
        self.services = {}
        self.active_tasks = {}
        logging.info("Starting Mesos scheduler")


    """
    API Interface Methods
    """
    def queue_service(self, service):
        self.scheduler.queue_service(service)

    """
    Mesos Interface Methods
    """
    def getExecutorInfo(self):
        """
        Build an executor request structure
        """

        uri_value = "%s/resources/nebula_worker.egg" % (self.master_url)
        logging.info("in getExecutorInfo, setting execPath = " + uri_value)
        executor = mesos_pb2.ExecutorInfo()
        executor.executor_id.value = "nebula_worker"

        uri = executor.command.uris.add()
        uri.value = uri_value
        uri.executable = True

        cmd = "python ./nebula_worker.egg -w %s " % (self.docstore.get_url())

        executor.command.value = cmd
        logging.info("Executor Command: %s" % cmd)

        env_path = executor.command.environment.variables.add()
        env_path.name = "PATH"
        env_path.value = os.environ['PATH']

        executor.name = "nebula_worker"
        executor.source = "nebula_farm"
        return executor

    def getTaskInfo(self, offer, request, accept_cpu, accept_mem):

        if request is None:
            task_name = "%s:%s" % (offer.hostname, "system")
        else:
            task_name = "%s:%s" % (offer.hostname, request.name)
        task = mesos_pb2.TaskInfo()
        task.task_id.value = task_name
        task.slave_id.value = offer.slave_id.value
        task.name = "Nebula Worker"
        task.executor.MergeFrom(self.getExecutorInfo())

        if request is not None:
            task_data = {} #FIXME: request.get_task_data(self.workrepo)
            print task_data
            task.data = json.dumps(task_data)

        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = accept_cpu

        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = accept_mem
        return task


    def registered(self, driver, fid, masterInfo):
        logging.info("Nebula registered with frameworkID %s" % fid.value)

    def resourceOffers(self, driver, offers):
        logging.debug("Got %s slot offers" % len(offers))
        batch_ready = {}

        for offer in offers:
            tasks = []
            if self.config.get("max_servers", 0) <= 0 or len(self.active_tasks) < self.config['max_servers']:
                #store the offer info
                cpu_count = 0
                for res in offer.resources:
                    if res.name == 'cpus':
                        cpu_count = int(res.scalar.value)

                mem_count = 0
                for res in offer.resources:
                    if res.name == 'mem':
                        mem_count = int(res.scalar.value)

                service = self.scheduler.get_service()
                if service is not None:
                    logging.info("Starting Service: %s" % (service))
                    logging.debug("Offered %d cpus" % (cpu_count))
                    cpu_slice = 1
                    mem_slice = 1024
                    task = self.getTaskInfo(offer, service, cpu_slice, mem_slice)
                    tasks.append(task)
            status = driver.launchTasks(offer.id, tasks)

    def statusUpdate(self, driver, status):
        if status.state == mesos_pb2.TASK_RUNNING:
            logging.info("Task %s, slave %s is RUNNING" % (status.task_id.value, status.slave_id.value))
            #print status.data

        if status.state == mesos_pb2.TASK_FINISHED:
            logging.info("Task %s, slave %s is FINISHED" % (status.task_id.value, status.slave_id.value))
            data = json.loads(status.data)
            #print data
            if data['task_type'] == 'fileScan':
                logging.info("Received worker file scan")
                for k, v in data['outputs'].items():
                    self.scheduler.add_data_location(k, status.slave_id.value)
            else:
                self.scheduler.complete_task(status.slave_id.value, data['task_id'], data)

        if status.state == mesos_pb2.TASK_FAILED:
            logging.info("Task %s, slave %s is FAILED" % (status.task_id.value, status.slave_id.value))
            #print status.data

    def getFrameworkName(self, driver):
        return "Nebula"
