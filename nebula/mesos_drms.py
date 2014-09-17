
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

import json
import logging
import threading

import mesos
import mesos_pb2

import nebula.drms

class MesosDRMS(nebula.drms.DRMSWrapper):

    def __init__(self, scheduler, config):
        super(MesosDRMS, self).__init__(scheduler, config)

        if self.config.mesos is None:
            logging.error("Mesos not configured")
            return
        self.sched = NebularMesos(scheduler, config)
        self.framework = mesos_pb2.FrameworkInfo()
        self.framework.user = "" # Have Mesos fill in the current user.
        self.framework.name = "Nebula"
        ## additional authentication stuff would go here
        self.driver = mesos.MesosSchedulerDriver(self.sched, self.framework, self.config.mesos)

    def start(self):
        logging.info("Starting Mesos Thread")
        self.driver_thread = DriverThread(self.driver)
        self.driver_thread.start()

    def stop(self):
        logging.info("Stoping Mesos Thread")
        self.driver_thread.stop()

class DriverThread(threading.Thread):
    def __init__(self, driver):
        threading.Thread.__init__(self)
        self.driver = driver

    def run(self):
        self.driver.run() #this doesn't return until stop is called by another thread

    def stop(self):
        self.driver.stop()

class NebularMesos(mesos.Scheduler):
    """
    The GridScheduler is responsible for deploying and managing child Galaxy instances using Mesos
    """
    def __init__(self, scheduler, config):
        mesos.Scheduler.__init__(self)
        self.scheduler = scheduler
        self.config = config
        self.hosts = {}
        self.master_url = "http://%s:%d" % (self.config.host, self.config.port)
        logging.info("Starting Mesos scheduler")
        logging.info("Mesos Resource URL %s" % (self.master_url))


    def getExecutorInfo(self, request):
        """
        Build an executor request structure
        """

        uri_value = "%s/resources/nebula_executor.py" % (self.master_url)
        logging.info("in getExecutorInfo, setting execPath = " + uri_value)
        executor = mesos_pb2.ExecutorInfo()
        executor.executor_id.value = "nebula_worker"

        uri = executor.command.uris.add()
        uri.value = uri_value
        uri.executable = True

        executor.command.value = "./nebula_executor.py"
        executor.name = "nebula_worker"
        executor.source = "nebula_farm"
        return executor

    def getTaskInfo(self, offer, request, accept_cpu, accept_mem):

        task_name = "nebula_worker:%s:%s" % (request.task_id, offer.hostname)
        task = mesos_pb2.TaskInfo()
        task.task_id.value = task_name
        task.slave_id.value = offer.slave_id.value
        task.name = "Nebula Worker"
        task.executor.MergeFrom(self.getExecutorInfo(request))

        task_data = request.get_task_data()
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
        #wq = WorkQueue(self.app.model.context)

        for offer in offers:
            #store the offer info
            cpu_count = 0
            for res in offer.resources:
                if res.name == 'cpus':
                    cpu_count = int(res.scalar.value)

            mem_count = 0
            for res in offer.resources:
                if res.name == 'mem':
                    mem_count = int(res.scalar.value)

            tasks = []

            if offer.hostname not in self.hosts:
                work = self.scheduler.get_work(offer.hostname)
                if work is not None:
                    logging.info("Starting work: %s" % (work))
                    logging.debug("Offered %d cpus" % (cpu_count))
                    cpu_request = 0
                    cpu_slice = 1
                    mem_slice = 1024
                    task = self.getTaskInfo(offer, work, cpu_slice, mem_slice)
                    cpu_request += cpu_slice
                    tasks.append(task)
                    self.hosts[offer.hostname] = self.hosts.get(offer.hostname, 0) + cpu_slice
            status = driver.launchTasks(offer.id, tasks)


    def statusUpdate(self, driver, status):
        if status.state == mesos_pb2.TASK_RUNNING:
            logging.info("Task %s, slave %s is RUNNING" % (status.task_id.value, status.slave_id.value))

        if status.state == mesos_pb2.TASK_FINISHED:
            logging.info("Task %s, slave %s is FINISHED" % (status.task_id.value, status.slave_id.value))


    def getFrameworkName(self, driver):
        return "GalaxyGrid"
