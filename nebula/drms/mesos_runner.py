
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

import nebula.drms.mesos_runner
import nebula.service
import nebula.docstore


class MesosJob(nebula.service.TaskJob):
    def __init__(self, job_id, task, service):
        super(MesosJob, self).__init__(job_id=job_id, task=task)
        self.service = service
        self.name = service.name + ":" + task.task_id
    
    def to_dict(self):
        return {
            'job_id' : self.job_id,
            'service' : self.service.to_dict(),
            'task' : self.task.to_dict()
        }

class MesosDRMS(nebula.service.Service):

    def __init__(self, config):
        super(MesosDRMS, self).__init__('mesos')
        
        self.config = config
        
        self.id_count = 0

        if "mesos" not in self.config:
            logging.error("Mesos not configured")
            return
        self.driver_thread = None
        self.sched = NebulaMesos(self, config)
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
    
    def submit(self, task, service):
        with self.queue_lock:
            j = self.job_count
            mesos_job = nebula.drms.mesos_runner.MesosJob(job_id=j, task=task, service=service)
            self.job_count += 1
            self.queue[j] = mesos_job
            return mesos_job


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
    def __init__(self, service, config):
        mesos.interface.Scheduler.__init__(self)
        self.service = service
        self.active_tasks = {}
        self.config = config
        self.worker_image = config.get('worker_image', 'nebula')
        self.docstore = nebula.docstore.from_url(self.config['docstore'])
        print "NebulaMesos config: %s" % (config)
        logging.info("Starting Mesos scheduler")

    """
    Mesos Interface Methods
    """
    def getExecutorInfo(self):
        """
        Build an executor request structure
        """

        logging.info("in getExecutorInfo, setting worker image = " + self.worker_image)
        executor = mesos_pb2.ExecutorInfo()
        executor.executor_id.value = "nebula_worker"
        
        """
        container = mesos_pb2.ContainerInfo()
        container.type = container.DOCKER
        docker = mesos_pb2.ContainerInfo.DockerInfo()
        docker.network = docker.BRIDGE
        docker.image = self.worker_image
        container.docker.MergeFrom(docker)
        executor.container.MergeFrom(container)
        
        cmd = "python /opt/bin/nebula worker -v"
        """
        cmd_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "bin/nebula.sh")
        cmd = "bash %s worker -v" % cmd_path
        
        executor.command.value = cmd
        logging.info("Executor Command: %s" % cmd)

        #env_path = executor.command.environment.variables.add()
        #env_path.name = "PATH"
        #env_path.value = os.environ['PATH']
        
        for env_name, env_value in self.config.get("env", {}).items():
            env_e = executor.command.environment.variables.add()
            env_e.name = env_name
            env_e.value = env_value

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
            print request
            task_data = request.to_dict()
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

                j = self.service.get_queued()
                if j is not None:
                    job_id, job = j
                    logging.info("Starting Job: %s" % (job))
                    logging.debug("Offered %d cpus" % (cpu_count))
                    cpu_slice = 1
                    mem_slice = 1024
                    task = self.getTaskInfo(offer, job, cpu_slice, mem_slice)
                    tasks.append(task)
                    job.set_running()
            status = driver.launchTasks(offer.id, tasks)

    def statusUpdate(self, driver, status):
        if status.state == mesos_pb2.TASK_RUNNING:
            logging.info("Task %s, slave %s is RUNNING" % (status.task_id.value, status.slave_id.value))
            #print status.data
        elif status.state == mesos_pb2.TASK_FINISHED:
            logging.info("Task %s, slave %s is FINISHED" % (status.task_id.value, status.slave_id.value))
            data = json.loads(status.data)
            print data
            #print data
            self.service.active[data['job_id']].set_done()
        elif status.state == mesos_pb2.TASK_FAILED:
            logging.info("Task %s, slave %s is FAILED" % (status.task_id.value, status.slave_id.value))
            print status.data
            #print status.data
        else:
            logging.info("Unknown Status Update: %s" % (status.state))
    def getFrameworkName(self, driver):
        return "Nebula"
    
    def queue(self, task):
        self.scheduler.queue(task)
