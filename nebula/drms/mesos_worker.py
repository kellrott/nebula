#!/usr/bin/env python

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


#try:
#    from pesos.executor import PesosExecutorDriver as MesosExecutorDriver
#except ImportError:
from mesos.native import MesosExecutorDriver
from mesos.interface import mesos_pb2
from mesos.interface import Executor

from argparse import ArgumentParser
import sys
import time
import os
import re
import socket
import json
import threading
import traceback
import logging
import subprocess
import tempfile
import uuid
import hashlib
import shutil
from glob import glob

from nebula.service import TaskJob, GalaxyService
from nebula.service import from_dict as service_from_dict
from nebula.tasks import from_dict as task_from_dict

logging.basicConfig(level=logging.INFO)

uuid4hex = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', re.I)


class MesosJob(threading.Thread):
    def __init__(self, driver, task):
        threading.Thread.__init__(self)
        self.driver = driver
        self.mesos_task = task
        self.mesos_task_data = json.loads(self.mesos_task.data)
        self.nebula_service_data = self.mesos_task_data['service']
        self.nebula_task_data = self.mesos_task_data['task']
        self.job_id = self.mesos_task_data['job_id']
        logging.debug("TaskData: %s" % (json.dumps(self.mesos_task_data, indent=4)))
        self.service_type = self.nebula_service_data['service_type']

    def set_running(self):
        logging.info("Running Mesos job: %s (Nebula Job %s)" % (self.mesos_task.task_id.value, self.job_id))
        nebula_task_id = None
        #try:
        update = mesos_pb2.TaskStatus()
        update.task_id.value = self.mesos_task.task_id.value
        update.state = mesos_pb2.TASK_RUNNING
        update.data = json.dumps( { "job_id" : self.job_id } )
        self.driver.sendStatusUpdate(update)
        #except 
    
    def set_done(self):
        logging.info("Finished Nebula job: %s" % (self.mesos_task.task_id.value))
        update = mesos_pb2.TaskStatus()
        update.task_id.value = self.mesos_task.task_id.value
        update.data = json.dumps( { "job_id" : self.job_id } )
        update.state = mesos_pb2.TASK_FINISHED
        self.driver.sendStatusUpdate(update)
    
    def set_error(self, msg):
        update = mesos_pb2.TaskStatus()
        update.task_id.value = self.mesos_task.task_id.value
        update.data = json.dumps( { "job_id" : self.job_id, "error_message" : msg } )
        update.state = mesos_pb2.TASK_FAILED
        self.driver.sendStatusUpdate(update)
    
    def run(self):
        try:
            logging.debug( "Service: %s" % (self.service_type) )
            service = service_from_dict(self.nebula_service_data)
            service.start()
            task = task_from_dict(self.nebula_task_data)
            job = service.submit(task)
            self.set_running()
            service.wait([job])
            if job.get_status() not in ['ok']:
                sys.stderr.write("---ERROR---\n")
                sys.stderr.write(job.error_msg + "\n")
                sys.stderr.write("---ERROR---\n")
            service.stop()
            self.set_done()
        except Exception, e:
            self.set_error(traceback.format_exc())


class NebulaExecutor(Executor):
    def __init__(self, config):
        logging.debug("Initing Executor")
        Executor.__init__(self)
        self.config = config
        self.tasks = []
        logging.debug("Executor starting")

    #def init(self, driver, arg):
    #    logging.info("Starting task worker")

    def launchTask(self, driver, task):
        logging.debug( "Running task %s" % task.task_id.value )
        job = MesosJob(driver, task)
        job.start()
        self.tasks.append(job)

    def killTask(self, driver, task_id):
        logging.debug( "Killing task %s" % task_id.value )
        update = mesos_pb2.TaskStatus()
        update.task_id.value = task_id.value
        update.state = mesos_pb2.TASK_FINISHED
        update.data = json.dumps( { 'hostname' : socket.gethostname(), 'task_id' : task_id.value } )
        driver.sendStatusUpdate(update)

    def frameworkMessage(self, driver, message):
        # Send it back to the scheduler.
        if message == "diskspace":
            pass

    def shutdown(self, driver):
        logging.debug( "shutdown" )
        #cleanup()

    def error(self, driver, code, message):
        logging.error( "Error: %s" % message )


def run_worker(config):
    executor = NebulaExecutor(config)
    driver = MesosExecutorDriver(executor)
    logging.info("Starting Mesos Executor")
    driver.run()
