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

import mesos
import mesos_pb2

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

from nebula.service import TaskJob, GalaxyService, ShellService

logging.basicConfig(level=logging.INFO)

uuid4hex = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', re.I)


class MesosJob(TaskJob):
    def __init__(self, driver, task, config):
        self.driver = driver
        self.task = task
        self.config = config
        self.task_data = json.loads(self.task.data)
        self.service_name = self.task_data['service']

    def set_running(self):
        logging.info("Running Nebula job: %s" % (self.task.task_id.value))
        nebula_task_id = None
        try:
            nebula_task_id = str(self.task_data['task_id'])
            update = mesos_pb2.TaskStatus()
            update.task_id.value = self.task.task_id.value
            update.state = mesos_pb2.TASK_RUNNING
            update.data = nebula_task_id
            self.driver.sendStatusUpdate(update)
    
    def set_done(self):
        logging.info("Finished Nebula job: %s" % (self.task.task_id.value))
        update = mesos_pb2.TaskStatus()
        update.task_id.value = self.task.task_id.value
        update.data = str(self.task_data['task_id'])
        update.state = mesos_pb2.TASK_FINISHED
        self.driver.sendStatusUpdate(update)
    
    def set_error(self):
        update = mesos_pb2.TaskStatus()
        update.task_id.value = self.task.task_id.value
        update.data = str(self.task_data['task_id'])
        update.state = mesos_pb2.TASK_FAILED
        self.driver.sendStatusUpdate(update)


class NebulaExecutor(mesos.Executor):
    def __init__(self, config):
        mesos.Executor.__init__(self)
        self.config = config
        self.services = {}

    def init(self, driver, arg):
        logging.info("Starting task worker")

    def launchTask(self, driver, task):
        logging.debug( "Running task %s" % task.task_id.value )
        job = TaskJob(driver, task, self.config)
        if job.service_name not in self.services:
            s = service_map[job.service_name]()
            self.services[job.service_name] = s
            s.start()
        self.services[job.service_name].submit(job)

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
        print "Error: %s" % message

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-w", "--workdir", default="/tmp")
    parser.add_argument("--docker", default=None)
    parser.add_argument("--storage-dir", default="/tmp/nebula-store")
    args = parser.parse_args()
    logging.info( "Starting Workflow Watcher" )
    executor = NebulaExecutor(args)
    mesos.MesosExecutorDriver(executor).run()
