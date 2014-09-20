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
import socket
import json
import threading
import traceback
import logging
import subprocess
import tempfile

"""
This program is designed to be modular, and not directly reference any other
code in the nebula library. That way it can be moved out to remote computers and
run as a single file.
"""


logging.basicConfig(level=logging.INFO)


class TaskRunner:
    def __init__(self, desc, config):
        self.desc = desc
        self.config = config

    def start(self):
        #FIXME: put wrapper code here
        self.run(self.desc)

    def run(self, data):
        raise Exception("Not implemented")

class ShellRunner(TaskRunner):

    def run(self, data):
        script = data['script']
        docker_image = data.get('docker', 'base')
        workdir=self.config.workdir
        execdir = os.path.abspath(tempfile.mkdtemp(dir=workdir, prefix="nebula_"))

        with open(os.path.join(execdir, "run.sh"), "w") as handle:
            handle.write(script)

        cmd = [
            "docker", "run", "--rm", "-u", str(os.geteuid()),
            "-v", "%s:/work" % execdir, "-w", "/work", docker_image,
            "/bin/bash", "/work/run.sh"
        ]
        logging.info("executing: " + " ".join(cmd))
        proc = subprocess.Popen(cmd, close_fds=True)
        proc.communicate()
        if proc.returncode != 0:
            raise Exception("Call Failed: %s" % (cmd))


task_runner_map = {
    'shell' : ShellRunner
}

class SubTask(object):
    def __init__(self, driver, task, config):
        self.driver = driver
        self.task = task
        self.config = config

    def run(self):
        logging.info("Running Nebula task: %s" % (self.task.task_id.value))
        nebula_task_id = None
        try:
            obj = json.loads(self.task.data)
            if 'task_type' in obj and obj['task_type'] in task_runner_map:
                nebula_task_id = str(obj['task_id'])
                update = mesos_pb2.TaskStatus()
                update.task_id.value = self.task.task_id.value
                update.state = mesos_pb2.TASK_RUNNING
                update.data = nebula_task_id
                self.driver.sendStatusUpdate(update)
                cl = task_runner_map[obj['task_type']]
                inst = cl(obj, self.config)
                inst.start()
                update = mesos_pb2.TaskStatus()
                update.task_id.value = self.task.task_id.value
                update.data = nebula_task_id
                update.state = mesos_pb2.TASK_FINISHED
                self.driver.sendStatusUpdate(update)
            else:
                raise Exception("Bad task request")

        except Exception, e:
            traceback.print_exc()
            update = mesos_pb2.TaskStatus()
            update.task_id.value = self.task.task_id.value
            update.data = nebula_task_id
            update.state = mesos_pb2.TASK_FAILED
            self.driver.sendStatusUpdate(update)


class NebulaExecutor(mesos.Executor):
    def __init__(self, config):
        mesos.Executor.__init__(self)
        self.config = config

    def init(self, driver, arg):
        logging.info("Starting task worker")

    def launchTask(self, driver, task):
        logging.debug( "Running task %s" % task.task_id.value )
        subtask = SubTask(driver, task, self.config)
        threading.Thread(target=subtask.run).start()

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
    args = parser.parse_args()
    logging.info( "Starting Workflow Watcher" )
    executor = NebulaExecutor(args)
    mesos.MesosExecutorDriver(executor).run()
