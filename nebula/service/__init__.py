

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
import sys
import time
import json
import logging
import traceback

from threading import Thread, RLock
from nebula.exceptions import NotImplementedException


class Service(Thread):
    def __init__(self, name):
        super(Service, self).__init__()
        self.name = name
        self.queue_lock = RLock()
        self.queue = {}
        self.active = {}

        self.running = True
        self.job_count = 0
        self.exception = None
        self.exception_str = None

    def get_config(self):
        raise NotImplementedException()

    def submit(self, task):
        with self.queue_lock:
            j = self.job_count
            job = TaskJob(j, task)
            self.job_count += 1
            self.queue[j] = job
            return job

    def get_queued(self):
        with self.queue_lock:
            if len(self.queue):
                job_id, job = self.queue.popitem()
                self.active[job_id] = job
                return job_id, job
        return None

    def is_ready(self):
        return True

    def run(self):
        try:
            self.runService()
        except Exception, e:
            self.exception_str = traceback.format_exc()
            logging.error("Service Failure:" + str(e))
            self.exception = e

    def in_error(self):
        if self.exception is not None:
            print self.exception_str
        return self.exception is not None

    def stop(self):
        self.running = False

    def wait(self, items):
        sleep_time = 1
        collected = []
        while True:
            waiting = False
            #print "Waiting", items
            for i in items:
                status = self.status(i.job_id)
                #print "Status", i, status, self.is_ready()
                logging.info("Status check %s %s" % (status, i))
                if status in ['ok'] and i.job_id not in collected:
                    logging.info("Collecting outputs of %s" % (i.job_id))
                    for name, dataset in i.get_outputs().items():
                        self.store_meta(dataset, self.docstore)
                        self.store_data(dataset, self.docstore)
                    collected.append(i.job_id)
                if status not in ['ok', 'error', 'unknown']:
                    waiting = True
            if not waiting:
                break
            if self.in_error():
                break
            time.sleep(sleep_time)
            if sleep_time < 60:
                sleep_time += 1

    def status(self, job_id):
        with self.queue_lock:
            if job_id in self.queue:
                return "waiting"
            if job_id in self.active:
                return "active"
            return "unknown"

    def get_job(self, job_id):
        with self.queue_lock:
            if job_id in self.queue:
                return self.queue[job_id]
            if job_id in self.active:
                return self.active[job_id]
        return None

    def store_data(self, data, object_store):
        raise NotImplementedException()


class ServiceConfig:
    def __init__(self, **kwds):
        self.config = kwds

    def store(self, handle):
        handle.write(json.dumps(self.config))

    def load(self, handle):
        line = handle.readline()
        self.config = json.loads(line)

    def create(self):
        return from_dict(self.config)


class TaskJob:
    def __init__(self, job_id, task):
        self.task = task
        #self.service_name = self.task['service']
        self.history = None
        self.outputs = {}
        self.error_msg = None
        self.job_id = job_id
        self.state = "queued"

    def set_running(self):
        pass

    def set_done(self):
        pass

    def set_error(self, msg="Failure"):
        self.error_msg = msg
        self.state = 'error'

    def get_inputs(self):
        return self.task.get_inputs()

    def get_outputs(self):
        return self.outputs

    def get_status(self):
        return self.state

    """
    def get_parameters(self):
        return self.task['request']['parameters']

    def get_outputs(self):
        return self.outputs
    """

from galaxy import GalaxyService

__mapping__ = {
    'Galaxy' : GalaxyService
}

def from_dict(data):
    return __mapping__[data['service_type']].from_dict(data)
