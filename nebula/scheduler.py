
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

import time
import logging

from nebula.dag import READY, PENDING, RUNNING, DONE
from nebula.jobrecord import JobRecord
from nebula.target import TargetFile
from nebula.dag import DagSet

class Scheduler:

    def __init__(self, config):
        logging.debug("Initializing Scheduler")
        self.config = config
        self.state = 'starting'
        self.queued_jobs = []
        self.queued_services = []
        self.workers = {}
        self.dags = DagSet()
        self.tasks = {}
        self.data_locality = {}

    def done(self):
        return self.state == 'done'

    def queue_service(self, service):
        self.queued_services.append(service)

    def get_service(self):
        return self.queued_services.pop()

    """
    def activate_tasks(self, max_dags=1):
        ready_tasks = self.dags.get_tasks([READY], 1)
        if len(ready_tasks):
            for a in ready_tasks:
                self.tasks[a.task_id] = a
            return
        logging.debug("Activating new tasks")

        pending_tasks = self.dags.get_tasks([PENDING])
        logging.debug("Found %s pending tasks" % (len(pending_tasks)))

        dag_set = {}
        for task in pending_tasks:
            if task.is_ready():
                if max_dags == 0 or len(dag_set) < max_dags or task.dag_id in dag_set:
                    record = self.workrepo.get_jobrecord(task.task_id)
                    if record is None or not record.match_inputs(task.get_inputs()):
                        logging.debug("Activating Task %s" % (task.task_id))
                        task.state = READY
                        ready_tasks.append(task)
                        dag_set[task.dag_id] = True
                    else:
                        logging.debug("Using stored task %s" % (task.task_id))
                        task.state=DONE
                        #fix the uuids in the task
                        alter_map = self.dags.dags[task.dag_id].alter_output_map(task.task_id, record.data['outputs'])
            else:
                logging.debug("Task %s not ready" % (task.task_id))
        for a in ready_tasks:
            self.tasks[a.task_id] = a

    def get_task(self, host):
        if len([a for a in self.tasks.values() if a.state == READY]) <= 0:
            self.activate_tasks()

        #FIXME: If nothing is found in the loop below, we may need to call activate_tasks again
        best_task = None
        for task in self.tasks.values():
            if task.state != READY:
                continue
            #get a map of data coverage for task
            data_count, location_map = self.get_task_locality(task.task_id)
            #check the host for location data readiness
            found = False
            if len(task.get_input_data()) == 0:
                best_task = task
                found = True

            if location_map.get(host, 0.0) == data_count:
                best_task = task
                found = True

            if location_map.get(host, 0.0) + location_map.get("*", 0.0) == data_count:
                best_task = task
                found = True

            if not found:
                logging.debug("Data for task %s not local to %s" % (task.task_id, host))
                print self.data_locality
                print location_map
                #TODO: Start scheduling data movement jobs if needed

        if best_task:
            best_task.state = RUNNING
            best_task.time_running = time.time()
        return best_task

    def get_task_locality(self, task_id):
        item_count = 0.0
        out = {}
        inputs = self.tasks[task_id].get_inputs().items()
        for k, v in inputs:
            if not self.datamanager.has_data(v.uuid) and isinstance(v, TargetFile):
                self.datamanager.add_file(v.uuid, path=v.path)
        for k, v in inputs:
            item_count += 1.0
            locs = self.datamanager.get_locations(v.uuid)
            if len(locs):
                for host in locs:
                    out[host] = out.get(host, 0.0) + 1.0
            else:
                logging.debug("Missing Data for task %s : %s" % (task.task_id, k))
        return item_count, out

    def complete_task(self, host, task_id, job_record):
        self.tasks[task_id].state = DONE
        self.workrepo.store_jobrecord(task_id, JobRecord(job_record))
        for k, v in job_record['outputs'].items():
            self.datamanager.add_location(v['uuid'], host)
        del self.tasks[task_id]
    """
