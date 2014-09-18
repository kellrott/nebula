
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

from dag import READY, PENDING, RUNNING

class Scheduler:

    def __init__(self, dags, workrepo, config):
        logging.debug("Initializing Scheduler")
        self.workrepo = workrepo
        self.config = config
        self.state = 'starting'
        self.queued_jobs = []
        self.active_targets = {}
        self.workers = {}
        self.dags = dags
        self.tasks = []

    def done(self):
        return self.state == 'done'
    
    def activate_tasks(self, max_dags=1):
        ready_tasks = self.dags.get_tasks([READY], 1)
        if len(ready_tasks):
            self.tasks += ready_tasks
            return
        logging.debug("Activating new tasks")
        
        pending_tasks = self.dags.get_tasks([PENDING])
        logging.debug("Found %s pending tasks" % (len(pending_tasks)))

        dag_set = {}
        for task in pending_tasks:
            if task.is_ready():
                if max_dags == 0 or len(dag_set) < max_dags or task.dag_id in dag_set:
                    record = self.workrepo.get_jobrecord()
                    if record is None or not record.match_inputs(task.get_inputs()):
                        logging.debug("Activating Task %s" % (task.task_id))
                        task.state = READY
                        ready_tasks.append(task)
                        dag_set[task.dag_id] = True
                    else:
                        logging.debug("Using stored task %s" % (task.task_id))
        self.tasks += ready_tasks
        
    def get_work(self, worker, host=None):
        self.workers.update({'host': host})
        best_t = float('inf')
        best_priority = float('-inf')
        best_task = None
        locally_ready_tasks = 0
        running_tasks = []

        if len([a for a in self.tasks if a.state == READY]) <= 0:
            self.activate_tasks()
        
        for task in self.tasks:
            if task.state == RUNNING:
                running_tasks.append(task.task_id)
            if task.state != READY:
                continue
            locally_ready_tasks += 1
            if (-task.priority, task.time) < (-best_priority, best_t):
                best_t = task.time
                best_priority = task.priority
                best_task = task
        if best_task:
            best_task.state = RUNNING
            best_task.worker_running = worker
            best_task.time_running = time.time()

        return best_task
