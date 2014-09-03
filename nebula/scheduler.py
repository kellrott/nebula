
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


class Scheduler:

    def __init__(self, compiler, config):
        self.compiler = compiler
        self.config = config
        self.state = 'starting'
        self.queued_jobs = []
        self.active_targets = {}
        self.workers = {}
        self.tasks = {}
        self.dags = compiler.to_dags()

    def done(self):
        return self.state == 'done'


    def get_work(self, worker, host=None):
        self.workers.update({'host': host})
        best_t = float('inf')
        best_priority = float('-inf')
        best_task = None
        locally_pending_tasks = 0
        running_tasks = []
        
        if len(self.tasks) <= 0:
            self.dags.get_active_tasks()

        for task_id, task in self._tasks.iteritems():
            if worker not in task.workers:
                continue

            if task.status == RUNNING:
                # Return a list of currently running tasks to the client,
                # makes it easier to troubleshoot
                other_worker = self._active_workers[task.worker_running]
                more_info = {'task_id': task_id, 'worker': str(other_worker)}
                if other_worker is not None:
                    more_info.update(other_worker.info)
                running_tasks.append(more_info)

            if task.status != PENDING:
                continue

            locally_pending_tasks += 1
            ok = True
            for dep in task.deps:
                if dep not in self._tasks:
                    ok = False
                elif self._tasks[dep].status != DONE:
                    ok = False

            if ok:
                if (-task.priority, task.time) < (-best_priority, best_t):
                    best_t = task.time
                    best_priority = task.priority
                    best_task = task_id

        if best_task:
            t = self._tasks[best_task]
            t.status = RUNNING
            t.worker_running = worker
            t.time_running = time.time()
            self._update_task_history(best_task, RUNNING, host=host)

        return {'n_pending_tasks': locally_pending_tasks,
                'task_id': best_task,
                'running_tasks': running_tasks}
