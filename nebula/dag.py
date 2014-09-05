
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
import time

from nebula.exceptions import NotImplementedException


PENDING = 'PENDING'
READY = 'READY'
FAILED = 'FAILED'
DONE = 'DONE'
RUNNING = 'RUNNING'
UNKNOWN = 'UNKNOWN'


class DagSet:
    def __init__(self):
        self.dags = {}

    def append(self, dag):
        dag_id = len(self.dags)
        self.dags[dag_id] = dag

    def get_tasks(self, states=None, limit=0):
        if states is None:
            states = [RUNNING]
        out = []
        for d in self.get_dags(states):
            if limit == 0:
                rlimit = 0
            else:
                rlimit = limit - len(out)
            a = d.get_tasks(states, rlimit)
            out += a
            if limit > 0 and len(out) >= limit:
                break
        return out

    def get_dags(self, states=None, limit=0):
        if states is None:
            states = [RUNNING]
        out = []
        for i, k in self.dags.items():
            if k.state in states:
                out.append(k)
            if limit > 0 and len(out) >= limit:
                break
        return out

class TaskDag(object):
    def __init__(self, dag_id, tasks):
        self.tasks = tasks
        self.state = PENDING
        for t in self.tasks:
            self.tasks[t].dag_id = dag_id

    def get_tasks(self, states=None, limit=0):
        if states is None:
            states = [RUNNING]
        out = []
        for i, k in self.tasks.items():
            if k.state in states:
                out.append(k)
            if limit > 0 and len(out) >= limit:
                break
        return out

    def __str__(self):
        return "[%s]" % (",".join(str(a) for a in self.tasks.values()))

class TargetFile(object):

    def __init__(self, path):
        self.path = path

class TaskNode(object):
    def __init__(self, task_id, inputs):
        self.task_id = task_id
        self.state = PENDING
        self.dag_id = None
        self.priority = 0.0
        self.time = time.time()

        self.inputs = {}
        self.params = {}

        if isinstance(inputs, TaskNode):
            self.inputs[None] = inputs
        else:
            if isinstance(inputs, list):
                ilist = inputs
            else:
                ilist = [inputs]
            for iset in ilist:
                for k, v in iset.items():
                    if isinstance(v, TargetFile):
                        self.inputs[i]

    def __str__(self):
        return "%s(inputs:%s)" % (self.task_id, ",".join(a.task_id for a in self.requires()))

    def requires(self):
        return self.inputs.values()

    def is_ready(self):
        for r in self.requires():
            if not r.is_complete():
                return False
        return True

    def is_complete(self):
        return self.state == DONE

class CommandLine(TaskNode):
    def __init__(self, task_id, inputs):
        super(CommandLine,self).__init__(task_id, inputs)

class FunctionCall(TaskNode):
    def __init__(self, task_id, function, inputs):
        super(FunctionCall,self).__init__(task_id, inputs)
        self.function = function

class GalaxyWorkflow(TaskNode):
    def __init__(self, task_id, workflow_file, inputs):
        super(GalaxyWorkflow,self).__init__(task_id, inputs)
        self.workflow_file = os.path.abspath(workflow_file)

    def environment(self):
        raise NotImplementedException()
