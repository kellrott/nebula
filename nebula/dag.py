
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
        self.src_path = None
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
        self.parent_task = None

class TaskNode(object):
    def __init__(self, task_id, inputs, outputs):
        self.task_id = task_id
        self.state = PENDING
        self.dag_id = None
        self.priority = 0.0
        self.time = time.time()

        self.params = {}
        self.inputs = {}
        self.input_tasks = {}
        if inputs is not None:
            self.init_inputs(inputs)
        self.outputs = {}
        if outputs is not None:
            self.init_outputs(outputs)
        
    def init_inputs(self, inputs):
        if inputs is None:
            self.inputs = {}
        else:
            if isinstance(inputs, TaskNode):
                self.inputs[None] = inputs
            else:
                if isinstance(inputs, list):
                    ilist = inputs
                else:
                    ilist = [inputs]
                for iset in ilist:
                    if isinstance(iset, TaskNode):
                        for k, v in iset.get_outputs().items():
                            self.inputs[k] = v
                    else:
                        for k, v in iset.items():
                            if isinstance(v, TargetFile):
                                self.inputs[k] = v

    def init_outputs(self, outputs):
        if outputs is None:
            self.outputs = {}
        else:
            if isinstance(outputs, TargetFile):
                outputs.parent_task = self
                self.outputs[None] = outputs
            else:
                if isinstance(outputs, list) or isinstance(outputs, set):
                    olist = outputs
                else:
                    olist = [outputs]
                for oset in olist:
                    if isinstance(oset, TaskNode):
                        for k, v in oset.get_outputs():
                            self.outputs[k] = v
                    else:
                        for k, v in oset.items():
                            if isinstance(v, TargetFile):
                                v.parent_task = self
                                self.outputs[k] = v
                            elif isinstance(v, TaskNode):
                                for k2, v2 in v.get_outputs().items():
                                    self.outputs[k2] = v2
    
    def get_input_data(self):
        out = {}
        for k, v in self.get_inputs():
            out[k] = v.uuid
        return out


    def __str__(self):
        return "%s(inputs:%s)" % (self.task_id, ",".join(str(a) for a in self.get_inputs().values()))

    #def requires(self):
    #    return self.inputs.values()
    
    def get_inputs(self):
        return self.inputs
    
    def get_outputs(self):
        return self.outputs
    
    def requires(self):
        out = {}
        for a in self.get_inputs().values():
            out[a.parent_task.task_id] = a.parent_task
        return out.values() 
    
    def is_active_task(self):
        "This task node actually does work. NebulaFile tasks carry subtasks, but don't actually do work themselves"
        return True
    
    def sub_targets(self):
        return {}

    def is_ready(self):
        for r in self.requires():
            if not r.is_complete():
                return False
        return True

    def is_complete(self):
        return self.state == DONE
