
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
import uuid
import time
import logging

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
        self.dag_id = dag_id
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

    def alter_output_map(self, task_id, output_remap):
        uuid_remap = {}
        task = self.tasks[task_id]
        t = task.get_outputs()
        for k, v in output_remap.items():
            uuid_remap[t[k].uuid] = v['uuid']
            logging.debug("Changing %s output target file uuid from %s to %s" % (task.task_id, t[k].uuid, v['uuid']))
            t[k].uuid = v['uuid']

        print "dag", self.dag_id, self.tasks
        for dep in self.tasks.values():
            if dep.has_requirement(task_id):
                logging.debug("Found dependent task of %s : %s" % (task_id, dep.task_id))
                t = dep.get_inputs()
                for k, v in t.items():
                    if v.uuid in uuid_remap:
                        logging.debug("Changing %s input target file uuid from %s to %s" % (task.task_id, v.uuid, uuid_remap[v.uuid]))
                        v.uuid = uuid_remap[v.uuid]
            else:
                logging.info("%s not dependent on %s" % (dep.task_id, task_id))


    def __str__(self):
        return "[%s]" % (",".join(str(a) for a in self.tasks.values()))

class TargetFile(object):
    def __init__(self, path):
        self.path = path
        self.uuid = str(uuid.uuid4())
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
        for k, v in inputs.items():
            self.inputs[k] = v

        """
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
        """

    def init_outputs(self, outputs):
        for k, v in outputs.items():
            t = TargetFile(v)
            t.parent_task = self
            self.outputs[k] = t

        """
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
        """

    def get_input_data(self):
        out = {}
        for k, v in self.get_inputs().items():
            out[k] = { 'uuid' : v.uuid }
        return out

    def get_output_data(self):
        out = {}
        for k, v in self.get_outputs().items():
            out[k] = { 'uuid' : v.uuid, 'path' : v.path }
        return out

    def __str__(self):
        return "%s(inputs:%s)" % (self.task_id, ",".join(str(a) for a in self.get_inputs().values()))

    #def requires(self):
    #    return self.inputs.values()

    def get_inputs(self):
        return self.inputs

    def get_outputs(self):
        return self.outputs

    def has_requirement(self, task_id):
        if len( list(a for a in self.get_inputs().values() if a.parent_task.task_id == task_id) ):
            return True
        return False

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
                print r, "not complete"
                return False
        return True

    def is_complete(self):
        return self.state == DONE


class TaskFuture:
    """
    A task that will be run in the future
    """
    def __init__(self, task):
        self.task = task

    def __getitem__(self, name):
        a = TargetFuture(self.task)
        if name in self.task.outputs:
            a.uuid = self.task.outputs[name].uuid
        return a

class TargetFuture:
    """
    Task output that will be generated in the future
    """
    def __init__(self, parent_task):
        self.parent_task = parent_task
        self.uuid = str(uuid.uuid4())
