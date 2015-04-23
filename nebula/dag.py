
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
import json
import time
import logging
from glob import glob
import subprocess

from nebula.docker import Docker
from nebula.exceptions import NotImplementedException
from nebula.exceptions import CompileException

PENDING = 'PENDING'
READY = 'READY'
FAILED = 'FAILED'
DONE = 'DONE'
RUNNING = 'RUNNING'
UNKNOWN = 'UNKNOWN'


class DagSet:
    def __init__(self):
        self.dags = {}

    def save(self, basedir):
        for k, v in self.dags.items():
            path = os.path.join(basedir, str(k) + ".json")
            with open(path, "w") as handle:
                handle.write(json.dumps(v.to_dict()))

    @staticmethod
    def load(basedir):
        out = DagSet()
        for path in glob(os.path.join(basedir, "*.json")):
            with open(path) as handle:
                data = json.loads(handle.read())
            dag = TaskDag.from_dict(data)
            out.dags[dag.dag_id] = dag
        return out

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
            self.tasks[t].dag = self

    def to_dict(self):
        return {
            'id' : self.dag_id,
            'tasks' : dict( (k, v.to_dict()) for k,v in self.tasks.items() )
        }

    @staticmethod
    def from_dict(data):
        dag_id = data['id']
        out = TaskDag(dag_id, {})
        tasks = {}
        for k, v in data['tasks'].items():
            tasks[k] = TaskNode.from_dict(v)
            tasks[k].dag = out
        out.tasks = tasks
        return out


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

        #print "dag", self.dag_id, self.tasks
        for dep in self.tasks.values():
            if dep.has_requirement(task_id):
                logging.debug("Found dependent task of %s : %s" % (task_id, dep.task_id))
                t = dep.get_inputs()
                for k, v in t.items():
                    if v.uuid in uuid_remap:
                        logging.debug("Changing %s input target file uuid from %s to %s" % (task.task_id, v.uuid, uuid_remap[v.uuid]))
                        v.uuid = uuid_remap[v.uuid]
            #else:
            #    logging.info("%s not dependent on %s" % (dep.task_id, task_id))


    def __str__(self):
        return "[%s]" % (",".join(str(a) for a in self.tasks.values()))



class TaskNode(object):
    def __init__(self, task_id, inputs=None, outputs=None, task_type=None, dag_id=None):
        self.task_id = task_id
        self.task_type = task_type
        self.state = PENDING
        self.priority = 0.0
        self.time = time.time()

        self.dag_id = dag_id
        self.dag = None

        self.inputs = {}
        self.input_tasks = {}
        if inputs is not None:
            self.init_inputs(inputs)
        self.outputs = {}
        if outputs is not None:
            self.init_outputs(outputs)


    @staticmethod
    def from_dict(data):
        #ugly hack to prevent circular import at startup
        module = __import__("nebula.tasks").tasks
        return module.__mapping__[data['task_type']](**data)

    def to_dict(self):
        return {
            'dag_id' : self.dag_id,
            'inputs' : dict( (k, v.to_dict()) for k, v in self.inputs.items() )
        }

    def init_inputs(self, inputs):
        for k, v in inputs.items():
            if isinstance(v, Target):
                self.inputs[k] = v
            else:
                raise Exception("Not implemented")
                self.inputs[k] = TargetFuture(v['task_id'], v['uuid'])

    def init_outputs(self, outputs):
        for k, v in outputs.items():
            if isinstance(v, TargetFuture):
                v.parent_task = self
                self.outputs[k] = v
            elif isinstance(v, basestring):
                t = TargetFuture(self.task_id)
                t.parent_task = self
                self.outputs[k] = t
            else:
                raise CompileException("Bad output path")

    def get_input_data(self):
        out = {}
        for k, v in self.get_inputs().items():
            out[k] = { 'uuid' : v.uuid }
        return out

    def get_output_data(self):
        out = {}
        for k, v in self.get_outputs().items():
            out[k] = { 'uuid' : v.uuid }
        return out

    def __str__(self):
        return "%s(inputs:%s)" % (self.task_id, ",".join(str(a) for a in self.get_inputs().values()))

    def get_inputs(self):
        return self.inputs

    def get_outputs(self):
        return self.outputs

    def has_requirement(self, task_id):
        if len( list(a for a in self.get_inputs().values() if a.parent_task_id == task_id) ):
            return True
        return False

    def requires(self):
        if self.dag is None:
            raise Exception("Node DAG parent is None")
        out = {}
        for a in self.get_inputs().values():
            if a.parent_task_id is not None:
                out[a.parent_task_id] = self.dag.tasks[a.parent_task_id]
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


class TaskFuture(object):
    """
    A task that will be run in the future
    """
    def __init__(self, task):
        self.task = task

    def __getitem__(self, name):
        a = TargetFuture(self.task.task_id)
        if name in self.task.outputs:
            a.uuid = self.task.outputs[name].uuid
        return a

    def keys(self):
        return self.task.outputs.keys()

    def __str__(self):
        return "TaskFuture{%s}" % (",".join(self.task.get_outputs().keys()))
