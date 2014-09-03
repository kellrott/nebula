
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
        self.dag_state = {}
    
    def append(self, dag):
        dag_id = len(self.dags)
        self.dags[dag_id] = dag
    
    def get_tasks(self, states=None, limit=0):
        if states is None:
            states = [RUNNING]
        
        out = []
        while True:
            found = False
            if limit != 0 and len(out) >= limit:
                break
            
            for d in self.get_dags(state):
                if limit == 0:
                    rlimit = 0
                else:
                    rlimit = limit - len(out)
                a = d.get_tasks(states, rlimit)
                if len(a):
                    found = True
                    out += a
            if not found:
                break
        return out
        
        
    def get_dags(self, states=None, limit=0):
        if states is None:
            states = [RUNNING]
        out = []
        for i, k in self.dags.items():
            if self.dag_state[i] in states:
                out.append(k)
            if limit > 0 and len(out) >= limit:
                break
        return out

    def get_active_tasks(self, limit=1):
        while self.get_dag_count(states) < limit and self.get_dag_count():
            if c == 0:
                self.activate_dag()
            for i, v in self.dags:
                raise Exception("Not Implemented")
    
class TaskDag:
    def __init__(self, tasks):
        self.tasks = tasks
        self.state = PENDING

    def get_tasks(self, states=None, limit=0):
        if states is None:
            states = [RUNNING]
        out = []
        for i, k in self.dags.items():
            if self.dag_state[i] in states:
                out.append(k)
            if limit > 0 and len(out) >= limit:
                break
        return out

        


class TaskNode:
    def __init__(self):
        self.state = PENDING

    def requires(self):
        return []


class CommandLine(TaskNode):
    def __init__(self, name, inputs):
        self.name = name
        self.inputs = inputs

class GalaxyWorkflow(TaskNode):
    def __init__(self, name, inputs):
        self.name = name
        self.inputs = inputs
    
    def environment(self):
        raise NotImplementedException()
