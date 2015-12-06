
import json
from nebula.exceptions import NotImplementedException

class Task(object):
    def __init__(self, task_id):
        self.task_id = task_id

    def get_inputs(self):
        raise NotImplementedException()

    def environment(self):
        raise NotImplementedException()

    def is_valid(self):
        raise NotImplementedException()

class TaskGroup(object):
    def __init__(self):
        self.tasks = []

    def append(self, task):
        self.tasks.append(task)

    def to_dict(self):
        return list( a.to_dict() for a in self.tasks )

    def store(self, handle):
        for a in self.tasks:
            handle.write( json.dumps(a.to_dict()) + "\n" )

    def load(self, handle):
        for line in handle:
            self.tasks.append( from_dict(json.loads(line)) )

    def __len__(self):
        return len(self.tasks)

    def __iter__(self):
        return self.tasks.__iter__()



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


from nebula.tasks.galaxy import GalaxyWorkflowTask

__mapping__ = {
    'GalaxyWorkflow' : GalaxyWorkflowTask,
}

def from_dict(data):
    return __mapping__[data['task_type']].from_dict(data)
