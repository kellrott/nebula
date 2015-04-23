
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


from nebula.tasks.python import FunctionCall
from nebula.tasks.galaxy import GalaxyWorkflowTask
from nebula.tasks.shell import Shell
from nebula.tasks.nebula_task import NebulaTask

__mapping__ = {
    'GalaxyWorkflow' : GalaxyWorkflowTask,
    'Shell' : Shell,
    'Python' : FunctionCall,
    'Nebula' : NebulaTask
}

def from_dict(data):
    return __mapping__[data['task_type']].from_dict(data)
