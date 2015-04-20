

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
