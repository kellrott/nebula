
from nebula.tasks.python import FunctionCall
from nebula.tasks.galaxy import GalaxyWorkflow
from nebula.tasks.shell import Shell
from nebula.tasks.nebula_task import NebulaTask

__mapping__ = {
    'Workflow' : GalaxyWorkflow,
    'Shell' : Shell,
    'Python' : FunctionCall,
    'Nebula' : NebulaTask
}


def from_dict(data):
    return TaskMapping[data['taskType']].from_dict(data)
