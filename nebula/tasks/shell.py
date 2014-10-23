
import logging
import subprocess

from nebula.dag import TaskNode

class Shell(TaskNode):
    def __init__(self, task_id, script, **kwds):
        super(Shell,self).__init__(task_id, **kwds)
        self.script = script

    def get_task_data(self, workrepo):
        return {
            'task_id' : self.task_id,
            'task_type' : 'shell',
            'script' : self.script,
            'inputs' : self.get_input_data(),
            'outputs' : self.get_output_data(),
            'docker' : self.docker.name
        }

    def to_dict(self):
        rval = super(Shell,self).to_dict()
        rval['task_type'] = 'Shell'
        rval['task_id'] = self.task_id
        rval['script'] = self.script
        return rval
