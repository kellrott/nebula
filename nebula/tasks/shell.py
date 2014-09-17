

from nebula.dag import TaskNode

class Shell(TaskNode):
    def __init__(self, task_id, command_line, inputs, outputs):
        print "Shell outputs", outputs
        super(Shell,self).__init__(task_id, inputs, outputs)
        self.command_line = command_line

    def get_task_data(self):
        return {
            'task_type' : 'shell',
            'command_line' : self.command_line,
            'outputs' : dict( (k, v.path) for k,v in self.outputs.items())
        }
