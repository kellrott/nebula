

from nebula.dag import TaskNode

class Shell(TaskNode):
    def __init__(self, task_id, command_line, inputs, outputs):
        super(Shell,self).__init__(task_id, inputs)
        self.command_line = command_line
        self.outputs = outputs
