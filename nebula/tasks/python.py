

from nebula.dag import TaskNode


class FunctionCall(TaskNode):
    def __init__(self, task_id, function, inputs):
        super(FunctionCall,self).__init__(task_id, inputs, None)
        self.function = function
