

from nebula.dag import TaskNode


class GalaxyWorkflow(TaskNode):
    def __init__(self, task_id, workflow_file, inputs):
        super(GalaxyWorkflow,self).__init__(task_id, inputs)
        self.workflow_file = os.path.abspath(workflow_file)

    def environment(self):
        raise NotImplementedException()
