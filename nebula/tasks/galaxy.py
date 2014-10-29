

import os
import json
from nebula.dag import TaskNode
from nebula.exceptions import CompileException


class GalaxyWorkflow(TaskNode):
    def __init__(self, task_id, workflow_file,  **kwds):
        if 'docker' not in kwds:
            kwds['docker'] = "bgruening/galaxy-stable"
        super(GalaxyWorkflow,self).__init__(task_id, **kwds)
        self.workflow_file = os.path.abspath(workflow_file)

        #parse the input and validate the inputs
        with open(self.workflow_file) as handle:
            txt = handle.read()
        self.data = json.loads(txt)

        for step in self.data['steps'].values():
            if step['type'] == 'data_input':
                name = step['inputs'][0]['name']
                if name not in self.inputs:
                    raise CompileException("Missing input: %s" % (name))

    def get_task_data(self, workrepo):
        return {
            'task_id' : self.task_id,
            'task_type' : 'galaxy',
            'workflow' : self.data,
            'inputs' : self.get_input_data(),
            'outputs' : self.get_output_data(),
            'docker' : self.docker.name
        }

    def environment(self):
        raise NotImplementedException()
