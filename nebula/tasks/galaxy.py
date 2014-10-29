

import os
import json
from nebula.dag import TaskNode
from nebula.exceptions import CompileException


class GalaxyWorkflow(TaskNode):
    def __init__(self, task_id, workflow_file,  **kwds):
        super(GalaxyWorkflow,self).__init__(task_id, **kwds)
        self.workflow_file = os.path.abspath(workflow_file)
        
        #parse the input and validate the inputs
        with open(self.workflow_file) as handle:
            txt = handle.read()
        data = json.loads(txt)
        
        for step in data['steps'].values():
            if step['type'] == 'data_input':
                name = step['inputs'][0]['name']
                if name not in self.inputs:
                    raise CompileException("Missing input: %s" % (name))

    def environment(self):
        raise NotImplementedException()
