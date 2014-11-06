

import os
import json
from nebula.dag import Target, TaskNode, TargetFuture
from nebula.exceptions import CompileException

class GalaxyTargetFuture(TargetFuture):

    def __init__(self, task_id, step_id, output_name):
        self.step_id = step_id
        self.output_name = output_name
        super(GalaxyTargetFuture, self).__init__(task_id)

class GalaxyWorkflow(TaskNode):
    def __init__(self, task_id, workflow_file, tool_dir=None, **kwds):
        if 'docker' not in kwds:
            kwds['docker'] = "bgruening/galaxy-stable"
        self.workflow_file = os.path.abspath(workflow_file)
        self.tool_dir = None

        #parse the input and validate the inputs
        with open(self.workflow_file) as handle:
            txt = handle.read()
        self.data = json.loads(txt)

        outputs = {}
        for step in self.data['steps'].values():
            if 'post_job_actions' in step and len(step['post_job_actions']):
                for act in step['post_job_actions'].values():
                    if act['action_type'] == 'RenameDatasetAction':
                        new_name = act["action_arguments"]["newname"]
                        old_name = act["output_name"]
                        outputs[new_name] = GalaxyTargetFuture(task_id=task_id, step_id=step['id'], output_name=old_name)
        kwds['outputs'] = outputs
        super(GalaxyWorkflow,self).__init__(task_id, **kwds)
        print kwds['inputs']
        for step in self.data['steps'].values():
            if step['type'] == 'data_input':
                name = step['inputs'][0]['name']
                if name not in self.inputs:
                    raise CompileException("Missing input: %s" % (name))


    def get_task_data(self):
        return {
            'task_id' : self.task_id,
            'service' : 'galaxy',
            'workflow' : self.data,
            'inputs' : self.get_input_data(),
            'outputs' : self.get_output_data(),
            'docker' : self.docker.name
        }

    def environment(self):
        raise NotImplementedException()
