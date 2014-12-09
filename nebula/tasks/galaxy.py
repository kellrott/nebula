

import os
import json
from nebula.dag import Target, TaskNode, TargetFuture
from nebula.exceptions import CompileException
from nebula.galaxy.yaml_to_workflow import yaml_to_workflow
from nebula.galaxy import Workflow

class GalaxyTargetFuture(TargetFuture):

    def __init__(self, task_id, step_id, output_name):
        self.step_id = step_id
        self.output_name = output_name
        super(GalaxyTargetFuture, self).__init__(task_id)

class GalaxyWorkflow(TaskNode):
    def __init__(self, task_id, workflow_file=None, yaml=None, tool_dir=None, **kwds):
        if 'docker' not in kwds:
            kwds['docker'] = "bgruening/galaxy-stable"

        self.tool_dir = None
        self.data = None

        if workflow_file is not None:
            with open(workflow_file) as handle:
                self.data = json.loads(handle.read())
        if yaml is not None:
            self.data = yaml_to_workflow(yaml)

        if self.data is None:
            raise Exception("Workflow not defined")

        outputs = {}
        for step in self.data['steps'].values():
            if 'post_job_actions' in step and len(step['post_job_actions']):
                for act in step['post_job_actions'].values():
                    if act['action_type'] == 'RenameDatasetAction':
                        new_name = act["action_arguments"]["newname"]
                        old_name = act["output_name"]
                        outputs[new_name] = GalaxyTargetFuture(task_id=task_id, step_id=step['id'], output_name=old_name)
        
        kwds['outputs'] = outputs
        wf = Workflow(self.data)
        conf_kwds = {}
        for k,v in kwds.items():
            if k != 'inputs':
                conf_kwds[k] = v
        wf_req = wf.adjust_input(kwds.get("inputs", {}), label_translate=False, ds_translate=False)
        conf_kwds['inputs'] = wf_req['ds_map']
        self.parameters = wf_req['parameters']
        super(GalaxyWorkflow,self).__init__(task_id, **conf_kwds)

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
            'parameters' : self.parameters,
            'outputs' : self.get_output_data(),
            'docker' : self.docker.name
        }

    def environment(self):
        raise NotImplementedException()
