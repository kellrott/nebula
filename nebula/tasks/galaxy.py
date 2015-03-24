

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
    def __init__(self, task_id, workflow_file=None, yaml=None, inputs=None, parameters=None, tool_dir=None, tool_data=None, tags=None, galaxy=None, **kwds):
        if galaxy is None:
            kwds['docker'] = "bgruening/galaxy-stable"
        else:
            kwds['docker'] = galaxy

        self.tool_dir = tool_dir
        self.tool_data = tool_data
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
        wf_inputs = {'inputs' : inputs, 'parameters' : parameters}
        if tags is not None:
            wf_inputs['tags'] = tags
        #print "BEFORE!!!", wf_inputs
        wf_req = wf.adjust_input(wf_inputs)
        self.request = wf_req
        #print "PARAMS!!!", json.dumps(self.parameters)
        super(GalaxyWorkflow,self).__init__(task_id, inputs=inputs, **kwds)

        for step in self.data['steps'].values():
            if step['type'] == 'data_input':
                name = step['inputs'][0]['name']
                if name not in self.inputs:
                    raise CompileException("Missing input: %s" % (name))


    def get_task_data(self):
        return {
            'task_id' : self.task_id,
            'service' : 'galaxy',
            'service_parameters' : {
                'tool_dir' : self.tool_dir,
                'tool_data' : self.tool_data
            },
            'workflow' : self.data,
            'request' : self.request,
            'outputs' : self.get_output_data(),
            'docker' : self.docker.name
        }

    def environment(self):
        raise NotImplementedException()
