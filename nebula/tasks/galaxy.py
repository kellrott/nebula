

import os
import json

#from nebula.dag import Target, TaskNode, TargetFuture
from nebula.tasks import Task
from nebula.target import Target, TargetFuture

from nebula.exceptions import CompileException
from nebula.galaxy.yaml_to_workflow import yaml_to_workflow
from nebula.galaxy import Workflow

class GalaxyTargetFuture(TargetFuture):

    def __init__(self, task_id, step_id, output_name):
        self.step_id = step_id
        self.output_name = output_name
        super(GalaxyTargetFuture, self).__init__(task_id)

class GalaxyWorkflowTask(Task):
    def __init__(self, task_id, workflow_file=None, yaml=None, workflow=None, inputs=None):

        super(GalaxyWorkflowTask,self).__init__(task_id) #, inputs=inputs, **kwds)

        if workflow_file is not None:
            with open(workflow_file) as handle:
                self.workflow_data = json.loads(handle.read())
        if yaml is not None:
            self.workflow_data = yaml_to_workflow(yaml)

        if workflow is not None:
            self.workflow_data = workflow

        if self.workflow_data is None:
            raise Exception("Workflow not defined")

        self.request = inputs

        """
        outputs = {}
        for step in self.data['steps'].values():
            if 'post_job_actions' in step and len(step['post_job_actions']):
                for act in step['post_job_actions'].values():
                    if act['action_type'] == 'RenameDatasetAction':
                        new_name = act["action_arguments"]["newname"]
                        old_name = act["output_name"]
                        outputs[new_name] = GalaxyTargetFuture(task_id=task_id, step_id=step['id'], output_name=old_name)

        wf = Workflow(self.data)
        wf_inputs = {'inputs' : inputs, 'parameters' : parameters}
        if tags is not None:
            wf_inputs['tags'] = tags
        #print "BEFORE!!!", wf_inputs
        wf_req = wf.adjust_input(wf_inputs)
        self.request = wf_req
        #print "PARAMS!!!", json.dumps(self.parameters)

        for step in self.data['steps'].values():
            if step['type'] == 'data_input':
                name = step['inputs'][0]['name']
                if name not in self.inputs:
                    raise CompileException("Missing input: %s" % (name))
        """

    def is_valid(self):
        valid = True

        outputs = {}
        for step in self.workflow_data['steps'].values():
            if 'post_job_actions' in step and len(step['post_job_actions']):
                for act in step['post_job_actions'].values():
                    if act['action_type'] == 'RenameDatasetAction':
                        new_name = act["action_arguments"]["newname"]
                        old_name = act["output_name"]
                        outputs[new_name] = GalaxyTargetFuture(task_id=self.task_id, step_id=step['id'], output_name=old_name)

        for step in self.workflow_data['steps'].values():
            if step['type'] == 'data_input':
                name = step['inputs'][0]['name']
                if name not in self.request:
                    #raise CompileException("Missing input: %s" % (name))
                    valid = False
        return valid

    @staticmethod
    def from_dict(data):
        request = {}
        for k,v in data['request'].items():
            if isinstance(v,dict) and 'uuid' in v:
                request[k] = Target(uuid=v['uuid'])
            else:
                request[k] = v
        return GalaxyWorkflowTask( data['task_id'], workflow=data['workflow'], inputs=request )

    def get_inputs(self):
        out = {}
        for k, v in self.request.items():
            if isinstance(v, Target):
                out[k] = v
        return out

    def to_dict(self):
        request = {}
        for k,v in self.request.items():
            if isinstance(v,Target):
                request[k] = v.to_dict()
            else:
                request[k] = v
        return {
            'task_type' : 'GalaxyWorkflow',
            'task_id' : self.task_id,
            'service' : 'galaxy',
            'workflow' : self.workflow_data,
            'request' : request,
            #'outputs' : self.get_output_data(),
        }

    def get_workflow_request(self):
        #FIXME: This code is just copy pasted at the moment
        #need to integrate properly
        dsmap = {}
        parameters = {}
        out = {}
        for k, v in input.get("inputs", input.get("ds_map", {})).items():
            if k in self.desc['steps']:
                out[k] == v
            else:
                found = False
                for step in self.steps():
                    label = step.uuid
                    if step.type == 'data_input':
                        if step.inputs[0]['name'] == k:
                            found = True
                            dsmap[label] = {'src':'uuid', 'id' : v.uuid}

        for k, v in input.get("parameters", {}).items():
            if k in self.desc['steps']:
                out[k] == v
            else:
                found = False
                for step in self.steps():
                    label = step.uuid
                    if step.type == 'tool':
                        if step.annotation == k:
                            found = True
                            parameters[label] = v

        #TAGS
        for tag in input.get("tags", []):
            for step, step_info in self.desc['steps'].items():
                step_name = step_info['uuid']
                if step_info['type'] == "tool":
                    pja_map = {}
                    for i, output in enumerate(step_info['outputs']):
                        output_name = output['name']
                        pja_map["RenameDatasetActionout_file%s" % (i)] = {
                            "action_type" : "TagDatasetAction",
                            "output_name" : output_name,
                            "action_arguments" : {
                                "tags" : tag
                            },
                        }
                    if step_name not in parameters:
                        parameters[step_name] = {} # json.loads(step_info['tool_state'])
                    parameters[step_name]["__POST_JOB_ACTIONS__"] = pja_map
        out['workflow_id'] = self.desc['uuid']
        out['inputs'] = dsmap
        out['parameters'] = parameters
        out['inputs_by'] = "step_uuid"
        return out

        raise Exception("Working on this")
        return {

        }
