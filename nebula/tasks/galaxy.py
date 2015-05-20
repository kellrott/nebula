

import os
import json
import logging

#from nebula.dag import Target, TaskNode, TargetFuture
from nebula.tasks import Task
from nebula.target import Target, TargetFuture

from nebula.exceptions import CompileException
from nebula.galaxy import GalaxyWorkflow

class GalaxyTargetFuture(TargetFuture):

    def __init__(self, task_id, step_id, output_name):
        self.step_id = step_id
        self.output_name = output_name
        super(GalaxyTargetFuture, self).__init__(task_id)

class GalaxyWorkflowTask(Task):
    def __init__(self, task_id, workflow, inputs=None, parameters=None, tags=None, tool_tags=None):

        super(GalaxyWorkflowTask,self).__init__(task_id) #, inputs=inputs, **kwds)

        """
        if workflow_file is not None:
            with open(workflow_file) as handle:
                self.workflow_data = json.loads(handle.read())
        if yaml is not None:
            self.workflow_data = yaml_to_workflow(yaml)

        if workflow is not None:
            self.workflow_data = workflow

        if self.workflow_data is None:
            raise Exception("Workflow not defined")
        """
        if not isinstance(workflow, GalaxyWorkflow):
            raise Exception("Need galaxy workflow")
        self.workflow = workflow
        self.inputs = inputs
        self.parameters = parameters
        self.tags = tags
        self.tool_tags = tool_tags

    def is_valid(self):
        valid = True

        workflow_data = self.workflow.to_dict()
        outputs = {}
        for step in workflow_data['steps'].values():
            if 'post_job_actions' in step and len(step['post_job_actions']):
                for act in step['post_job_actions'].values():
                    if act['action_type'] == 'RenameDatasetAction':
                        new_name = act["action_arguments"]["newname"]
                        old_name = act["output_name"]
                        outputs[new_name] = GalaxyTargetFuture(task_id=self.task_id, step_id=step['id'], output_name=old_name)

        for step in workflow_data['steps'].values():
            if step['type'] == 'data_input':
                name = step['inputs'][0]['name']
                if name not in self.inputs:
                    #raise CompileException("Missing input: %s" % (name))
                    valid = False
        return valid

    def get_inputs(self):
        out = {}
        for k, v in self.inputs.items():
            if isinstance(v, Target):
                out[k] = v
            else:
                logging.error("Unknown Input Type: %s" % (k))
        return out

    @staticmethod
    def from_dict(data):
        request = {}
        for k,v in data['inputs'].items():
            if isinstance(v,dict) and 'uuid' in v:
                request[k] = Target(uuid=v['uuid'])
            else:
                request[k] = v
        return GalaxyWorkflowTask(
            data['task_id'], workflow=GalaxyWorkflow(data['workflow']),
            inputs=request, parameters=data.get('parameters', None),
            tags=data.get('tags', None), tool_tags=data.get('tool_tags', None)
        )

    def to_dict(self):
        inputs = {}
        for k,v in self.inputs.items():
            if isinstance(v,Target):
                inputs[k] = v.to_dict()
            else:
                inputs[k] = v
        return {
            'task_type' : 'GalaxyWorkflow',
            'task_id' : self.task_id,
            'service' : 'galaxy',
            'workflow' : self.workflow.to_dict(),
            'inputs' : inputs,
            'parameters' : self.parameters,
            'tags' : self.tags,
            'tool_tags' : self.tool_tags
            #'outputs' : self.get_output_data(),
        }

    def get_workflow_request(self):
        #FIXME: This code is just copy pasted at the moment
        #need to integrate properly
        dsmap = {}
        parameters = {}
        out = {}
        workflow_data = self.workflow.to_dict()
        for k, v in self.inputs.items():
            if isinstance(v, Target):
                if k in workflow_data['steps']:
                    out[k] = {'src':'uuid', 'id' : v.uuid}
                else:
                    found = False
                    for step_id, step in workflow_data['steps'].items():
                        label = step['uuid']
                        if step['type'] == 'data_input':
                            if step['inputs'][0]['name'] == k:
                                dsmap[label] = {'src':'uuid', 'id' : v.uuid}
            else:
                pass
        if self.parameters is not None:
            for k,v in self.parameters.items():
                if k in workflow_data['steps']:
                    out[k] == v
                else:
                    found = False
                    for step_id, step in workflow_data['steps'].items():
                        label = step['uuid']
                        if step['type'] == 'tool':
                            if step['annotation'] == k:
                                parameters[label] = v

        #TAGS
        if self.tags is not None:
            for step, step_info in workflow_data['steps'].items():
                step_name = step_info['uuid']
                if step_info['type'] == "tool":
                    pja_map = {}
                    for i, output in enumerate(step_info['outputs']):
                        output_name = output['name']
                        pja_map["RenameDatasetActionout_file%s" % (i)] = {
                            "action_type" : "TagDatasetAction",
                            "output_name" : output_name,
                            "action_arguments" : {
                                "tags" : ",".join(self.tags)
                            },
                        }
                    if step_name not in parameters:
                        parameters[step_name] = {} # json.loads(step_info['tool_state'])
                    parameters[step_name]["__POST_JOB_ACTIONS__"] = pja_map
        if self.tool_tags is not None:
            for step, step_info in workflow_data['steps'].items():
                if step_info['type'] == "tool":
                    step_name = None
                    if step_info['label'] in self.tool_tags:
                        step_name = step_info['label']
                    if step_info['annotation'] in self.tool_tags:
                        step_name = step_info['annotation']
                    if step_info['uuid'] in self.tool_tags:
                        step_name = step_info['uuid']
                    step_id = step_info['uuid']
                    if step_name is not None:
                        pja_map = {}
                        for i, output in enumerate(step_info['outputs']):
                            output_name = output['name']
                            if output_name in self.tool_tags[step_name]:
                                pja_map["RenameDatasetActionout_file%s" % (i)] = {
                                    "action_type" : "TagDatasetAction",
                                    "output_name" : output_name,
                                    "action_arguments" : {
                                        "tags" : ",".join(self.tool_tags[step_name][output_name])
                                    },
                                }
                        if len(pja_map):
                            #print "PJA", pja_map
                            if step_name not in parameters:
                                parameters[step_name] = {} # json.loads(step_info['tool_state'])
                            if "__POST_JOB_ACTIONS__" not in parameters[step_name]:
                                parameters[step_id]["__POST_JOB_ACTIONS__"] = {}
                            for k,v in pja_map.items():
                                parameters[step_id]["__POST_JOB_ACTIONS__"][k] = v

        out['workflow_id'] = workflow_data['uuid']
        out['inputs'] = dsmap
        out['parameters'] = parameters
        out['inputs_by'] = "step_uuid"
        return out
