
"""
Core classes for working with the Galaxy engine
"""

import json
import logging
import uuid
import tarfile

from nebula import Task, Target, TargetFuture
from nebula.util import engine_from_dict

def getText(nodelist):
    rc = []
    for node in nodelist:
        if node.nodeType == node.TEXT_NODE:
            rc.append(node.data)
    return ''.join(rc)


def dom_scan(node, query):
    stack = query.split("/")
    if node.localName == stack[0]:
        return dom_scan_iter(node, stack[1:], [stack[0]])
    return []

def dom_scan_iter(node, stack, prefix):
    if len(stack):
        for child in node.childNodes:
            if child.nodeType == child.ELEMENT_NODE:
                if child.localName == stack[0]:
                    for out in dom_scan_iter(child, stack[1:], prefix + [stack[0]]):
                        yield out
                elif '*' == stack[0]:
                    for out in dom_scan_iter(child, stack[1:], prefix + [child.localName]):
                        yield out
    else:
        if node.nodeType == node.ELEMENT_NODE:
            yield node, prefix, dict(node.attributes.items()), getText(node.childNodes)
        elif node.nodeType == node.TEXT_NODE:
            yield node, prefix, None, getText(node.childNodes)

class GalaxyResources(object):
    def __init__(self):
        self.tools = []
        self.images = []
    def add_tool_package(self, path, meta=None):
        if meta is None:
            meta = {}
        o = {"meta" : meta}
        o['path'] = path
        self.tools.append(o)
    def add_docker_image_file(self, path, meta=None):
        if meta is None:
            meta = {}
        o = {"meta" : meta}
        o['path'] = path
        self.images.append(o)
    
    def sync(self, ds):
        for tool in self.tools:
            tool_file_id = str(uuid.uuid4())
            t = Target(tool_file_id)
            ds.update_from_file(t, tool['path'], create=True)
            ds.put(t.id, tool['meta'])
            tool['id'] = tool_file_id

        for image in self.images:
            t = tarfile.TarFile(image['path'])
            meta_str = t.extractfile('repositories').read()
            meta = json.loads(meta_str)
            tag, rev_value = meta.items()[0]
            rev, rev_hash = rev_value.items()[0]
            
            image_file_id = None
            for ds_id, meta in ds.filter(type="docker_image", rev_hash=rev_hash):
                image_file_id = ds_id
                print "found", ds_id

            if image_file_id is None:
                image_file_id = str(uuid.uuid4())
                t = Target(image_file_id)
                ds.update_from_file(t, image['path'], create=True)
                meta = dict(image['meta'])
                meta['type'] = 'docker_image'
                meta['tag'] = tag
                meta['rev_tag'] = rev
                meta['rev_hash'] = rev_hash
                ds.put(t.id, meta)
            image['id'] = image_file_id
    
    def to_dict(self):
        return {
            'tools'  : list({"id" : a['id']} for a in self.tools),
            'images' : list({"id" : a['id']} for a in self.images)
        }
    
    @staticmethod
    def from_dict(data):
        out = GalaxyResources()
        for i in data['tools']:
            out.tools.append(i)
        for i in data['images']:
            out.tools.append(i)
        return out

class WorkflowStep(object):
    def __init__(self, workflow, desc):
        self.workflow = workflow
        self.desc = desc
        self.step_id = desc["id"]
        self.uuid = desc['uuid']
        self.type = self.desc['type']
        label = str(self.desc['uuid'])
        if self.desc['label'] is not None:
            label = self.desc['label']
        elif len(self.desc['annotation']):
            label = self.desc['annotation']            
        self.label = label
        self.tool_id = self.desc.get('tool_id', None)
        state = json.loads(self.desc.get('tool_state', "null"))
        self.tool_state = {}
        if self.type == "tool":
            for k, v in state.items():
                if k not in ["__page__", "__rerun_remap_job_id__"]:
                    self.tool_state[k] = json.loads(v)
        elif self.type == "data_input":
            self.tool_state['name'] = state['name']
        self.input_connections = self.desc.get("input_connections", {})
        self.inputs = self.desc.get("inputs", [])
        self.outputs = self.desc.get("outputs", [])
        self.annotation = self.desc.get("annotation", "")

    def validate_input(self, data, tool):
        tool_inputs = tool.get_inputs()
        for tin in tool_inputs:
            value = None
            tin_state = self.find_state(tin)
            if tin_state is not None:
                value = tin_state
            if tool_inputs[tin].type == 'data':
                if value is None:
                    if tin not in self.input_connections:
                        if not tool_inputs[tin].optional:
                            raise ValidationError("Tool %s Missing input dataset: %s.%s" % (self.tool_id, self.step_id, tin))
            else:
                if value is None:
                    if self.step_id not in data['ds_map'] or tin not in data[self.step_id]:
                        if tool_inputs[tin].value is None and not tool_inputs[tin].optional:
                            raise ValidationError("Tool %s Missing input: %s.%s" % (self.tool_id, self.step_id, tin))
                else:
                    if isinstance(value, dict):
                        #if they have missed one of the required runtime values in the pipeline
                        if value.get("__class__", None) == 'RuntimeValue':
                            if self.step_id not in data['parameters'] or tin not in data['parameters'][self.step_id]:
                                raise ValidationError("Tool %s Missing runtime value: %s.%s" % (self.tool_id, self.step_id, tin))

    def find_state(self, param):
        if param.count("|"):
            return self.find_state_rec(param.split("|"), self.tool_state)
        return self.tool_state.get(param, None)

    def find_state_rec(self, params, state):
        if len(params) == 1:
            return state[params[0]]
        if params[0] not in state:
            return None
        return self.find_state_rec(params[1:],state[params[0]])

class ValidationError(Exception):

    def __init__(self, message):
        super(ValidationError, self).__init__(message)

class GalaxyWorkflow(object):
    """
    Document describing Galaxy Workflow
    """
    def __init__(self, workflow=None, ga_file=None):
        if ga_file is not None:
            with open(ga_file) as handle:
                self.desc = json.loads(handle.read())
        else:
            self.desc = workflow

    def to_dict(self):
        return self.desc

    def steps(self):
        for s in self.desc['steps'].values():
            yield WorkflowStep(self, s)

    def get_inputs(self):
        inputs = []
        for step in self.steps():
            if step.type == 'data_input':
                inputs.append(step.label)
        return inputs

    def get_outputs(self, all=False):
        outputs = []
        hidden = self.get_hidden_outputs()
        for step in self.steps():
            if step.type == 'tool':
                for o in step.outputs:
                    output_name = "%s|%s" % (step.label, o['name'])
                    if all or output_name not in hidden:
                        outputs.append( output_name )
        return outputs

    def get_hidden_outputs(self):
        outputs = []
        for step in self.steps():
            if step.type == 'tool' and 'post_job_actions' in step.desc:
                for pja in step.desc['post_job_actions'].values():
                    if pja['action_type'] == 'HideDatasetAction':
                        outputs.append( "%s|%s" % (step.label, pja['output_name']) )
        return outputs

    def validate_input(self, data, toolbox):
        for step in self.steps():
            if step.type == 'tool':
                if step.tool_id not in toolbox:
                    raise ValidationError("Missing Tool: %s" % (step.tool_id))
                tool = toolbox[step.tool_id]
                step.validate_input(data, tool)
            if step.type == 'data_input':
                if step.step_id not in data['ds_map']:
                    raise ValidationError("Missing Data Input: %s" % (step.inputs[0]['name']))
        return True

    def adjust_input(self, input):
        dsmap = {}
        parameters = {}
        out = {}
        for k, v in input.get("inputs", input.get("ds_map", {})).items():
            if k in self.desc['steps']:
                out[k] = v
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
                out[k] = v
            else:
                #found = False
                for step in self.steps():
                    label = step.uuid
                    if step.type == 'tool':
                        if step.annotation == k:
                            #found = True
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

"""
class ToolBox(object):
    def __init__(self):
        self.config_files = {}
        self.tools = {}

    def scan_dir(self, tool_dir):
        #scan through directory looking for tool_dir/*/*.xml files and
        #attempting to load them
        for tool_conf in glob(os.path.join(os.path.abspath(tool_dir), "*", "*.xml")):
            dom = parseXML(tool_conf)
            s = list(dom_scan(dom.childNodes[0], "tool"))
            if len(s):
                if 'id' in s[0][2]:
                    tool_id = s[0][2]['id']
                    self.config_files[tool_id] = tool_conf
                    self.tools[tool_id] = Tool(tool_conf)

    def keys(self):
        return self.tools.keys()

    def __contains__(self, key):
        return key in self.tools

    def __getitem__(self, key):
        return self.tools[key]


class ToolParam(object):
    def __init__(self, name, type, value=None, optional=False, label=""):
        self.name = name
        self.type = type
        self.value = value
        self.optional = optional
        self.label = label

class Tool(object):
    def __init__(self, config_file):
        self.config_file = os.path.abspath(config_file)

        self.inputs = {}
        dom = parseXML(self.config_file)
        s = dom_scan(dom.childNodes[0], "tool/inputs/param")
        for elem, stack, attrs, text in s:
            for name, param in self._param_parse(elem):
                self.inputs[name] = param

        s = dom_scan(dom.childNodes[0], "tool/inputs/conditional")
        for elem, stack, attrs, text in s:
            c = list(dom_scan(elem, "conditional/param"))
            if 'name' in attrs:
                for p_elem, p_stack, p_attrs, p_text in c:
                    for name, param in self._param_parse(p_elem, prefix=attrs['name']):
                        self.inputs[name] = param


    def _param_parse(self, param_elem, prefix=None):
        if 'type' in param_elem.attributes.keys() and 'name' in param_elem.attributes.keys():
            param_name = param_elem.attributes['name'].value
            param_type = param_elem.attributes['type'].value
            if param_type in ['data', 'text', 'integer', 'float', 'boolean', 'select', 'hidden', 'baseurl', 'genomebuild', 'data_column', 'drill_down']:
                optional = False
                if "optional" in param_elem.attributes.keys():
                    optional = bool(param_elem.attributes.get("optional").value)
                label = ""
                if "label" in param_elem.attributes.keys():
                    label = param_elem.attributes.get("label").value
                value = ""
                if "value" in param_elem.attributes.keys():
                    value = param_elem.attributes.get("value").value
                param = ToolParam(name=param_name, type=param_type, value=value, optional=optional, label=label)
                if prefix is None:
                    yield (param_name, param)
                else:
                    yield (prefix + "|" + param_name, param)
            else:
                raise ValidationError('unknown input_type: %s' % (param_type))

    def get_inputs(self):
        return self.inputs

"""

class GalaxyTargetFuture(TargetFuture):

    def __init__(self, task_id, step_id, output_name):
        self.step_id = step_id
        self.output_name = output_name
        super(GalaxyTargetFuture, self).__init__(task_id)

class GalaxyWorkflowTask(Task):
    """
    Instance of a Galaxy Workflow to be run
    """
    def __init__(self, engine, workflow, inputs=None, parameters=None, tags=None, tool_tags=None):
        super(GalaxyWorkflowTask, self).__init__()

        if not isinstance(workflow, GalaxyWorkflow):
            raise Exception("Need galaxy workflow")
        self.engine = engine
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
                        outputs[new_name] = GalaxyTargetFuture(
                            step_id=step['id'],
                            output_name=old_name
                        )

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
    def from_dict(data, engine=None):
        request = {}
        for k, v in data['inputs'].items():
            if isinstance(v, dict) and 'uuid' in v:
                request[k] = Target(uuid=v['uuid'])
            else:
                request[k] = v
        if engine is None:
            engine = engine_from_dict(data['engine'])
        return GalaxyWorkflowTask(
            engine=engine, workflow=GalaxyWorkflow(data['workflow']),
            inputs=request, parameters=data.get('parameters', None),
            tags=data.get('tags', None), tool_tags=data.get('tool_tags', None)
        )

    def to_dict(self):
        inputs = {}
        for k, v in self.inputs.items():
            if isinstance(v, Target):
                inputs[k] = v.to_dict()
            else:
                inputs[k] = v
        return {
            'task_type' : 'GalaxyWorkflow',
            'engine' : self.engine.to_dict(),
            'workflow' : self.workflow.to_dict(),
            'inputs' : inputs,
            'parameters' : self.parameters,
            'tags' : self.tags,
            'tool_tags' : self.tool_tags
            #'outputs' : self.get_output_data(),
        }

    def get_workflow_request(self, uuid_ldda_map={}):
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
                                if v.uuid in uuid_ldda_map:
                                    dsmap[label] = {'src':'ldda', 'id' : uuid_ldda_map[v.uuid]}
                                else:
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
        if self.tags is not None or self.tool_tags is not None:
            for step, step_info in workflow_data['steps'].items():
                step_id = step_info['uuid']
                if step_info['type'] == "tool":
                    step_name = None
                    if self.tool_tags is not None:
                        if step_info['label'] in self.tool_tags:
                            step_name = step_info['label']
                        if step_info['annotation'] in self.tool_tags:
                            step_name = step_info['annotation']
                        if step_info['uuid'] in self.tool_tags:
                            step_name = step_info['uuid']
                    tags = []
                    if self.tags is not None:
                        tags += self.tags

                    pja_map = {}
                    for i, output in enumerate(step_info['outputs']):
                        output_name = output['name']
                        if step_name is not None and output_name in self.tool_tags[step_name]:
                            cur_tags = tags + self.tool_tags[step_name][output_name]
                        else:
                            cur_tags = tags
                        if len(cur_tags):
                            pja_map["RenameDatasetActionout_file%s" % (len(pja_map))] = {
                                "action_type" : "TagDatasetAction",
                                "output_name" : output_name,
                                "action_arguments" : {
                                    "tags" : ",".join(cur_tags)
                                },
                            }

                    if len(pja_map):
                        if step_id not in parameters:
                            parameters[step_id] = {} # json.loads(step_info['tool_state'])
                        parameters[step_id]["__POST_JOB_ACTIONS__"] = pja_map



        out['workflow_id'] = workflow_data['uuid']
        out['inputs'] = dsmap
        out['parameters'] = parameters
        out['inputs_by'] = "step_uuid"
        return out
