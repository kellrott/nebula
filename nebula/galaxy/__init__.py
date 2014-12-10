
import os
import json
from glob import glob
from xml.dom.minidom import parse as parseXML


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
            yield node, prefix, dict(node.attributes.items()), getText( node.childNodes )
        elif node.nodeType == node.TEXT_NODE:
            yield node, prefix, None, getText( node.childNodes )



class WorkflowStep(object):
    def __init__(self, workflow, desc):
        self.step_id = desc["id"]
        self.workflow = workflow
        self.desc = desc
        self.type = self.desc['type']
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

class Workflow(object):
    def __init__(self, desc):
        self.desc = desc

    def steps(self):
        for s in self.desc['steps'].values():
            yield WorkflowStep(self, s)

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

    def adjust_input(self, input, label_translate=True, ds_translate=True):
        dsmap = {}
        parameters = {}
        out = {}
        for k, v in input.get("ds_map", {}).items():
            if k in self.desc['steps']:
                out[k] == v
            else:
                found = False
                for step in self.steps():
                    label = k
                    if label_translate:
                        label = step.step_id
                    #if they referred to a named input
                    if step.type == 'data_input':
                        if step.inputs[0]['name'] == k:
                            found = True
                            if ds_translate:
                                dsmap[label] = {'src':'uuid', 'id' : v.uuid}
                            else:
                                dsmap[label] = v

        for k, v in input.get("parameters", {}).items():
            if k in self.desc['steps']:
                out[k] == v
            else:
                found = False
                for step in self.steps():
                    label = k
                    if label_translate:
                        label = step.step_id
                    if step.type == 'tool':
                        if step.annotation == k:
                            found = True
                            parameters[label] = v
        out['ds_map'] = dsmap
        out['parameters'] = parameters
        return out

class ToolBox(object):
    def __init__(self):
        self.config_files = {}
        self.tools = {}

    def scan_dir(self, tool_dir):
        """
        scan through directory looking for tool_dir/*/*.xml files and
        attempting to load them
        """
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
