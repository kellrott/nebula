
import os
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
        self.workflow = workflow
        self.desc = desc
        self.type = self.desc['type']
        self.tool_id = self.desc.get('tool_id', None)
    
    def validate_input(self, data, tool):
        print tool.get_inputs()


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
                print step.tool_id
                if step.tool_id not in toolbox:
                    raise ValidationError("Missing Tool: %s" % (step.tool_id))
                tool = toolbox[step.tool_id]
                step.validate_input(data, tool)
                

        return True

class ToolBox(object):
    def __init__(self):
        self.config_files = {}
        self.tools = {}

    def scan_dir(self, tool_dir):
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
            if param_type in ['data', 'text', 'integer', 'float', 'boolean', 'select', 'hidden']:
                if prefix is None:
                    yield (param_name, param_type)
                else:
                    yield (prefix + "." + param_name, param_type)
            else:
                raise ValidationError('unknown input_type: %s' % (param_type))
    
    def get_inputs(self):
        return self.inputs
