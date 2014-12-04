import sys

import yaml
import json
import uuid

try:
    from collections import OrderedDict
except ImportError:
    from galaxy.util.odict import odict as OrderedDict


STEP_TYPE_ALIASES = {
    'input': 'data_input',
    'input_collection': 'data_collection_input',
}


def yaml_to_workflow(has_yaml):
    as_python = yaml.load(has_yaml)

    if isinstance(as_python, list):
        as_python = {"steps": as_python}

    __ensure_defaults(as_python, {
        "a_galaxy_workflow": "true",
        "format-version": "0.1",
        "annotation": "",
        "name": "Workflow",
        "uuid" : str(uuid.uuid4())
    })

    steps = as_python["steps"]

    conversion_context = ConversionContext()
    if isinstance(steps, list):
        steps_as_dict = OrderedDict()
        for i, step in enumerate(steps):
            steps_as_dict[ str(i) ] = step
            if "id" not in step:
                step["id"] = i

            if "label" in step:
                label = step["label"]
                conversion_context.labels[label] = i

            if "position" not in step:
                # TODO: this really should be optional in Galaxy API.
                step["position"] = {
                    "left": 10 * i,
                    "top": 10 * i
                }

        as_python["steps"] = steps_as_dict
        steps = steps_as_dict

    for i, step in steps.iteritems():
        step_type = step.get("type", "tool")
        step_type = STEP_TYPE_ALIASES.get(step_type, step_type)
        if step_type not in [ "data_input", "data_collection_input", "tool", "pause"]:
            raise Exception("Unknown step type encountered %s" % step_type)
        step["type"] = step_type
        eval("transform_%s" % step_type)(conversion_context, step)

    return as_python


def transform_data_input(context, step):
    transform_input(context, step, default_name="Input dataset")


def transform_data_collection_input(context, step):
    transform_input(context, step, default_name="Input dataset collection")


def transform_input(context, step, default_name):
    default_name = step.get("label", default_name)
    __ensure_defaults( step, {
        "annotation": "",
    })

    __ensure_inputs_connections(step)

    if not "inputs" in step:
        step["inputs"] = [{}]

    step_inputs = step["inputs"][0]
    if "name" in step_inputs:
        name = step_inputs["name"]
    else:
        name = default_name

    __ensure_defaults( step_inputs, {
        "name": name,
        "description": "",
    })
    tool_state = {
        "name": name
    }
    if "collection_type" in step:
        tool_state["collection_type"] = step["collection_type"]

    __populate_tool_state(step, tool_state)


def transform_tool(context, step):
    if "tool_id" not in step:
        raise Exception("Tool steps must define a tool_id.")

    __ensure_defaults( step, {
        "annotation": "",
    } )
    __ensure_inputs_connections(step)

    tool_state = {
        # TODO: Galaxy should not require tool state actually specify a __page__.
        "__page__": 0,
    }

    if "connect" not in step:
        step["connect"] = {}

    connect = step["connect"]
    del step["connect"]

    def append_link(key, value):
        if key not in connect:
            connect[key] = []
        connect[key].append(value["$link"])

    def replace_links(value, key=""):
        if __is_link(value):
            append_link(key, value)
            return None
        if isinstance(value, dict):
            new_values = {}
            for k, v in value.iteritems():
                new_key = __join_prefix(key, k)
                new_values[k] = replace_links(v, new_key)
            return new_values
        elif isinstance(value, list):
            new_values = []
            for i, v in enumerate(value):
                # If we are a repeat we need to modify the key
                # but not if values are actually $links.
                if __is_link(v):
                    append_link(key, v)
                    new_values.append(None)
                else:
                    new_key = "%s_%d" % ( key, i )
                    new_values.append(replace_links(v, new_key))
            return new_values
        else:
            return value

    if "state" in step:
        step_state = step["state"]
        step_state = replace_links(step_state)

        for key, value in step_state.iteritems():
            tool_state[key] = json.dumps(value)
        del step["state"]

    for key, values in connect.iteritems():
        input_connection_value = []
        if not isinstance(values, list):
            values = [ values ]
        for value in values:
            if not isinstance(value, dict):
                value_parts = str(value).split("#")
                if len(value_parts) == 1:
                    value_parts.append("output")
                id = value_parts[0]
                if id in context.labels:
                    id = context.labels[id]
                value = {"id": int(id), "output_name": value_parts[1]}
            input_connection_value.append(value)
        # TODO: this should be a list
        step["input_connections"][key] = input_connection_value[0]

    __populate_tool_state(step, tool_state)


class ConversionContext(object):

    def __init__(self):
        self.labels = {}


def __is_link(value):
    return isinstance(value, dict) and "$link" in value


def __join_prefix(prefix, key):
    if prefix:
        new_key = "%s|%s" % (prefix, key)
    else:
        new_key = key
    return new_key


def __ensure_inputs_connections(step):
    if "input_connections" not in step:
        step["input_connections"] = {}


def __ensure_defaults(in_dict, defaults):
    for key, value in defaults.items():
        if key not in in_dict:
            in_dict[ key ] = value


def __populate_tool_state(step, tool_state):
    step["tool_state"] = json.dumps(tool_state)


def main(argv):
    with open(argv[1]) as handle:
        print json.dumps(yaml_to_workflow(handle.read()), indent=4)


if __name__ == "__main__":
    main(sys.argv)
