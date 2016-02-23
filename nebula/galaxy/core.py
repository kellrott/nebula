
"""
Core classes for working with the Galaxy engine
"""

import os
import json
import logging
import uuid
import tarfile
import shutil
import tempfile
import subprocess

from nebula import Task, Target, TargetFuture
from nebula.util import engine_from_dict
from nebula.galaxy.io import GalaxyWorkflow
from nebula import warpdrive

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
        self.tool_dirs = []
        self.images = []
    def add_tool_package(self, path, meta=None):
        if meta is None:
            meta = {}
        o = {"meta" : meta}
        o['path'] = path
        self.tools.append(o)
    
    def add_tool_dir(self, path):
        self.tool_dirs.append(path)

    
    def add_docker_image_file(self, path, meta=None):
        if meta is None:
            meta = {}
        o = {"meta" : meta}
        o['path'] = path
        self.images.append(o)
    
    def sync(self, ds, workdir="./", docker_host=None, docker_sudo=False):
        
        image_dir = tempfile.mkdtemp(dir=workdir, prefix="nebula_pack_")
        if not os.path.exists(image_dir):
            os.mkdir(image_dir)

        images = []
        tools = []
        
        for tooldir in self.tool_dirs:
            for tool_id, tool_conf, docker_tag in warpdrive.tool_dir_scan(tooldir):
                if docker_tag is not None:
                    dockerfile = os.path.join(os.path.dirname(tool_conf), "Dockerfile")
                    if os.path.exists(dockerfile):
                        warpdrive.call_docker_build(
                            host=docker_host,
                            sudo=docker_sudo,
                            no_cache=False,
                            tag=docker_tag,
                            dir=os.path.dirname(tool_conf)
                        )
                            
                    image_file = os.path.join(image_dir, "docker_" + docker_tag.split(":")[0].replace("/", "_") + ".tar")
                    warpdrive.call_docker_save(
                        host=docker_host,
                        sudo=docker_sudo,
                        tag=docker_tag,
                        output=image_file
                    )
                    self.add_docker_image_file(image_file)

                archive_dir = os.path.dirname(tool_conf)        
                archive_name = os.path.basename(os.path.dirname(tool_conf))        
                archive_tar = os.path.join(image_dir, "%s.tar.gz" % (archive_name))
                pack_cmd = "tar -C %s -cvzf %s %s" % (
                                                      os.path.dirname(archive_dir),
                                                      archive_tar, archive_name)
                print "Calling", pack_cmd
                subprocess.check_call(pack_cmd, shell=True)
                self.add_tool_package(archive_tar)
            
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
        
        shutil.rmtree(image_dir)

    
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



class GalaxyTargetFuture(TargetFuture):

    def __init__(self, task_id, step_id, output_name):
        self.step_id = step_id
        self.output_name = output_name
        super(GalaxyTargetFuture, self).__init__(task_id)

class GalaxyWorkflowTask(Task):
    """
    Instance of a Galaxy Workflow to be run
    """
    def __init__(self, engine, workflow, inputs=None, parameters=None, tags=None, step_tags=None):
        super(GalaxyWorkflowTask, self).__init__()

        if not isinstance(workflow, GalaxyWorkflow):
            raise Exception("Need galaxy workflow")
        self.engine = engine
        self.workflow = workflow
        self.inputs = inputs
        self.parameters = parameters
        self.tags = tags
        self.step_tags = step_tags

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
            tags=data.get('tags', None), step_tags=data.get('step_tags', None)
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
            'step_tags' : self.step_tags
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
        if self.tags is not None or self.step_tags is not None:
            for step, step_info in workflow_data['steps'].items():
                step_id = step_info['uuid']
                if step_info['type'] == "tool":
                    step_name = None
                    if self.step_tags is not None:
                        if step_info['label'] in self.step_tags:
                            step_name = step_info['label']
                        if step_info['annotation'] in self.step_tags:
                            step_name = step_info['annotation']
                        if step_info['uuid'] in self.step_tags:
                            step_name = step_info['uuid']
                    tags = []
                    if self.tags is not None:
                        tags += self.tags

                    pja_map = {}
                    for i, output in enumerate(step_info['outputs']):
                        output_name = output['name']
                        if step_name is not None and output_name in self.step_tags[step_name]:
                            cur_tags = tags + self.step_tags[step_name][output_name]
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
