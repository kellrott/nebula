
"""
Code to run dockerized copy of Galaxy
"""

import os
import logging
import time
import json
import shutil
import socket

import nebula.docstore
from nebula import Engine, Target
from nebula.warpdrive import run_up, run_down, RemoteGalaxy, web_wait, library_paste_sync
from nebula.galaxy.core import GalaxyResources
from nebula.galaxy.io import GalaxyWorkflow

class HDATarget(Target):
    """
    """
    def __init__(self, meta):
        super(HDATarget, self).__init__(self)
        self.meta = meta
        self.id = meta['id']


def which(program):
    for path in os.environ["PATH"].split(":"):
        p = os.path.join(path, program)
        if os.path.exists(p):
            return p

def port_active(portnum):
    """
    Check if a port is active or not (to prevent trying to allocate used ports)
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex(('127.0.0.1', portnum))
    if result == 0:
        return True
    else:
        return False



class GalaxyEngine(Engine):

    launch_config_defaults = {
        'name' : "nebula_galaxy",
        'port' : 19999,
        'metadata_suffix' : ".json",
        'galaxy' : "nebula_galaxy",
        'force' : True,
        'tool_docker' : True
    }

    inside_config_defaults = {
        'url' : 'http://localhost:8080',
        'metadata_suffix' : ".json",
        'galaxy' : "nebula_galaxy",
        #'docker_user' : '1450'
    }


    def __init__(self, 
        docstore, 
        resources, 
        url=None, api_key=None,
        port=None, launch_docker=False, 
        child_network='bridge',
        work_volume=None, hold=False):
        super(GalaxyEngine, self).__init__('galaxy')
        self.launch_docker = launch_docker
        if work_volume is not None:
            work_volume = os.path.abspath(work_volume)
        self.config = {
            'work_volume' : work_volume,
            'child_network' : child_network,
            'port' : port,
            'url' : url,
            'api_key' : api_key,
            'hold' : hold
        }
        if launch_docker:
            for k in self.launch_config_defaults:
                if k not in self.config or self.config[k] is None:
                    self.config[k] = self.launch_config_defaults[k]
        else:
            for k in self.inside_config_defaults:
                if k not in self.config or self.config[k] is None:
                    self.config[k] = self.inside_config_defaults[k]
        self.resources = resources
        self.docstore = docstore
        self.rg = None
        self.ready = False

    def to_dict(self):
        res = None
        if self.resources is not None:
            res = self.resources.to_dict()
        return {
            'engine_type' : 'GalaxyEngine',
            'config' : self.config,
            'resources' : res,
            'docstore_url' : self.docstore.get_url()
        }

    @staticmethod
    def from_dict(data):
        #meta = dict(data)
        doc_store = nebula.docstore.from_url(
            data['docstore_url'],
            **data.get('docstore_config', {})
        )
        resource = GalaxyResources.from_dict(data['resources'])
        return GalaxyEngine(doc_store, resource, **data['config'])

    def is_ready(self):
        return self.ready

    def get_docker_image(self):
        return self.config['galaxy']
    
    def get_docker_user(self):
        return None
        #return self.config['docker_user']

    def get_wrapper_command(self):
        return ["/opt/nebula/bin/nebula", "galaxy", "run"]
    
    def get_work_volume(self):
        if 'work_volume' in self.config:
            return "%s:/export" % (self.config['work_volume'])

    def runEngine(self):
        web_wait(self.config['url'], 120)

        if 'lib_data' in self.config:
            self.config['lib_data'].append(self.docstore.local_cache_base())
        else:
            self.config['lib_data'] = [self.docstore.local_cache_base()]

        if self.launch_docker:
            print "running config", self.config
            self.rg = run_up(**self.config)
        else:
            common_dir_map = {}
            if self.launch_docker and 'common_dirs' in self.config:
                for c in self.config['common_dirs']:
                    common_dir_map[c] = c
            self.rg = RemoteGalaxy(self.config['url'],
                                   self.config['api_key'],
                                   path_mapping=common_dir_map
                                  )

        library_paste_sync(self.rg, [], {})

        library_id = self.rg.library_find("Imported")['id']
        folder_id = self.rg.library_find_contents(library_id, "/")['id']

        self.ready = True

        logging.info("Galaxy Running")
        while self.running:
            time.sleep(3)
            req = self.get_queued()
            if req is not None:
                logging.info("Received task request")
                uuid_ldda_map = {}
                with self.queue_lock:
                    job_id, job = req
                    wids = []
                    for k, v in job.get_inputs().items():
                        file_path = self.docstore.get_filename(Target(v.id))
                        file_meta = self.docstore.get(v.id)
                        file_name = v.id
                        if 'name' in file_meta:
                            file_name = file_meta['name']
                        logging.info("Loading FilePath: %s (%s) %s" % (v.id, file_name, file_path))
                        nli = self.rg.library_paste_file(library_id=library_id,
                                                         library_folder_id=folder_id,
                                                         name=file_name,
                                                         datapath=file_path,
                                                         uuid=v.uuid)
                        if 'id' not in nli:
                            raise Exception("Failed to load data %s: %s" % (k, str(nli)))
                        wids.append(nli['id'])
                        uuid_ldda_map[v.uuid] = nli['id']

                    #wait for the uploading of the files to finish
                    while True:
                        done = True
                        for w in wids:
                            d = self.rg.library_get_contents(library_id, w)
                            if d['state'] == 'error':
                                raise Exception("Data loading Error")
                            if d['state'] != 'ok':
                                logging.debug("Data loading: %s" % (d['state']))
                                done = False
                                break
                        if done:
                            break
                        time.sleep(2)

                    workflow_data = job.task.to_dict()['workflow']
                    logging.info("Loading Workflow: %s" % (workflow_data['uuid']))
                    self.rg.add_workflow(workflow_data)
                    wf = GalaxyWorkflow(workflow_data)
                    print "uuid_map", uuid_ldda_map
                    request = job.task.get_workflow_request(uuid_ldda_map)
                    print "Calling Workflow", json.dumps(request)
                    invc = self.rg.call_workflow(request=request)
                    print "Called Workflow", json.dumps(invc)
                    if 'err_msg' in invc:
                        logging.error("Workflow invocation failed")
                        job.set_error("Workflow Invocation Failed")
                    else:
                        job.history = invc['history']
                        job.instance_id = invc['uuid']
                        job.outputs = {}
                        job.hidden = {}
                        wf_outputs = wf.get_outputs(all=True)
                        for step in invc['steps']:
                            if 'outputs' in step:
                                if step['workflow_step_label'] is not None:
                                    step_name = step['workflow_step_label']
                                else:
                                    step_name = str(step['workflow_step_uuid'])
                                for ok, ov in step['outputs'].items():
                                    output_name = "%s|%s" % (step_name, ok)
                                    if output_name in wf_outputs: #filter out produced items that are not part of the final output
                                        job.outputs[ output_name ] = ov
                                    else:
                                        job.hidden[ output_name ] = ov

        down_config = {}
        #if "work_dir" in self.config:
        #    down_config['work_dir'] = self.config['work_dir']
        if self.launch_docker:
            run_down(name=self.config['name'], rm=True, sudo=self.config.get("sudo", False), **down_config)

    def status(self, job_id):
        if job_id in self.active:
            if self.rg is not None:
                job = self.get_job(job_id)
                if job.state == 'error':
                    return "error"
                ready = True
                for outputname, data in job.get_outputs(all=True).items():
                    meta = self.rg.get_hda(job.history, data['id'])
                    if meta['state'] == 'error':
                        job.set_error(meta['misc_info'])
                    if meta['state'] != 'ok':
                        ready = False
                if ready:
                    job.state = "ok"
                return job.state
            return "waiting"
        elif job_id in self.queue:
            return "waiting"
        return "unknown"

    def store_data(self, object, doc_store):
        meta = self.rg.get_dataset(object['id'], object['src'])
        print "Storing", meta
        meta['id'] = meta['uuid'] #use the glocal id
        hda = HDATarget(meta)
        doc_store.create(hda)
        path = doc_store.get_filename(hda)
        shutil.copy(meta['file_name'], path)
        #self.rg.download(meta['download_url'], path)
        doc_store.update_from_file(hda)

    def store_meta(self, object, doc_store):
        """
        """
        meta = self.get_meta(object)
        doc_store.put(meta['uuid'], meta)

    def get_meta(self, object):
        """
        """
        meta = self.rg.get_dataset(object['id'], object['src'])
        prov = self.rg.get_provenance(meta['history_id'], object['id'])
        meta['provenance'] = prov
        meta['id'] = meta['uuid']
        meta['job'] = self.rg.get_job(prov['job_id'])
        return meta
