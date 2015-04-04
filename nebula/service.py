

# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import os
import time
import json
import logging

from nebula.dag import Target
from nebula.warpdrive import run_up, run_add, run_down
from threading import Thread, RLock
from nebula.exceptions import NotImplementedException

from nebula.galaxy import Workflow

def which(file):
    for path in os.environ["PATH"].split(":"):
        p = os.path.join(path, file)
        if os.path.exists(p):
            return p


def port_active(portnum):
    """
    Check if a port is active or not (to prevent trying to allocate used ports)
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex(('127.0.0.1',portnum))
    if result == 0:
        return True
    else:
        return False


class Service(Thread):
    def __init__(self, name):
        super(Service, self).__init__()
        self.name = name
        self.queue_lock = RLock()
        self.queue = {}
        self.active = {}

        self.running = True
        self.job_count = 0

    def submit(self, job):
        with self.queue_lock:
            j = self.job_count
            self.job_count += 1
            self.queue[j] = job
            return j

    def get_queued(self):
        with self.queue_lock:
            if len(self.queue):
                job_id, job = self.queue.popitem()
                self.active[job_id] = job
                return job_id, job
        return None

    def stop(self):
        self.running = False

    def status(self, job_id):
        with self.queue_lock:
            if job_id in self.queue:
                return "waiting"
            if job_id in self.active:
                return "active"
            return "unknown"

    def get_job(self, job_id):
        with self.queue_lock:
            if job_id in self.queue:
                return self.queue[job_id]
            if job_id in self.active:
                return self.active[job_id]
        return None

    def store_data(self, data, object_store):
        raise NotImplementedException()


class TaskJob:
    def __init__(self, task_data):
        self.task_data = task_data
        self.service_name = self.task_data['service']
        self.history = None
        self.outputs = []
        self.error = None

    def set_running(self):
        pass

    def set_done(self):
        pass

    def set_error(self, msg="Failure"):
        self.error = msg

    def get_inputs(self):
        out = {}
        for name, value in self.task_data['request']['inputs'].items():
            out[name] = HDATarget(value)
        return out

    def get_parameters(self):
        return self.task_data['request']['parameters']

    def get_outputs(self):
        return self.outputs

class HDATarget(Target):
    def __init__(self, meta):
        self.meta = meta
        self.uuid = meta['id']

class GalaxyService(Service):
    def __init__(self, objectstore, **kwds):
        super(GalaxyService, self).__init__('galaxy')
        self.config = kwds
        self.objectstore = objectstore
        self.rg = None

    def run(self):
        with self.queue_lock:
            self.rg = run_up( **self.config )
            library_id = self.rg.library_find("Imported")['id']
            folder_id = self.rg.library_find_contents(library_id, "/")['id']

        logging.info("Galaxy Running")
        while self.running:
            time.sleep(3)
            req = self.get_queued()
            if req is not None:
                with self.queue_lock:
                    job_id, job = req
                    wids = []
                    for k, v in job.get_inputs().items():
                        file_path = self.objectstore.get_filename(Target(v.uuid))
                        logging.info("Loading FilePath: %s" % (file_path))

                        nli = self.rg.library_paste_file(library_id=library_id, library_folder_id=folder_id,
                            name=v.uuid, datapath=file_path, uuid=v.uuid)
                        if 'id' not in nli:
                            raise Exception("Failed to load data: %s" % (str(nli)))
                        wids.append(nli['id'])

                    #wait for the uploading of the files to finish
                    while True:
                        done = True
                        for w in wids:
                            d = self.rg.library_get_contents(library_id, w)
                            if d['state'] != 'ok':
                                logging.debug("Data loading: %s" % (d['state']))
                                done = False
                        if done:
                            break
                        time.sleep(2)

                    self.rg.add_workflow(job.task_data['workflow'])
                    wf = Workflow(job.task_data['workflow'])
                    inputs = {}
                    for k, v in job.get_inputs().items():
                        inputs[k] = {
                            'src' : "uuid",
                            'id' : v.uuid
                        }
                    invc = self.rg.call_workflow(request=job.task_data['request'])
                    if 'err_msg' in invc:
                        logging.error("Workflow invocation failed")
                        job.set_error("Workflow Invocation Failed")
                    else:
                        job.history = invc['history']
                        job.outputs = list( {"id" : i, "history" : invc['history'], "src" : "hda"} for i in invc['outputs'] )
        down_config = {}
        #if "work_dir" in self.config:
        #    down_config['work_dir'] = self.config['work_dir']
        run_down(name=self.config['name'], rm=True, sudo=self.config.get("sudo", False), **down_config)

    def status(self, job_id):
        s = super(GalaxyService, self).status(job_id)
        if s == 'active':
            if self.rg is not None:
                job = self.get_job(job_id)
                if job.error is not None:
                    return "error"
                for data in job.outputs:
                    meta = self.rg.get_hda(job.history, data['id'])
                    if meta['state'] != 'ok':
                        return meta['state']
                return "ok"
            return "waiting"
        return s

    def store_data(self, data, object_store):
        meta = self.rg.get_hda(data['history'], data['id'])
        meta['id'] = meta['uuid'] #use the glocal id
        hda = HDATarget(meta)
        object_store.create(hda)
        path = object_store.get_filename(hda)
        self.rg.download(meta['download_url'], path)
        object_store.update_from_file(hda)

    def store_meta(self, data, doc_store):
        meta = self.rg.get_hda(data['history'], data['id'])
        prov = self.rg.get_provenance(data['history'], data['id'])
        meta['provenance'] = prov
        meta['job'] = self.rg.get_job(prov['job_id'])
        doc_store.put(meta['uuid'], meta)

    def get_meta(self, data):
        meta = self.rg.get_hda(data['history'], data['id'])
        prov = self.rg.get_provenance(data['history'], data['id'])
        meta['provenance'] = prov
        meta['job'] = self.rg.get_job(prov['job_id'])
        return meta


service_map = {
    #'shell' : ShellService,
    'galaxy' : GalaxyService
}

config_defaults= {
    'galaxy' : {
        'name' : "nebula_galaxy",
        'port' : 19999,
        'metadata_suffix' : ".json"
    }

}

def ServiceFactory(service_name, **kwds):
    d = {}
    d.update(config_defaults[service_name])
    d.update(kwds)
    return service_map[service_name](**d)
