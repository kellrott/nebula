
import logging
import time
import json

import nebula.docstore
from nebula.service import Service, ServiceConfig
from nebula.target import Target
from nebula.warpdrive import run_up, run_add, run_down
from nebula.galaxy import GalaxyWorkflow

class HDATarget(Target):
    def __init__(self, meta):
        self.meta = meta
        self.id = meta['id']


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



class GalaxyService(Service):

    config_defaults= {
        'name' : "nebula_galaxy",
        'port' : 19999,
        'metadata_suffix' : ".json",
        'galaxy' : "bgruening/galaxy-stable",
        'force' : True,
        'tool_docker' : True
    }

    def __init__(self, docstore, **kwds):
        super(GalaxyService, self).__init__('galaxy')
        self.config = kwds
        for k in self.config_defaults:
            if k not in self.config:
                self.config[k] = self.config_defaults[k]
        self.docstore = docstore
        self.rg = None
        self.ready = False

    def to_dict(self):
        return {
            'service_type' : 'Galaxy',
            'config' : self.config,
            'docstore_url' : self.docstore.get_url()
        }

    @staticmethod
    def from_dict(data):
        meta = dict(data)
        doc_store = nebula.docstore.from_url(data['docstore_url'], **data.get('docstore_config', {}))
        return GalaxyService( doc_store, **data['config'] )

    def get_config(self):
        return ServiceConfig(**self.to_dict() )

    def is_ready(self):
        return self.ready

    def runService(self):
        #FIXME: the 'file_path' value is specific to the DiskObjectStore
        docstore_path = self.docstore.file_path
        if 'lib_data' in self.config:
            self.config['lib_data'].append(self.docstore.local_cache_base())
        else:
            self.config['lib_data'] = [ self.docstore.local_cache_base() ]
        self.rg = run_up( **self.config )
        library_id = self.rg.library_find("Imported")['id']
        folder_id = self.rg.library_find_contents(library_id, "/")['id']

        self.ready = True

        logging.info("Galaxy Running")
        while self.running:
            time.sleep(3)
            req = self.get_queued()
            if req is not None:
                logging.info("Received task request")
                with self.queue_lock:
                    job_id, job = req
                    wids = []
                    for k, v in job.get_inputs().items():
                        file_path = self.docstore.get_filename(Target(v.id))
                        logging.info("Loading FilePath: %s" % (file_path))

                        nli = self.rg.library_paste_file(library_id=library_id, library_folder_id=folder_id,
                            name=v.id, datapath=file_path, uuid=v.uuid)
                        if 'id' not in nli:
                            raise Exception("Failed to load data: %s" % (str(nli)))
                        wids.append(nli['id'])

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
                    request = job.task.get_workflow_request()
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
                        wf_outputs = wf.get_outputs()
                        for step in invc['steps']:
                            if 'outputs' in step:
                                step_name = step['workflow_step_label'] if step['workflow_step_label'] is not None else str(step['workflow_step_uuid'])
                                for ok, ov in step['outputs'].items():
                                    output_name = "%s|%s" % (step_name, ok)
                                    if output_name in wf_outputs: #filter out produced items that are not part of the final output
                                        job.outputs[ output_name ] = ov
                                    else:
                                        job.hidden[ output_name ] = ov

        down_config = {}
        #if "work_dir" in self.config:
        #    down_config['work_dir'] = self.config['work_dir']
        run_down(name=self.config['name'], rm=True, sudo=self.config.get("sudo", False), **down_config)

    def status(self, job_id):
        s = super(GalaxyService, self).status(job_id)
        if s == 'active':
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
        return s

    def store_data(self, object, doc_store):
        meta = self.rg.get_dataset(object['id'], object['src'])
        meta['id'] = meta['uuid'] #use the glocal id
        hda = HDATarget(meta)
        doc_store.create(hda)
        path = doc_store.get_filename(hda)
        self.rg.download(meta['download_url'], path)
        doc_store.update_from_file(hda)

    def store_meta(self, object, doc_store):
        meta = self.get_meta(object)
        doc_store.put(meta['uuid'], meta)

    def get_meta(self, object):
        meta = self.rg.get_dataset(object['id'], object['src'])
        prov = self.rg.get_provenance(meta['history_id'], object['id'])
        meta['provenance'] = prov
        meta['job'] = self.rg.get_job(prov['job_id'])
        return meta
