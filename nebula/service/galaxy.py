
import logging
import time

import nebula.docstore
from nebula.service import Service, ServiceConfig
from nebula.galaxy import Workflow
from nebula.target import Target
from nebula.warpdrive import run_up, run_add, run_down

class HDATarget(Target):
    def __init__(self, meta):
        self.meta = meta
        self.uuid = meta['id']


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
            'docstore_url' : self.docstore.url
        }

    @staticmethod
    def from_dict(data):
        meta = dict(data)
        doc_store = nebula.docstore.from_url(data['docstore_url'])
        return GalaxyService( doc_store, **data['config'] )

    def get_config(self):
        return ServiceConfig(**self.to_dict() )

    def is_ready(self):
        return self.ready

    def runService(self):
        #FIXME: the 'file_path' value is specific to the DiskObjectStore
        docstore_path = self.docstore.file_path
        if 'lib_data' in self.config:
            self.config['lib_data'].append(self.docstore.file_path)
        else:
            self.config['lib_data'] = [ self.docstore.file_path ]
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
                        file_path = self.docstore.get_filename(Target(v.uuid))
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

                    self.rg.add_workflow(job.task.workflow_data)
                    wf = Workflow(job.task.workflow_data)
                    inputs = {}
                    for k, v in job.get_inputs().items():
                        inputs[k] = {
                            'src' : "uuid",
                            'id' : v.uuid
                        }
                    invc = self.rg.call_workflow(request=job.task.get_workflow_request())
                    print "Called Workflow", invc
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
                if job.state == 'error':
                    return "error"
                ready = True
                for data in job.outputs:
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
        meta = self.rg.get_hda(object['history'], object['id'])
        meta['id'] = meta['uuid'] #use the glocal id
        hda = HDATarget(meta)
        doc_store.create(hda)
        path = doc_store.get_filename(hda)
        self.rg.download(meta['download_url'], path)
        doc_store.update_from_file(hda)

    def store_meta(self, object, doc_store):
        meta = self.get_meta(a)
        doc_store.put(meta['uuid'], meta)


    def store_meta(self, object, doc_store):
        meta = self.rg.get_hda(object['history'], object['id'])
        prov = self.rg.get_provenance(object['history'], object['id'])
        meta['provenance'] = prov
        meta['job'] = self.rg.get_job(prov['job_id'])
        doc_store.put(meta['uuid'], meta)

    def get_meta(self, object):
        meta = self.rg.get_hda(object['history'], object['id'])
        prov = self.rg.get_provenance(object['history'], object['id'])
        meta['provenance'] = prov
        meta['job'] = self.rg.get_job(prov['job_id'])
        return meta
