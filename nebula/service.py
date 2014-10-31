

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

from nebula.warpdrive import run_up, run_add, run_down
from threading import Thread, RLock

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
        self.queue = []
        self.running = True

    def submit(self, job):
        with self.queue_lock:
            self.queue.append(job)

    def stop(self):
        self.running = False


class TaskJob:
    def __init__(self, task_data):
        self.task_data = task_data
        self.service_name = self.task_data['service']

    def set_running(self):
        pass

    def set_done(self):
        pass

    def set_error(self):
        pass

    def get_inputs(self):
        return self.task_data['inputs']


class GalaxyService(Service):
    def __init__(self, **kwds):
        super(GalaxyService, self).__init__('galaxy')
        self.config = kwds

    def run(self):
        rg = run_up( **self.config )
        library_id = rg.library_find("Imported")['id']
        folder_id = rg.library_find_contents(library_id, "/")['id']

        print "Galaxy Running"
        while self.running:
            time.sleep(1)
            with self.queue_lock:
                if len(self.queue):
                    job = self.queue.pop()
                    for k, v in job.get_inputs().items():
                        print rg.library_paste_file(library_id, folder_id, v['uuid'], v['path'], uuid=v['uuid'])
                    rg.add_workflow(job.task_data['workflow'])
                    rg.call_workflow(job.task_data['workflow']['uuid'], inputs=job.get_inputs(), params={})
        run_down(rm=True)


class ShellService(Service):

    def run(self, data):
        script = data['script']
        docker_image = data.get('docker', 'base')
        self.workdir = self.config.workdir
        self.execdir = os.path.abspath(tempfile.mkdtemp(dir=self.workdir, prefix="nebula_"))

        with open(os.path.join(self.execdir, "run.sh"), "w") as handle:
            handle.write(script)

        docker_path = which('docker')
        if docker_path is None:
            raise Exception("Cannot find docker")

        cmd = [
            docker_path, "run", "--rm", "-u", str(os.geteuid()),
            "-v", "%s:/work" % self.execdir, "-w", "/work", docker_image,
            "/bin/bash", "/work/run.sh"
        ]
        env = dict(os.environ)
        if self.config.docker is not None:
            env['DOCKER_HOST'] = self.config.docker

        logging.info("executing: " + " ".join(cmd))
        proc = subprocess.Popen(cmd, close_fds=True, env=env)
        proc.communicate()
        if proc.returncode != 0:
            raise Exception("Call Failed: %s" % (cmd))

        self.outputs = {}
        for k, v in data['outputs'].items():
            self.outputs[k] = {
                'store_path' : os.path.join(self.execdir, v['path']),
                'uuid' : v['uuid']
            }

    def getOutputs(self):
        return self.outputs


class FileScanner(Service):

    def run(self, data):
        out = {}
        logging.info("Scanning for files %s" % (self.config.storage_dir))
        for a in glob(os.path.join(self.config.storage_dir, "*")):
            name = os.path.basename(a)
            if uuid4hex.match(name):
                out[name] = {'uuid' : name, 'store_path' : os.path.abspath(a)}
        self.outputs = out

    def getOutputs(self):
        return self.outputs


service_map = {
    'shell' : ShellService,
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
