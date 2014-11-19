

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
import json
import tarfile
from nebula.jobrecord import JobRecord
from nebula.egg import write_worker_egg
from nebula.dag import Target


IMAGE_DIR = "images"
JOB_DIR = "jobs"
WORKER_EGG = "nebula_worker.egg"

class WorkInfoManager:

    def __init__(self, object_store, doc_store):
        self.object_store = object_store
        self.doc_store = doc_store

    def get_dockerimage_sha1(self, name):
        records = list(self.doc_store(model_class='DockerImage', name=name))
        if len(records) == 0:
            return None
        if len(records) == 1:
            return records[0]['repositories']
        raise Exception("Multiple Matching Records")
        
    def build_worker_egg(self):
        
        egg_path = os.path.join(self.get_image_dir(), WORKER_EGG)
        write_worker_egg(egg_path)

    def get_jobrecord(self, nebula_id, task_id):
        records = list(self.object_store(model_class='JobRecord', nebula_id=nebula_id, task_id=task_id))
        if len(records) == 0:
            return None
        if len(records) == 1:
            return records[0]
        raise Exception("Multiple Matching Records")

    def store_jobrecord(self, nebula_id, task_id, record):
        old = self.get_jobrecord(nebula_id, task_id)
        if old is not None:
            record_id = old['id']
        else:
            record_id = str(uuid.uuid4())
        
        record['model_class'] = "JobRecord"
        record['nebula_id'] = nebula_id
        record['task_id'] = task_id
        self.doc_store(record_id, record)

    def build_image(self, name, path=None):
        #setup the host value for docker calls
        env = dict(os.environ)
        if config.docker is not None:
            env['DOCKER_HOST'] = config.docker

        workrepo = config.get_workrepo()
        sha1 = workrepo.get_dockerimage_sha1(self.docker.name)
        if sha1 is None:
            logging.info("Missing Docker Image: " + self.docker.name)
            if self.docker.path is not None and os.path.exists(self.docker.path):
                logging.info("Running Docker Build")
                if config.docker_clean:
                    cache = "--no-cache"
                else:
                    cache = ""
                cmd = "docker build %s -t %s %s" % (cache, self.docker.name, self.docker.path)
                subprocess.check_call(cmd, shell=True, env=env)
                logging.info("Saving Docker Image: " + self.docker.name)
                cmd = "docker save %s > %s" % (self.docker.name, workrepo.get_dockerimage_path(self.docker.name))
                subprocess.check_call(cmd, shell=True, env=env)
            else:
                logging.info("Pulling Docker Image")
                cmd = "docker pull %s" % (self.docker.name)
                logging.info(cmd)
                subprocess.check_call(cmd, shell=True, env=env)
                logging.info("Saving Docker Image: " + self.docker.name)
                cmd = "docker save %s > %s" % (self.docker.name, workrepo.get_dockerimage_path(self.docker.name))
                logging.info(cmd)
                subprocess.check_call(cmd, shell=True, env=env)
            
            t = tarfile.TarFile(image)
            meta_str = t.extractfile('repositories').read()
            meta = json.loads(meta_str)

