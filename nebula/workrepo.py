

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


IMAGE_DIR = "images"
JOB_DIR = "jobs"

class WorkRepo:
    
    def __init__(self, basedir):
        self.basedir = basedir
        if not os.path.exists(basedir):
            os.mkdir(basedir)
        images = os.path.join(basedir, IMAGE_DIR)
        if not os.path.exists(images):
            os.mkdir(images)
        jobs = os.path.join(basedir, JOB_DIR)
        if not os.path.exists(jobs):
            os.mkdir(jobs)
    
    def get_dockerimage_sha1(self, name):
        image = self.get_dockerimage_path(name)
        if not os.path.exists(image):
            return None
        
        t = tarfile.TarFile(image)
        meta_str = t.extractfile('repositories').read()
        meta = json.loads(meta_str)
        
        return meta #FIXME: better output parsing
    
    def get_dockerimage_path(self, name):
        return os.path.join(self.basedir, IMAGE_DIR, name + ".tar")
    
    def get_jobrecord(self, task_id):
        path = os.path.join(self.basedir, JOB_DIR, task_id + ".json")
        if os.path.exists(path):
            with open(path) as handle:
                txt = handle.read()
                data = json.loads(txt)
                return JobRecord(data)
        return None
    
    def store_jobrecord(self, task_id, record):
        path = os.path.join(self.basedir, JOB_DIR, task_id + ".json")
        with open(path, "w") as handle:
            txt = json.dumps(record.data)
            handle.write(txt)
