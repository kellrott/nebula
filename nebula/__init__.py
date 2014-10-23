
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
from parser import NebulaCompile
from scheduler import Scheduler
from website import WebSite
from workrepo import WorkRepo
from dag import DagSet

class Config:
    def __init__(self,
        mesos=None,
        port=9999,
        host='localhost',
        workdir="/tmp", docker=None, basedir="./",
        dist_storage_dir="/tmp/nebula_storage",
        max_servers=0,
        docker_clean=False):

        self.mesos = mesos
        self.port = port
        self.host = host
        self.workdir = workdir
        self.docker = docker
        self.docker_clean = docker_clean
        self.basedir = basedir
        self.max_servers = max_servers
        self.dist_storage_dir = dist_storage_dir
        self.storage_dir = os.path.join(self.workdir, 'data')
        if not os.path.exists(self.storage_dir):
            os.makedirs(self.storage_dir)
        if not os.path.exists( os.path.join(self.basedir, ".nebula") ):
            os.mkdir(os.path.join(self.basedir, ".nebula"))
        self.schema_dir = os.path.join(self.basedir, ".nebula", "schema")
        if not os.path.exists(self.schema_dir):
            os.mkdir(self.schema_dir)


    def get_work_repo(self):
        return WorkRepo(os.path.join(self.basedir, ".nebula"))

    def get_schema_dir(self):
        return os.path.join(self.basedir, ".nebula", "schema")
