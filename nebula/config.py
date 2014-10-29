
import os
from nebula.workrepo import WorkRepo
from nebula.datamanager import DataManager

class Config:
    def __init__(self,
        mesos=None,
        port=9999,
        host='localhost',
        workdir="/tmp", docker=None, basedir="./",
        dist_storage_dir="/tmp/nebula_storage",
        max_servers=0,
        imagedir=None,
        shared_dirs=[],
        docker_clean=False):

        self.mesos = mesos
        self.port = port
        self.host = host
        self.workdir = workdir
        self.shared_dirs = shared_dirs
        self.docker = docker
        self.docker_clean = docker_clean
        self.basedir = basedir
        if imagedir is None:
            imagedir = os.path.join(basedir, "images")
        self.imagedir = imagedir
        self.max_servers = max_servers
        self.dist_storage_dir = dist_storage_dir
        self.storage_dir = os.path.join(self.workdir, 'data')
        if not os.path.exists(self.storage_dir):
            os.makedirs(self.storage_dir)
        if not os.path.exists( os.path.join(self.basedir, ".nebula") ):
            os.mkdir(os.path.join(self.basedir, ".nebula"))
        if not os.path.exists(self.get_schema_dir()):
            os.mkdir(self.get_schema_dir())

        if not os.path.exists(self.get_output_dir()):
            os.mkdir(self.get_output_dir())
            

    def get_datamanager(self):
        out = DataManager(output_dir=self.get_output_dir(), shared_dirs=self.shared_dirs)
    
    def get_output_dir(self):
        return os.path.join(self.basedir, ".nebula", "data")

    def get_workrepo(self):
        return WorkRepo(os.path.join(self.basedir, ".nebula"))

    def get_schema_dir(self):
        return os.path.join(self.basedir, ".nebula", "schema")


