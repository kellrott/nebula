
import os
from nebula.workinfo import WorkInfoManager
from nebula.datamanager import DataManager

class Config:
    def __init__(self,
        object_store,
        doc_store,
        mesos=None,
        port=9999,
        host='localhost',
        workdir="/tmp", docker=None, basedir="./",
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
        self.object_store = object_store
        self.doc_store = doc_store
    
    def get_workinfo_manager(self):
        return WorkInfoManager(object_store=self.object_store, doc_store=self.doc_store)
