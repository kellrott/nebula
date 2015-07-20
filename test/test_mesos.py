
import os
import unittest
import shutil
import logging
from urlparse import urlparse
import time
import socket

import nbtest_utils

from nebula.warpdrive import call_docker_run, call_docker_kill, call_docker_rm

import nebula.drms.mesos_runner
import nebula.scheduler
import nebula.service
from nebula.target import Target
from nebula.galaxy import GalaxyWorkflow
from nebula.docstore import FileDocStore
from nebula.docstore.util import sync_doc_dir
from nebula.tasks.md5_task import MD5Task

logging.basicConfig(level=logging.DEBUG)

#How to mantually setup a docker mesos system
#docker run -it -p 5050:5050 --rm --name mesos_master mesosphere/mesos-master:0.22.1-1.0.ubuntu1404
#docker run -it --rm --link mesos_master:mesos_master --name=mesos_slave mesosphere/mesos-slave:0.22.1-1.0.ubuntu1404 --master=mesos_master:5050


MASTER_IMAGE="mesosphere/mesos-master:0.22.1-1.0.ubuntu1404"
SLAVE_IMAGE="mesosphere/mesos-slave:0.22.1-1.0.ubuntu1404"

CONFIG_EXISTING_MESOS = "127.0.0.1:5050"
CONFIG_SUDO = False
CONFIG_PARENT_PORT = 15050


MASTER_NAME = "mesos_master"
SLAVE_NAME_BASE = "mesos_slave_%d"


def get_abspath(path):
    return os.path.join(os.path.dirname(__file__), path)

def get_host_ip():
    s=socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    h = s.getsockname()
    s.close()
    return h[0]


class TestMesos(unittest.TestCase):

    def setUp(self):
        if not os.path.exists("./test_tmp"):
            os.mkdir("test_tmp")
        self.service = None
        if CONFIG_EXISTING_MESOS is not None:
            self.setExistingMesos(CONFIG_EXISTING_MESOS)
        else:
            self.setupDockeredMesos()
        
        
    
    def setExistingMesos(self, addr):
        self.host_ip = addr
    
    def setupDockeredMesos(self):
        if 'DOCKER_HOST' in os.environ:
            n = urlparse(os.environ['DOCKER_HOST'])
            self.host_ip = n.netloc.split(":")[0]
        else:
            self.host_ip = get_host_ip()
        logging.info("Using HostIP: %s" % (self.host_ip))

        call_docker_run(image=MASTER_IMAGE,
            #ports={CONFIG_PARENT_PORT:5050},
            sudo=CONFIG_SUDO,
            name=MASTER_NAME,
            #env = {
            #    "MESOS_IP" : "0.0.0.0",
            #    "MESOS_HOSTNAME" : "0.0.0.0"
            #}
            env = {
                "MESOS_IP" : self.host_ip,
                "MESOS_HOSTNAME" : self.host_ip
            },
            args=[
            #    "--ip=%s" % (self.host_ip),
                "--registry=in_memory",
                "--port=%s" % (CONFIG_PARENT_PORT)
            ],
            net="host"
        )

        call_docker_run(image=SLAVE_IMAGE,
            sudo=CONFIG_SUDO,
            name=SLAVE_NAME_BASE % (0),
            #links={
            #    "mesos_master" : "mesos_master"
            #},
            #args = [ "--master=mesos_master:5050" ]
            args = ["--master=%s:%s" % (self.host_ip,CONFIG_PARENT_PORT) ],
            ports = { 15051 : 5050 },
            mounts = { "/var/run/docker.sock" : "/var/run/docker.sock" }
        )


    def tearDown(self):
        if self.service is not None:
            self.service.stop()
            self.service = None
            time.sleep(5)

        if os.path.exists("./test_tmp/docstore"):
            shutil.rmtree("./test_tmp/docstore")
        
        """
        call_docker_kill(MASTER_NAME)
        call_docker_kill(SLAVE_NAME_BASE % (0))

        call_docker_rm(MASTER_NAME)
        call_docker_rm(SLAVE_NAME_BASE % (0))
        """

    def testMesosLaunch(self):
        input_file_1 = Target("c39ded10-6073-11e4-9803-0800200c9a66"),
        input_file_2 = Target("26fd12a2-9096-4af2-a989-9e2f1cb692fe")

        doc = FileDocStore(file_path="./test_tmp/docstore")
        logging.info("Adding files to object store")
        sync_doc_dir("examples/simple_galaxy/", doc,
            uuid_set=["c39ded10-6073-11e4-9803-0800200c9a66", "26fd12a2-9096-4af2-a989-9e2f1cb692fe"]
        )

        #task_1 = MD5Task(input_file_1)
        #md5_service = nebula.service.md5_service.MD5Service(doc)
        
        input_file_1 = Target(uuid="c39ded10-6073-11e4-9803-0800200c9a66")
        input_file_2 = Target(uuid="26fd12a2-9096-4af2-a989-9e2f1cb692fe")
        workflow = GalaxyWorkflow(ga_file=get_abspath("../examples/simple_galaxy/SimpleWorkflow.ga"))
        task_1 = nebula.tasks.GalaxyWorkflowTask("workflow_test",
            workflow,
            inputs={
                'input_file_1' : input_file_1,
                'input_file_2' : input_file_2
            },
            parameters = {
                "tail_select" : {
                    "lineNum" : 3
                }
            }
        )
        service = nebula.service.GalaxyService(
            docstore=doc,
            name="nosetest_galaxy",
            galaxy="bgruening/galaxy-stable:dev",
            force=True,
            port=20022
        )

        sched = nebula.scheduler.Scheduler({})
        env = {}
        for v in [ 'DOCKER_HOST', 'DOCKER_CERT_PATH', 'DOCKER_TLS_VERIFY']:
            if v in os.environ:
                env[v] = os.environ[v]

        mesos = nebula.drms.mesos_runner.MesosDRMS(sched, {
            "mesos" : "%s:%s" % (self.host_ip, CONFIG_PARENT_PORT),
            "docstore" : doc.get_url(),
            "env" : env
        })
        mesos.start()
        job_1 = mesos.submit(service, task_1)
        mesos.wait([job_1])
        print job_1
        logging.info("Sleeping for 15")
        time.sleep(15)
        mesos.stop()
