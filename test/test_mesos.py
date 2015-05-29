
import os
import unittest
import logging
import time
import socket
from nebula.warpdrive import call_docker_run, call_docker_kill, call_docker_rm

import nebula.drms.mesos_runner
import nebula.scheduler
import nebula.service
from nebula.target import Target
import shutil
from nebula.docstore import FileDocStore
from nebula.docstore.util import sync_doc_dir
from nebula.tasks.md5_task import MD5Task

logging.basicConfig(level=logging.DEBUG)

#How to mantually setup a docker mesos system
#docker run -it -p 5050:5050 --rm --name mesos_master mesosphere/mesos-master:0.22.1-1.0.ubuntu1404
#docker run -it --rm --link mesos_master:mesos_master --name=mesos_slave mesosphere/mesos-slave:0.22.1-1.0.ubuntu1404 --master=mesos_master:5050


MASTER_IMAGE="mesosphere/mesos-master:0.22.1-1.0.ubuntu1404"
SLAVE_IMAGE="mesosphere/mesos-slave:0.22.1-1.0.ubuntu1404"

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
            args = ["--master=%s:%s" % (self.host_ip,CONFIG_PARENT_PORT) ]
        )


    def tearDown(self):
        if self.service is not None:
            self.service.stop()
            self.service = None
            time.sleep(5)

        if os.path.exists("./test_tmp/docstore"):
            shutil.rmtree("./test_tmp/docstore")

        return

        call_docker_kill(MASTER_NAME)
        call_docker_kill(SLAVE_NAME_BASE % (0))

        call_docker_rm(MASTER_NAME)
        call_docker_rm(SLAVE_NAME_BASE % (0))


    def testMesosLaunch(self):
        input_file_1 = Target("c39ded10-6073-11e4-9803-0800200c9a66"),
        input_file_2 = Target("26fd12a2-9096-4af2-a989-9e2f1cb692fe")

        doc = FileDocStore(file_path="./test_tmp/docstore")
        logging.info("Adding files to object store")
        sync_doc_dir("examples/simple_galaxy/", doc,
            uuid_set=["c39ded10-6073-11e4-9803-0800200c9a66", "26fd12a2-9096-4af2-a989-9e2f1cb692fe"]
        )

        task_1 = MD5Task(input_file_1)


        md5_service = nebula.service.md5_service.MD5Service(doc)

        sched = nebula.scheduler.Scheduler({})
        mesos = nebula.drms.mesos_runner.MesosDRMS(sched, {
            "mesos" : "%s:%s" % (self.host_ip, CONFIG_PARENT_PORT)
        })
        mesos.start()
        mesos_md5_service = mesos.deploy_service(md5_service)
        job_1 = mesos_md5_service.submit(task_1)

        print job_1
        logging.info("Sleeping for 15")
        time.sleep(15)
        mesos.stop()
