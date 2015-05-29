
import os
import unittest
import logging
import time
import socket
from nebula.warpdrive import call_docker_run, call_docker_kill, call_docker_rm

import nebula.drms.mesos_runner
import nebula.scheduler

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

class TestScheduler:
    def __init__(self):
        pass

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
        sched = nebula.scheduler.Scheduler({})
        mesos = nebula.drms.mesos_runner.MesosDRMS(sched, {
            "mesos" : "%s:%s" % (self.host_ip, CONFIG_PARENT_PORT)
        })
        mesos.start()
        time.sleep(30)
        mesos.stop()
