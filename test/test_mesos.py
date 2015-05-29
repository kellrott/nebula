
import os
import unittest
from nebula.warpdrive import call_docker_run, call_docker_kill, call_docker_rm

#How to mantually setup a docker mesos system
#docker run -it -p 5050:5050 --rm --name mesos_master mesosphere/mesos-master:0.22.1-1.0.ubuntu1404
#docker run -it --rm --link mesos_master:mesos_master --name=mesos_slave mesosphere/mesos-slave:0.22.1-1.0.ubuntu1404 --master=mesos_master:5050


MASTER_IMAGE="mesosphere/mesos-master:0.22.1-1.0.ubuntu1404"
SLAVE_IMAGE="mesosphere/mesos-slave:0.22.1-1.0.ubuntu1404"

CONFIG_SUDO = False

MASTER_NAME = "mesos_master"
SLAVE_NAME_BASE = "mesos_slave_%d"

class TestMesos(unittest.TestCase):

    def setUp(self):
        if not os.path.exists("./test_tmp"):
            os.mkdir("test_tmp")
        self.service = None

        call_docker_run(image=MASTER_IMAGE,
            ports={5050:5050},
            sudo=CONFIG_SUDO,
            name=MASTER_NAME
        )

        call_docker_run(image=SLAVE_IMAGE,
            sudo=CONFIG_SUDO,
            name=SLAVE_NAME_BASE % (0),
            links={
                "mesos_master" : "mesos_master"
            },
            args = [ "--master=mesos_master:5050" ]
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
        pass
