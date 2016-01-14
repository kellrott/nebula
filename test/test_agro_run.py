
import os
import unittest
import subprocess
import shutil
from nebula.docstore import AgroDocStore
from nebula.docstore.util import sync_doc_dir
from nebula import Target
from nebula.galaxy import GalaxyWorkflow, GalaxyWorkflowTask, GalaxyService
import nebula.galaxy.cmd
from nebula.deploy import AgroDeploy
from urlparse import urlparse

def get_abspath(path):
    return os.path.join(os.path.dirname(__file__), path)


class TestAgro(unittest.TestCase):
    
    def setUp(self):
        cmd = "docker-compose -f %s up -d" % (get_abspath("../docker-compose/agro/docker-compose.yml"))
        subprocess.check_call(cmd, shell=True)
        
        self.agro_server = "localhost:9713"
        if 'DOCKER_HOST' in os.environ:
            self.agro_server = urlparse(os.environ['DOCKER_HOST']).netloc.split(":")[0] + ":9713"
        
        if not os.path.exists("./test_tmp"):
            os.mkdir("test_tmp")
        self.service = None

    def tearDown(self):
        if os.path.exists("./test_tmp/docstore"):
            shutil.rmtree("./test_tmp/docstore")
        return
        cmd = "docker-compose -f %s stop" % (get_abspath("../docker-compose/agro/docker-compose.yml"))
        subprocess.check_call(cmd, shell=True)
        cmd = "docker-compose -f %s rm -fv" % (get_abspath("../docker-compose/agro/docker-compose.yml"))
        subprocess.check_call(cmd, shell=True)
        
    def testWorkflow(self):
        agro_url = "agro://" + self.agro_server
        print "agro_url: ", agro_url
        resources = nebula.galaxy.cmd.action_pack(get_abspath("../examples/md5_sum"), agro_url)
        
        deploy = AgroDeploy(self.agro_server)
        docstore = nebula.docstore.from_url(agro_url, "workdir")

        sync_doc_dir(get_abspath("../examples/simple_data/"), docstore,
            uuid_set=["c39ded10-6073-11e4-9803-0800200c9a66", "26fd12a2-9096-4af2-a989-9e2f1cb692fe"]
        )

        input_file_1 = Target(uuid="c39ded10-6073-11e4-9803-0800200c9a66")
        input_file_2 = Target(uuid="26fd12a2-9096-4af2-a989-9e2f1cb692fe")
        workflow = GalaxyWorkflow(ga_file=get_abspath("../examples/md5_sum/Galaxy-Workflow-MD5_Workflow.ga"))
        task = GalaxyWorkflowTask("test",
            workflow,
            inputs={
                'INPUT' : input_file_1,
            },
        )
        
        service = GalaxyService(
            resources=resources,
            docstore=docstore
        )
        
        deploy.run(service, task)
    