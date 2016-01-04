
import os
import unittest
import subprocess
import shutil
from nebula.docstore import AgroDocStore
from nebula.docstore.util import sync_doc_dir
from nebula import Target
from nebula.galaxy import GalaxyWorkflow, GalaxyWorkflowTask, GalaxyService
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
        
        cmd = "docker-compose -f %s stop" % (get_abspath("../docker-compose/agro/docker-compose.yml"))
        subprocess.check_call(cmd, shell=True)
        cmd = "docker-compose -f %s rm -fv" % (get_abspath("../docker-compose/agro/docker-compose.yml"))
        subprocess.check_call(cmd, shell=True)
        
    def testWorkflowTagging(self):
        
        docstore = AgroDocStore(self.agro_server, "./test_tmp/docstore")

        sync_doc_dir(get_abspath("../examples/simple_galaxy/"), docstore,
            uuid_set=["c39ded10-6073-11e4-9803-0800200c9a66", "26fd12a2-9096-4af2-a989-9e2f1cb692fe"]
        )
        
        input_file_1 = Target(uuid="c39ded10-6073-11e4-9803-0800200c9a66")
        input_file_2 = Target(uuid="26fd12a2-9096-4af2-a989-9e2f1cb692fe")
        workflow = GalaxyWorkflow(ga_file=get_abspath("../examples/simple_galaxy/SimpleWorkflow.ga"))
        task_tag = GalaxyWorkflowTask("workflow_ok",
            workflow,
            inputs={
                'input_file_1' : input_file_1,
                'input_file_2' : input_file_2
            },
            parameters={
                "tail_select" : {
                    "lineNum" : 3
                }
            },
            tags=[
                "fileType:testing",
                "testType:workflow"
            ]
        )
        print "Starting Service"
        service = GalaxyService(
            docstore=docstore,
            name="nosetest_galaxy",
            galaxy="bgruening/galaxy-stable:dev",
            force=True,
            port=20022
        )
        service.start()
        self.service = service
        job = service.submit(task_tag)
        service.wait([job])
        self.assertIn(job.get_status(), ['ok'])
        self.assertFalse( service.in_error() )
        self.docstore = None
