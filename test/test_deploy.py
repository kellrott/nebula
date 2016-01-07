
import json
import shutil
import unittest
import time
import os
from nebula.galaxy import GalaxyWorkflow, GalaxyService, GalaxyWorkflowTask
from nebula import Target
import nebula.docstore
from nebula.docstore.util import sync_doc_dir

from nebula.deploy import CmdLineDeploy
import nebula.galaxy.cmd

def get_abspath(path):
    return os.path.join(os.path.dirname(__file__), path)

class TestWorkflow(unittest.TestCase):
    def setUp(self):
        if not os.path.exists("./test_tmp"):
            os.mkdir("test_tmp")

    def testGalaxy(self):        
        #FIXME: need configuration
        docstore_url = "agro://10.250.35.126"
        
        resources = nebula.galaxy.cmd.action_pack(get_abspath("../examples/md5_sum"), docstore_url)
        
        deploy = CmdLineDeploy()        
        docstore = nebula.docstore.from_url(docstore_url, "workdir")

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
        
    def testRabix(self):
        
        deploy = CmdLineDeploy()        
        docstore = FileDocStore("./test_tmp/docstore")

        input_file_1 = Target(uuid="c39ded10-6073-11e4-9803-0800200c9a66")
        input_file_2 = Target(uuid="26fd12a2-9096-4af2-a989-9e2f1cb692fe")
        workflow = CWLWorkflow(ga_file=get_abspath("../examples/simple_cwl/SimpleWorkflow.cwl"))
        task = GalaxyWorkflowTask("test",
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
        
        service = GalaxyService(
            docstore=docstore,
            name="nosetest_galaxy",
            galaxy="nebula_galaxy",
            force=True,
            port=20022
        )
        
        deploy.run(service, task)
        
        