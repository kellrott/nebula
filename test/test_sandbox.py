

import os
import time
import json
import shutil
import logging
import unittest
import subprocess
from nebula import TaskGroup, Target
from nebula.deploy import CmdLineDeploy
from nebula.docstore import FileDocStore
from nebula.docstore.util import sync_doc_dir
from nebula.galaxy import GalaxyEngine, GalaxyWorkflow, GalaxyWorkflowTask, GalaxyResources

def get_abspath(path):
    return os.path.abspath(os.path.join(os.path.dirname(__file__), path))

class TestSandBox(unittest.TestCase):

    def setUp(self):
        if not os.path.exists(get_abspath("../test_tmp")):
            os.mkdir(get_abspath("../test_tmp"))

    def tearDown(self):
        if os.path.exists(get_abspath("../test_tmp/docstore")):
            shutil.rmtree(get_abspath("../test_tmp/docstore"))

    def testNebulaLaunch(self):
        
        doc = FileDocStore(
            file_path=get_abspath("../test_tmp/docstore")
        )
        
        resources = GalaxyResources()
        resources.add_tool_dir(get_abspath("../examples/sandbox"))
        resources.sync(doc)
        engine = GalaxyEngine(
            docstore=doc,
            resources=resources,
            port=20022,
            child_network="none",
            work_volume=get_abspath("../test_tmp/galaxy"),
            hold=True
        )

        logging.info("Creating Task")
        workflow = GalaxyWorkflow(ga_file=get_abspath("../examples/sandbox/Galaxy-Workflow-SandBoxTest.ga"))
        task = GalaxyWorkflowTask(
            engine,
            workflow,
            inputs={}
        )

        deploy = CmdLineDeploy()
        deploy.run(task)
            
        for i in doc.filter():
            print json.dumps(i, indent=4)
