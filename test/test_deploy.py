
"""

"""

import os
import unittest
from nebula.galaxy import GalaxyWorkflow, GalaxyEngine, GalaxyWorkflowTask
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

    def test_galaxy(self):
        """
        Test deploying Galaxy
        """
        #FIXME: need configuration
        #docstore_url = "agro://192.168.99.100"
        #docstore = nebula.docstore.from_url(docstore_url, "workdir")
        docstore = nebula.docstore.FileDocStore(file_path="./test_tmp/docstore")

        resources = nebula.galaxy.cmd.action_pack(get_abspath("../examples/md5_sum"), docstore.url)

        deploy = CmdLineDeploy()

        sync_doc_dir(get_abspath("../examples/simple_data/"), docstore,
                uuid_set=["c39ded10-6073-11e4-9803-0800200c9a66", "26fd12a2-9096-4af2-a989-9e2f1cb692fe"]
                )

        input_file_1 = Target(uuid="c39ded10-6073-11e4-9803-0800200c9a66")
        #input_file_2 = Target(uuid="26fd12a2-9096-4af2-a989-9e2f1cb692fe")
        workflow = GalaxyWorkflow(
            ga_file=get_abspath("../examples/md5_sum/Galaxy-Workflow-MD5_Workflow.ga")
        )

        engine = GalaxyEngine(
            resources=resources,
            docstore=docstore
        )
        
        task = GalaxyWorkflowTask(engine,
                                    workflow,
                                    inputs={
                                        'INPUT' : input_file_1,
                                    },
        )

        deploy.run(task)
    
        