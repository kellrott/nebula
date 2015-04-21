


import unittest
import time
import os
import shutil
import nebula.tasks
from nebula.docstore import FileDocStore
from nebula.docstore.util import sync_doc_dir
import nebula.tasks
from nebula.service import GalaxyService, TaskJob
import logging
import json

class TestRunWorkflow(unittest.TestCase):

    def setUp(self):
        if not os.path.exists("./test_tmp"):
            os.mkdir("test_tmp")
        self.service = None

    def tearDown(self):
        if self.service is not None:
            self.service.stop()

        if os.path.exists("./test_tmp/docstore"):
            shutil.rmtree("./test_tmp/docstore")

    def testRunSimple(self):
        input = {
            "input_file_1" :
                {"uuid" : "c39ded10-6073-11e4-9803-0800200c9a66"},
            "input_file_2" :
                {"uuid" : "26fd12a2-9096-4af2-a989-9e2f1cb692fe"},
            "tail_select" : {
                "lineNum" : 3
            }
        }

        doc = FileDocStore(file_path="./test_tmp/docstore")
        logging.info("Adding files to object store")
        sync_doc_dir("examples/simple_galaxy/", doc,
            uuid_set=["c39ded10-6073-11e4-9803-0800200c9a66", "26fd12a2-9096-4af2-a989-9e2f1cb692fe"]
        )
        logging.info("Creating Task")
        task = nebula.tasks.GalaxyWorkflowTask(
            "test_workflow",
            "examples/simple_galaxy/SimpleWorkflow.ga",
            inputs=input
        )

        task_data = task.to_dict()
        #make sure the task data can be serialized
        task_data_str = json.dumps(task_data)

        service = GalaxyService(
            objectstore=doc,
            name="nosetest_galaxy",
            galaxy="bgruening/galaxy-stable:dev"
        )
        self.service = service

        new_task_data = json.loads(task_data_str)
        new_task = nebula.tasks.from_dict(new_task_data)

        logging.info("Starting Service")
        service.start()
        self.assertFalse( service.in_error() )
        logging.info("Starting Tasks")
        job = service.submit(new_task)
        self.assertTrue( isinstance(job, TaskJob) )
        self.assertFalse( service.in_error() )
        #logging.info("Waiting")
        service.wait([job])

        self.assertIn(job.get_status(), ['ok'])

        self.assertFalse( service.in_error() )