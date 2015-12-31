

import os
import unittest
import time
import shutil
import logging
import json
import nebula.tasks
from nebula.docstore import FileDocStore
from nebula.docstore.util import sync_doc_dir
from nebula import Target
from nebula.galaxy import GalaxyService, GalaxyWorkflow

class TestRunWorkflow(unittest.TestCase):

    def setUp(self):
        if not os.path.exists("./test_tmp"):
            os.mkdir("test_tmp")
        self.service = None

    def tearDown(self):
        if self.service is not None:
            self.service.stop()
            self.service = None
            time.sleep(5)

        if os.path.exists("./test_tmp/docstore"):
            shutil.rmtree("./test_tmp/docstore")

    def testRunSimple(self):
        input = {
            "input_file_1" :
                Target("c39ded10-6073-11e4-9803-0800200c9a66"),
            "input_file_2" :
                Target("26fd12a2-9096-4af2-a989-9e2f1cb692fe")
        }
        parameters = {
            "tail_select" : {
                "lineNum" : 3
            }
        }
        bad_parameters = dict(parameters)
        del bad_parameters['tail_select']

        doc = FileDocStore(file_path="./test_tmp/docstore")
        logging.info("Adding files to object store")
        sync_doc_dir("examples/simple_galaxy/", doc,
            uuid_set=["c39ded10-6073-11e4-9803-0800200c9a66", "26fd12a2-9096-4af2-a989-9e2f1cb692fe"]
        )
        logging.info("Creating Task")
        workflow = GalaxyWorkflow(ga_file="examples/simple_galaxy/SimpleWorkflow.ga")
        task = nebula.tasks.GalaxyWorkflowTask(
            "test_workflow", workflow,
            inputs=input,
            parameters=parameters
        )

        task_data = task.to_dict()
        #make sure the task data can be serialized
        task_data_str = json.dumps(task_data)

        service = GalaxyService(
            docstore=doc,
            name="nosetest_galaxy",
            galaxy="bgruening/galaxy-stable",
            force=True,
            port=20022
        )
        self.service = service

        #make sure the generated task is serializable
        new_task_data = json.loads(task_data_str)
        new_task = nebula.tasks.from_dict(new_task_data)

        logging.info("Starting Service")
        print "Starting service"
        service.start()
        self.assertFalse( service.in_error() )
        logging.info("Starting Tasks")
        job = service.submit(new_task)
        self.assertTrue( isinstance(job, TaskJob) )
        self.assertFalse( service.in_error() )
        #logging.info("Waiting")
        service.wait([job])
        self.assertIn(job.get_status(), ['ok'])

        bad_task = nebula.tasks.GalaxyWorkflowTask(
            "test_workflow_bad",
            workflow,
            inputs=input,
            parameters=bad_parameters
        )
        job = service.submit(bad_task)
        service.wait([job])
        self.assertIn(job.get_status(), ['error'])

        self.assertFalse( service.in_error() )


    def testWorkflowTagging(self):

        doc = FileDocStore(file_path=get_abspath("../test_tmp/docstore"))
        sync_doc_dir(get_abspath("../examples/simple_galaxy/"), doc,
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
            docstore=doc,
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


    def testToolTagging(self):

        doc = FileDocStore(file_path=get_abspath("../test_tmp/docstore"))
        sync_doc_dir(get_abspath("../examples/simple_galaxy/"), doc,
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
            tags = [
                "run:testing"
            ],
            tool_tags= {
                "tail_select" : {
                    "out_file1" : [
                        "file:tail"
                    ]
                },
                "concat_out" : {
                    "out_file1" : ["file:output"]
                }
            }
        )
        print "Starting Service"
        service = GalaxyService(
            docstore=doc,
            name="nosetest_galaxy",
            galaxy="bgruening/galaxy-stable:dev",
            force=True,
            port=20022
        )
        service.start()
        self.service = service
        job = service.submit(task_tag)
        print "JOB", job.get_status()
        service.wait([job])
        self.assertIn(job.get_status(), ['ok'])
        self.assertFalse( service.in_error() )
        print service.in_error()
