

import unittest
import time
import os
import nebula.tasks
from nebula.galaxy import GalaxyWorkflow
from nebula.service import GalaxyService, TaskJob
from nebula.target import Target
from nebula.docstore import FileDocStore
from nebula.docstore.util import sync_doc_dir
import json
import shutil

def get_abspath(path):
    return os.path.join(os.path.dirname(__file__), path)

class TestWorkflow(unittest.TestCase):

    def setUp(self):
        if not os.path.exists("./test_tmp"):
            os.mkdir("test_tmp")
        self.service = None

    def tearDown(self):
        #if os.path.exists("./test_tmp/docstore"):
        #    shutil.rmtree("./test_tmp/docstore")
        if self.service is not None:
            self.service.stop()
            self.service = None

    def testWorkflow(self):

        input_file_1 = Target(uuid="c39ded10-6073-11e4-9803-0800200c9a66")
        input_file_2 = Target(uuid="26fd12a2-9096-4af2-a989-9e2f1cb692fe")
        workflow = GalaxyWorkflow(ga_file=get_abspath("../examples/simple_galaxy/SimpleWorkflow.ga"))
        task = nebula.tasks.GalaxyWorkflowTask("workflow_test",
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

        task_data = task.to_dict()
        task_data_str = json.dumps(task_data)
        new_task_data = json.loads(task_data_str)
        new_task = nebula.tasks.from_dict(new_task_data)

        self.assertEqual(len(task.get_inputs()), len(new_task.get_inputs()))

        task_inputs = task.get_inputs()
        new_task_inputs = new_task.get_inputs()

        for k,v in task_inputs.items():
            self.assertIn(k, new_task_inputs)
            self.assertEqual( v, new_task_inputs[k] )

    def testWorkflowCheck(self):

        input_file_1 = Target(uuid="c39ded10-6073-11e4-9803-0800200c9a66")
        input_file_2 = Target(uuid="26fd12a2-9096-4af2-a989-9e2f1cb692fe")
        workflow = GalaxyWorkflow(ga_file=get_abspath("../examples/simple_galaxy/SimpleWorkflow.ga"))
        task_ok = nebula.tasks.GalaxyWorkflowTask("workflow_ok",
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

        task_missing = nebula.tasks.GalaxyWorkflowTask("workflow_broken",
            workflow,
            inputs={
                'input_file_1' : input_file_1,
                "tail_select" : {
                    "lineNum" : 3
                }
            }
        )

        self.assertTrue(task_ok.is_valid())
        self.assertFalse(task_missing.is_valid())


    def testWorkflowOutputs(self):
        workflow = GalaxyWorkflow(ga_file=get_abspath("../examples/simple_galaxy/SimpleWorkflow.ga"))
        inputs = workflow.get_inputs()
        self.assertIn('input_file_1', inputs)
        self.assertIn('input_file_2', inputs)

        outputs = workflow.get_outputs()
        print outputs

        all_outputs = workflow.get_outputs(all=True)
        print all_outputs

        hidden_outputs = workflow.get_hidden_outputs()
        print hidden_outputs


    def testWorkflowTagging(self):

        doc = FileDocStore(file_path=get_abspath("../test_tmp/docstore"))
        sync_doc_dir(get_abspath("../examples/simple_galaxy/"), doc,
            uuid_set=["c39ded10-6073-11e4-9803-0800200c9a66", "26fd12a2-9096-4af2-a989-9e2f1cb692fe"]
        )

        input_file_1 = Target(uuid="c39ded10-6073-11e4-9803-0800200c9a66")
        input_file_2 = Target(uuid="26fd12a2-9096-4af2-a989-9e2f1cb692fe")
        workflow = GalaxyWorkflow(ga_file=get_abspath("../examples/simple_galaxy/SimpleWorkflow.ga"))
        task_tag = nebula.tasks.GalaxyWorkflowTask("workflow_ok",
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
