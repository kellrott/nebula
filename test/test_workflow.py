
import json
import shutil
import unittest
import time
import os
from nebula.galaxy import GalaxyWorkflow, GalaxyService, GalaxyWorkflowTask
from nebula import Target
from nebula.docstore import FileDocStore
from nebula.docstore.util import sync_doc_dir

def get_abspath(path):
    return os.path.join(os.path.dirname(__file__), path)

class TestWorkflow(unittest.TestCase):

    def setUp(self):
        if not os.path.exists("./test_tmp"):
            os.mkdir("test_tmp")
        self.service = None

    def tearDown(self):
        if os.path.exists("./test_tmp/docstore"):
            shutil.rmtree("./test_tmp/docstore")
        if self.service is not None:
            self.service.stop()
            self.service = None

    def testWorkflow(self):

        input_file_1 = Target(uuid="c39ded10-6073-11e4-9803-0800200c9a66")
        input_file_2 = Target(uuid="26fd12a2-9096-4af2-a989-9e2f1cb692fe")
        workflow = GalaxyWorkflow(ga_file=get_abspath("../examples/simple_galaxy/SimpleWorkflow.ga"))
        task = GalaxyWorkflowTask("workflow_test",
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
        new_task = GalaxyWorkflowTask.from_dict(new_task_data)

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
        task_ok = GalaxyWorkflowTask("workflow_ok",
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

        task_missing = GalaxyWorkflowTask("workflow_broken",
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

