

import unittest
import time
import os
import nebula.tasks
from nebula.target import Target
import json
import shutil

class TestWorkflow(unittest.TestCase):

    def setUp(self):
        if not os.path.exists("./test_tmp"):
            os.mkdir("test_tmp")

    def tearDown(self):
        if os.path.exists("./test_tmp/docstore"):
            shutil.rmtree("./test_tmp/docstore")

    def testWorkflow(self):

        input_file_1 = Target(uuid="c39ded10-6073-11e4-9803-0800200c9a66")
        input_file_2 = Target(uuid="26fd12a2-9096-4af2-a989-9e2f1cb692fe")
        task = nebula.tasks.GalaxyWorkflowTask("workflow_test",
            "examples/simple_galaxy/SimpleWorkflow.ga",
            inputs={
                'input_file_1' : input_file_1,
                'input_file_2' : input_file_2,
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
        task_ok = nebula.tasks.GalaxyWorkflowTask("workflow_ok",
            "examples/simple_galaxy/SimpleWorkflow.ga",
            inputs={
                'input_file_1' : input_file_1,
                'input_file_2' : input_file_2,
                "tail_select" : {
                    "lineNum" : 3
                }
            }
        )

        task_missing = nebula.tasks.GalaxyWorkflowTask("workflow_broken",
            "examples/simple_galaxy/SimpleWorkflow.ga",
            inputs={
                'input_file_1' : input_file_1,
                "tail_select" : {
                    "lineNum" : 3
                }
            }
        )

        self.assertTrue(task_ok.is_valid())
        self.assertFalse(task_missing.is_valid())
