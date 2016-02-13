
"""
Test Methods for generating workflow requests
"""


import unittest

import os
import json
import shutil
from glob import glob

from nebula import TargetFile, TaskGroup
from nebula.docstore import FileDocStore
from nebula.galaxy import GalaxyEngine, GalaxyWorkflowTask, GalaxyWorkflow

def get_abspath(path):
    """
    Given path relative to this .py file, get full path
    """
    return os.path.join(os.path.dirname(__file__), path)


class DocStoreTest(unittest.TestCase):
    """
    Test workflow generation and connections to the docstore
    """

    def setUp(self):
        if not os.path.exists(get_abspath("../test_tmp")):
            os.mkdir(get_abspath("../test_tmp"))

    def tearDown(self):
        if os.path.exists(get_abspath("../test_tmp/docstore")):
            shutil.rmtree(get_abspath("../test_tmp/docstore"))

    def test_task_generate(self):
        """
        Generate tasks
        """
        targets = []
        for input_path in glob(get_abspath("../examples/simple_galaxy/*.fasta")):
            targets.append(TargetFile(input_path))
        doc = FileDocStore(file_path=get_abspath("../test_tmp/docstore"))
        engine = GalaxyEngine(docstore=doc)
        tasks = TaskGroup()
        for target in targets:
            workflow = GalaxyWorkflow(
                ga_file=get_abspath("../examples/simple_galaxy/SimpleWorkflow.ga")
            )
            task = GalaxyWorkflowTask(engine, workflow,
                                      inputs={
                                          'input_file' : target
                                      }
                                     )
            tasks.append(task)

        #check if elements can be serialized
        for task_dict in tasks.to_dict():
            task_json = json.dumps(task_dict)
            assert task_json is not None

        with open(get_abspath("../test_tmp/nebula_tasks"), "w") as handle:
            tasks.store(handle)

        new_tasks = TaskGroup()
        with open(get_abspath("../test_tmp/nebula_tasks")) as handle:
            new_tasks.load(handle)

        self.assertEqual(len(tasks), len(new_tasks))
        for task in new_tasks:
            print task

    def test_service_generate(self):
        """
        Generate service data structure
        """
        doc = FileDocStore(file_path=get_abspath("../test_tmp/docstore"))
        service = GalaxyEngine(
            docstore=doc,
            name="nosetest_galaxy",
            galaxy="bgruening/galaxy-stable:dev",
            port=20022
        )
        json.dumps(service.to_dict())
