


import unittest

import os
import json
import shutil
import nebula.docstore
from glob import glob

from nebula.target import TargetFile
from nebula.docstore import FileDocStore
from nebula.service import GalaxyService
from nebula.tasks import GalaxyWorkflowTask, TaskGroup
from nebula.galaxy import GalaxyWorkflow

def get_abspath(path):
    return os.path.join(os.path.dirname(__file__), path)


class DocStoreTest(unittest.TestCase):

    def setUp(self):
        if not os.path.exists(get_abspath("../test_tmp")):
            os.mkdir(get_abspath("../test_tmp"))

    def tearDown(self):
        if os.path.exists(get_abspath("../test_tmp/docstore")):
            shutil.rmtree(get_abspath("../test_tmp/docstore"))

    def testTaskGenerate(self):
        targets = []
        for a in glob(get_abspath("../examples/simple_galaxy/*.fasta")):
            targets.append(TargetFile(a))

        tasks = TaskGroup()
        for i, t in enumerate(targets):
            workflow = GalaxyWorkflow(ga_file=get_abspath("../examples/simple_galaxy/SimpleWorkflow.ga"))
            task = GalaxyWorkflowTask("workflow_%s" % (i), workflow,
                inputs={
                'input_file' : t
                }
            )
            tasks.append(task)

        #check if elements can be serialized
        for a in tasks.to_dict():
            task_json = json.dumps(a)

        with open(get_abspath("../test_tmp/nebula_tasks"), "w") as handle:
            tasks.store(handle)

        new_tasks = TaskGroup()
        with open(get_abspath("../test_tmp/nebula_tasks")) as handle:
            new_tasks.load(handle)

        self.assertEqual(len(tasks), len(new_tasks))
        for task in new_tasks:
            print task

    def testServiceGenerate(self):
        doc = FileDocStore(file_path=get_abspath("../test_tmp/docstore"))
        service = GalaxyService(
            docstore=doc,
            name="nosetest_galaxy",
            galaxy="bgruening/galaxy-stable:dev",
            port=20022
        )
        json.dumps(service.to_dict())
