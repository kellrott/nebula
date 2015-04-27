

import os
import time
import json
import shutil
import logging
import unittest
import nebula.builder
import nebula.service
import subprocess
from nebula.tasks import TaskGroup
from nebula.docstore import FileDocStore
from nebula.docstore.util import sync_doc_dir
from nebula.service import GalaxyService, TaskJob

def get_abspath(path):
    return os.path.join(os.path.dirname(__file__), path)

class TestLaunch(unittest.TestCase):

    def setUp(self):
        if not os.path.exists(get_abspath("../test_tmp")):
            os.mkdir(get_abspath("../test_tmp"))

    def tearDown(self):
        if os.path.exists(get_abspath("../test_tmp/docstore")):
            shutil.rmtree(get_abspath("../test_tmp/docstore"))
        
    def testNebulaLaunch(self):
        input = {
            "input_file_1" :
                {"uuid" : "c39ded10-6073-11e4-9803-0800200c9a66"},
            "input_file_2" :
                {"uuid" : "26fd12a2-9096-4af2-a989-9e2f1cb692fe"},
            "tail_select" : {
                "lineNum" : 3
            }
        }

        doc = FileDocStore(file_path=get_abspath("../test_tmp/docstore"))
        logging.info("Adding files to object store")
        sync_doc_dir("examples/simple_galaxy/", doc,
            uuid_set=["c39ded10-6073-11e4-9803-0800200c9a66", "26fd12a2-9096-4af2-a989-9e2f1cb692fe"]
        )
        logging.info("Creating Task")
        task = nebula.tasks.GalaxyWorkflowTask(
            "test_workflow",
            get_abspath("../examples/simple_galaxy/SimpleWorkflow.ga"),
            inputs=input
        )

        service = GalaxyService(
            docstore=doc,
            name="nosetest_galaxy",
            galaxy="bgruening/galaxy-stable",
            port=20022
        )

        task_path = get_abspath("../test_tmp/test.tasks")
        service_path = get_abspath("../test_tmp/test.service")
        taskset = TaskGroup()
        taskset.append(task)
        with open(task_path, "w") as handle:
            taskset.store(handle)

        with open(service_path, "w") as handle:
            service.get_config().store(handle)

        env = dict(os.environ)
        if 'PYTHONPATH' in env:
            env['PYTHONPATH'] += ":" + get_abspath("../")
        else:
            env['PYTHONPATH'] = get_abspath("../")
        subprocess.check_call([get_abspath("../bin/nebula"), "run", service_path, task_path], env=env)
