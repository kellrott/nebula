
import unittest

import os
import shutil
import logging
import time
import nebula.docstore
from nebula.docstore.util import sync_doc_dir
from nebula.target import Target
from nebula.service import GalaxyService, TaskJob
from nebula.galaxy import GalaxyWorkflow
import nebula.tasks

logging.basicConfig(level=logging.INFO)

def get_abspath(path):
    return os.path.join(os.path.dirname(__file__), path)

class DocStoreTest(unittest.TestCase):

    def setUp(self):
        if not os.path.exists(get_abspath("../test_tmp")):
            os.mkdir(get_abspath("../test_tmp"))
        self.service = None

    def tearDown(self):
        if self.service is not None:
            self.service.stop()
            self.service = None
            time.sleep(5)

        for a in ["../test_tmp/docstore", "../test_tmp/cache_1", "../test_tmp/cache_2"]:
            if os.path.exists(get_abspath(a)):
                shutil.rmtree(get_abspath(a))

    def testDocStore(self):
        docstore = nebula.docstore.from_url(get_abspath("../test_tmp/docstore"))
        f_uuid = "c39ded10-6073-11e4-9803-0800200c9a66"
        t = Target(f_uuid)
        docstore.update_from_file(t, get_abspath("../examples/simple_galaxy/P04637.fasta"), create=True)
        self.assertEqual(docstore.size(t), os.stat( get_abspath("../examples/simple_galaxy/P04637.fasta") ).st_size)


    def testCaching(self):
        docstore_1 = nebula.docstore.FileDocStore(get_abspath("../test_tmp/docstore"),
            cache_path=get_abspath("../test_tmp/cache_1"))
        f_uuid = "c39ded10-6073-11e4-9803-0800200c9a66"
        t = Target(f_uuid)
        docstore_1.update_from_file(t, get_abspath("../examples/simple_galaxy/P04637.fasta"), create=True)

        docstore_2 = nebula.docstore.FileDocStore(get_abspath("../test_tmp/docstore"),
            cache_path=get_abspath("../test_tmp/cache_2"))

        self.assertTrue(docstore_2.exists(t))
        print docstore_2.get_filename(t)

    def testWorkflowCaching(self):
        input = {
            "input_file_1" : Target("c39ded10-6073-11e4-9803-0800200c9a66"),
            "input_file_2" : Target("26fd12a2-9096-4af2-a989-9e2f1cb692fe")
        }
        parameters = {
            "tail_select" : {
                "lineNum" : 3
            }
        }

        doc = nebula.docstore.FileDocStore(
            get_abspath("../test_tmp/docstore"),
            cache_path=get_abspath("../test_tmp/cache")
        )

        logging.info("Adding files to object store")
        sync_doc_dir(get_abspath("../examples/simple_galaxy/"), doc,
            uuid_set=["c39ded10-6073-11e4-9803-0800200c9a66", "26fd12a2-9096-4af2-a989-9e2f1cb692fe"]
        )
        logging.info("Creating Task")
        workflow = GalaxyWorkflow(ga_file=get_abspath("../examples/simple_galaxy/SimpleWorkflow.ga"))
        task = nebula.tasks.GalaxyWorkflowTask(
            "test_workflow", workflow,
            inputs=input,
            parameters=parameters,
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

        service = GalaxyService(
            docstore=doc,
            name="nosetest_galaxy",
            galaxy="bgruening/galaxy-stable:dev",
            force=True,
            port=20022
        )
        self.service = service

        logging.info("Starting Service")
        print "Starting service"
        service.start()
        self.assertFalse( service.in_error() )
        logging.info("Starting Tasks")
        job = service.submit(task)
        self.assertTrue( isinstance(job, TaskJob) )
        self.assertFalse( service.in_error() )
        #logging.info("Waiting")
        service.wait([job])
        found = False
        for id, info in doc.filter(tags="file:output"):
            logging.info("Found result object: %s size: %d" % (id, doc.size(info)))
            self.assertTrue( doc.size(info) > 0 )
            found = True
        self.assertTrue(found)
        self.assertFalse( service.in_error() )
        self.assertIn(job.get_status(), ['ok'])
