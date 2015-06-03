
import unittest

import os
import shutil
import logging
import time
import subprocess
import nebula.docstore
from nebula.docstore.util import sync_doc_dir
from nebula.docstore.objectspace import ObjectSpace
from nebula.target import Target
from nebula.service import GalaxyService, TaskJob
from nebula.galaxy import GalaxyWorkflow
import nebula.tasks
import threading

logging.basicConfig(level=logging.INFO)

def get_abspath(path):
    return os.path.join(os.path.dirname(__file__), path)


class ObjectSpaceRunner(threading.Thread):
    def __init__(self):
        self.running = False
        threading.Thread.__init__(self)

    def run(self):
        self.running = True
        env = dict(os.environ)
        env['GOPATH'] = get_abspath("../objectspace/")
        proc = subprocess.Popen("go run objectspace.go", shell=True,
            cwd=get_abspath("../objectspace/"), env=env)
        while self.running:
            time.sleep(1)

        proc.kill()


    def stop(self):
        self.running = False

class DocStoreTest(unittest.TestCase):

    def setUp(self):
        if not os.path.exists(get_abspath("../test_tmp")):
            os.mkdir(get_abspath("../test_tmp"))
        self.os_thread = None

    def tearDown(self):
        if self.os_thread is not None:
            self.os_thread.stop()
            self.os_thread = None
            time.sleep(5)

        for a in ["../test_tmp/object_db"]:
            if os.path.exists(get_abspath(a)):
                shutil.rmtree(get_abspath(a))

    def testDocStore(self):
        self.os_thread = ObjectSpaceRunner()
        self.os_thread.start()
        time.sleep(1)

        docstore = ObjectSpace("http://localhost:18888")

        for id, doc in docstore.filter():
            print id

        """
        docstore = nebula.docstore.from_url(get_abspath("../test_tmp/docstore"))
        f_uuid = "c39ded10-6073-11e4-9803-0800200c9a66"
        t = Target(f_uuid)
        docstore.update_from_file(t, get_abspath("../examples/simple_galaxy/P04637.fasta"), create=True)
        self.assertEqual(docstore.size(t), os.stat( get_abspath("../examples/simple_galaxy/P04637.fasta") ).st_size)
        """
