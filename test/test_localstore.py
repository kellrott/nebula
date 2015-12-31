
import unittest

import os
import shutil
import logging
import time
import nebula.docstore
from nebula.docstore.util import sync_doc_dir
from nebula import Target
from nebula.galaxy import GalaxyService, GalaxyWorkflow

logging.basicConfig(level=logging.INFO)

def get_abspath(path):
    return os.path.abspath(os.path.join(os.path.dirname(__file__), path))

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
        docstore = nebula.docstore.LocalDocStore(get_abspath("../test_tmp/docstore"))
        f_uuid = "c39ded10-6073-11e4-9803-0800200c9a66"
        t = Target(f_uuid)
        docstore.update_from_file(t, get_abspath("../examples/simple_galaxy/P04637.fasta"), create=True)

        self.assertEqual(docstore.size(t), os.stat( get_abspath("../examples/simple_galaxy/P04637.fasta") ).st_size)
        self.assertEqual(docstore.get_filename(t), get_abspath("../examples/simple_galaxy/P04637.fasta"))
        
        