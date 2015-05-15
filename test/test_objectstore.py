


import unittest

import os
import shutil
import nebula.docstore
from nebula.target import Target

def get_abspath(path):
    return os.path.join(os.path.dirname(__file__), path)

class DocStoreTest(unittest.TestCase):

    def setUp(self):
        if not os.path.exists(get_abspath("../test_tmp")):
            os.mkdir(get_abspath("../test_tmp"))

    def tearDown(self):
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
            cache=get_abspath("../test_tmp/cache_1"))
        f_uuid = "c39ded10-6073-11e4-9803-0800200c9a66"
        t = Target(f_uuid)
        docstore_1.update_from_file(t, get_abspath("../examples/simple_galaxy/P04637.fasta"), create=True)

        docstore_2 = nebula.docstore.FileDocStore(get_abspath("../test_tmp/docstore"),
            cache=get_abspath("../test_tmp/cache_2"))

        self.assertTrue(docstore_2.exists(t))
