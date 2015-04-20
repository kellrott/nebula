


import unittest

import os
import shutil
import nebula.docstore


class DocStoreTest(unittest.TestCase):

    def setUp(self):
        if not os.path.exists("./test_tmp"):
            os.mkdir("test_tmp")

    def tearDown(self):
        if os.path.exists("./test_tmp/docstore"):
            shutil.rmtree("./test_tmp/docstore")

    def testDocStore(self):
        nebula.docstore.init_docstore_url("./test_tmp/docstore")
