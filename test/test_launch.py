

import unittest
import time
import os
import shutil
from nebula.galaxy import GalaxyEngine
from nebula.docstore import FileDocStore

class TestLaunch(unittest.TestCase):

    def setUp(self):
        if not os.path.exists("./test_tmp"):
            os.mkdir("test_tmp")
        self.service = None

    def tearDown(self):
        if self.service is not None:
            self.service.stop()
            time.sleep(5)

        if os.path.exists("./test_tmp/docstore"):
            shutil.rmtree("./test_tmp/docstore")


    def testServiceDescription(self):
        store = FileDocStore("./test_tmp/docstore")
        service = GalaxyEngine(store)
        service_dict = service.to_dict()
        self.assertIn('service_type', service_dict)
        self.assertEqual('Galaxy', service_dict['service_type'])
        print service_dict


    def testServiceStart(self):
        store = FileDocStore("./test_tmp/docstore")
        self.service = GalaxyEngine(
            store,
            name="nosetest_galaxy",
            galaxy="bgruening/galaxy-stable:dev",
            force=True,
            port=20022
        )
        self.service.start()
        time.sleep(10)
        self.assertFalse(self.service.in_error())
