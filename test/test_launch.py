

import unittest
import time
import os
import shutil
import nebula.builder
import nebula.service
from nebula.docstore import FileDocStore

class TestLaunch(unittest.TestCase):

    def setUp(self):
        if not os.path.exists("./test_tmp"):
            os.mkdir("test_tmp")
        self.service = None

    def tearDown(self):
        if self.service is not None:
            self.service.stop()

        if os.path.exists("./test_tmp/docstore"):
            shutil.rmtree("./test_tmp/docstore")


    def testCapture(self):
        capture = nebula.builder.init_capture()

    def testServiceDescription(self):
        store = FileDocStore("./test_tmp/docstore")
        service = nebula.service.GalaxyService(store)
        service_dict = service.to_dict()
        self.assertIn('service_name', service_dict)
        self.assertEqual('galaxy', service_dict['service_name'])
        print service_dict


    def testServiceStart(self):
        store = FileDocStore("./test_tmp/docstore")
        self.service = nebula.service.GalaxyService(
            store,
            name="nosetest_galaxy",
            force=True,
            port=20022
        )
        self.service.start()
        time.sleep(10)
        self.assertFalse(self.service.in_error())
        self.service.stop()
        self.service = None
