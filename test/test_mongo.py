
import unittest

import os
import shutil
import logging
import time
import signal
import uuid
import subprocess
from urlparse import urlparse

import nebula.docstore
from urllib2 import urlopen, URLError
from nebula.docstore.util import sync_doc_dir
from nebula.docstore.mongo import MongoStore

from nebula.warpdrive import call_docker_run, call_docker_kill, call_docker_rm

from nebula.target import Target
from nebula.service import GalaxyService, TaskJob
from nebula.galaxy import GalaxyWorkflow
import nebula.tasks
import threading

logging.basicConfig(level=logging.INFO)

def get_abspath(path):
    return os.path.join(os.path.dirname(__file__), path)


class DocStoreTest(unittest.TestCase):

    def setUp(self):
        if not os.path.exists(get_abspath("../test_tmp")):
            os.mkdir(get_abspath("../test_tmp"))
        self.mongo_url = None

    def tearDown(self):
        if self.mongo_url is None:
            self.stop_mongo()
            
                
    def start_mongo(self):
        if 'DOCKER_HOST' in os.environ:
            n = urlparse(os.environ['DOCKER_HOST'])
            self.host_ip = n.netloc.split(":")[0]
        else:
            self.host_ip = "localhost"
        logging.info("Using HostIP: %s" % (self.host_ip))

        call_docker_run(
            "mongo", ports={27017:27017},
            name="nebula_test_mongo"
            )
        self.mongo_url = "mongodb://%s:27017" % (self.host_ip)
        
    def stop_mongo(self):
        call_docker_rm(
            name="nebula_test_mongo", volume_delete=True
        )
        self.mongo_url = None

    def testDocStore(self):
        self.start_mongo()
        
        docstore = MongoStore(self.mongo_url)

        logging.info("")
        test_records = {}
        for i in range(10):
            t = {
                "uuid" : str(uuid.uuid4()),
                "name" : "testing_record_%d" % (i),
                "value_1" : "the first value"

            }
            test_records[t['uuid']] = t

        for t in test_records.values():
            docstore.put(t['uuid'], t)

        
        for id, doc in docstore.filter():
            self.assertIn(id, test_records)
        
        found_count = 0
        for id, doc in docstore.filter(name="testing_record_5"):
            self.assertEqual(doc['name'], "testing_record_5")
            found_count += 1
        self.assertEqual(found_count, 1)

    