
import unittest

import os
import shutil
import logging
import time
import signal
import uuid
import subprocess
import nebula.docstore
from urllib2 import urlopen, URLError
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

def which(file):
    for path in os.environ["PATH"].split(":"):
        p = os.path.join(path, file)
        if os.path.exists(p):
            return p


class ObjectSpaceRunner(threading.Thread):
    def __init__(self):
        self.running = False
        threading.Thread.__init__(self)

    def run(self):
        self.running = True
        env = dict(os.environ)
        env['GOPATH'] = get_abspath("../objectspace/")
        subprocess.check_call( [which("go"), "build", "objectspace.go"],
            cwd=get_abspath("../objectspace/"), env=env)
        cmd = ["./objectspace", "../test_tmp/object_db"]
        proc = subprocess.Popen(cmd,
            cwd=get_abspath("../objectspace/"), env=env)
        print "Server Started"
        while self.running:
            time.sleep(1)

        proc.send_signal(signal.SIGTERM)
        proc.wait()
        print "Server Stopped"


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
                os.unlink(get_abspath(a))

    def testDocStore(self):
        self.os_thread = ObjectSpaceRunner()
        self.os_thread.start()
        while True:
            try:
                urlopen("http://localhost:18888").read()
                break
            except URLError:
                pass
        time.sleep(1)

        docstore = ObjectSpace("http://localhost:18888")

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

        sync_doc_dir(get_abspath("../examples/simple_galaxy/"), docstore,
            uuid_set=["c39ded10-6073-11e4-9803-0800200c9a66", "26fd12a2-9096-4af2-a989-9e2f1cb692fe"]
        )

        """
        docstore = nebula.docstore.from_url(get_abspath("../test_tmp/docstore"))
        f_uuid = "c39ded10-6073-11e4-9803-0800200c9a66"
        t = Target(f_uuid)
        docstore.update_from_file(t, get_abspath("../examples/simple_galaxy/P04637.fasta"), create=True)
        self.assertEqual(docstore.size(t), os.stat( get_abspath("../examples/simple_galaxy/P04637.fasta") ).st_size)
        """
