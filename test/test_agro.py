
import pyagro
from pyagro import agro_pb2
import unittest
import time
import os
import uuid
from nebula import Target
from nebula.galaxy import GalaxyWorkflow, GalaxyService
from nebula.docstore import AgroDocStore
from nebula.docstore.util import sync_doc_dir
import json
import shutil

def get_abspath(path):
    return os.path.join(os.path.dirname(__file__), path)

AGRO_SERVER="localhost:9713"

type_map = {
    int.__name__ : "int",
    long.__name__ : "int",
    float.__name__ : "float",
    str.__name__ : "str",
    unicode.__name__ : "str",
    bool.__name__ : "bool",
    list.__name__ : "list",
    dict.__name__ : "dict",
    'NoneType' : "None"
}

def compare_dicts(a, b):
    a_s = set(a.keys())
    b_s = set(b.keys())
    
    a_d = a_s.difference(b_s)
    b_d = b_s.difference(a_s)
    print a
    print b
    assert(len(a_d) == 0 or a_d == set(['_id']))
    assert(len(b_d) == 0 or b_d == set(['_id']))
    
    for k in a_s:
        assert( type_map[type(a[k]).__name__] == type_map[type(b[k]).__name__] )
        if isinstance(a[k], dict):
            compare_dicts(a[k], b[k])
        

    

class TestAgro(unittest.TestCase):

    def setUp(self):
        if not os.path.exists("./test_tmp"):
            os.mkdir("test_tmp")
        self.service = None

    def tearDown(self):
        if os.path.exists("./test_tmp/docstore"):
            shutil.rmtree("./test_tmp/docstore")
        if self.service is not None:
            self.service.stop()
            self.service = None
    
    def testPack(self):
        with open(get_abspath("../examples/simple_galaxy/SimpleWorkflow.ga")) as handle:
            in_doc = json.loads(handle.read())
        
        doc = pyagro.pack_doc(str(uuid.uuid4()), in_doc )
        out_doc = pyagro.unpack_doc(doc)
        
        compare_dicts(in_doc, out_doc)

    def testDocStore_1(self):
        docstore = AgroDocStore(AGRO_SERVER)
        
        with open(get_abspath("../examples/simple_galaxy/SimpleWorkflow.ga")) as handle:
            in_doc = json.loads(handle.read())
        
        u = str(uuid.uuid4())
        docstore.put(u, in_doc)
        
        out_doc = docstore.get(u)
        
        #print in_doc
        #print out_doc
        
        compare_dicts(in_doc, out_doc)
    
    def testDocStore_direct(self):
        docstore = AgroDocStore(AGRO_SERVER)
        
        with open(get_abspath("../examples/simple_galaxy/SimpleWorkflow.ga")) as handle:
            in_doc = json.loads(handle.read())
        
        u = str(uuid.uuid4())
        start_doc = pyagro.pack_doc(u, in_doc)
        docstore.filestore.CreateDoc(start_doc)
        end_doc = docstore.filestore.GetDoc(agro_pb2.FileID(id=u))
        
        print len(start_doc.fields), len(end_doc.fields)
        
        #for a,b in zip(start_doc.fields, end_doc.fields):
        #    print (a,b)

    def testDocStore_2(self):
        docstore = AgroDocStore(AGRO_SERVER)
        
        in_doc = {
            "a" : "a",
            "b" : [1,2,3],
            "c" : True,
            "d" : {
                "da" : ["hi", "there"]
            },
            "e" : [
                {"item":"a"},
                {"item":"b"}
            ]
        }
        
        u = str(uuid.uuid4())
        docstore.put(u, in_doc)
        
        out_doc = docstore.get(u)
        
        #print in_doc
        #print out_doc
        
        compare_dicts(in_doc, out_doc)
