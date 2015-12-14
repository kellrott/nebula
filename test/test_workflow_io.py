

import unittest
import time
import os
import nebula.tasks
from nebula.galaxy import GalaxyWorkflow
import json
import shutil

def get_abspath(path):
    return os.path.join(os.path.dirname(__file__), path)

class TestWorkflow(unittest.TestCase):


    def testReadWorkflow(self):
        workflow = GalaxyWorkflow(ga_file=get_abspath("../examples/simple_galaxy/SimpleWorkflow.ga"))
        print workflow.get_inputs()
        print workflow.get_outputs()
        