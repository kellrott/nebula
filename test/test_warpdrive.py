

import unittest
import time
import os
import nebula
from nebula import warpdrive
import json
import shutil

def get_abspath(path):
    return os.path.join(os.path.dirname(__file__), path)

class TestWarpdrive(unittest.TestCase):

    def testLaunchContainer(self):
        warpdrive.run_up(name="nebula_galaxy", force=True, port=19999, tool_docker=True)
        warpdrive.run_status(name="nebula_galaxy")
        warpdrive.run_down(name="nebula_galaxy")
    
    
    def testToolConfig(self):
        env = {}
        warpdrive.config_tool_dir(get_abspath("../examples/md5_sum/"), 
            env=env, 
            config_path=get_abspath("../test_tmp/tool_conf.xml"))
        print env
    
    def testJobConfig(self):
        env = { }
        warpdrive.config_jobs(
            smp=[["cool_tool", 9],["other_tool",5],["other_tool2",5]], 
            env=env,
            parent_name='test', 
            job_conf_file=get_abspath("../test_tmp/job_conf.xml"), 
            default_container="nebula_galaxy", 
            plugin="local", handler="main")