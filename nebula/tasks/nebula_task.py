
import os
import logging

from nebula.dag import TaskNode

class NebulaTask(TaskNode):
    def __init__(self, path):
        super(NebulaTask,self).__init__("nebula:/" + path, None, None)
        self.path = os.path.abspath(path)
        self.task_map = None
    
    def parse(self):
        from nebula.parser import NebulaCompile
        logging.info("Parsing SubNode: %s" % (self.path))        
        parser = NebulaCompile()
        parser.compile(self.path)
        self.task_map = parser.target_map
        for v in self.task_map.values():
            v.src_path = self.path
        self.init_outputs(self.task_map)
    
    def get_outputs(self):
        if self.task_map is None:
            self.parse()        
        print "nebula outputs", self.outputs
        return self.outputs
        
    def sub_targets(self):
        if self.task_map is None:
            self.parse()        
        return self.task_map
    
    def is_active_task(self):
        return False