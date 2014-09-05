
import os
import logging

from nebula.dag import TaskNode

class NebulaTask(TaskNode):
    def __init__(self, path):
        super(NebulaTask,self).__init__("nebula:/" + path, None)
        self.path = os.path.abspath(path)
        self.task_map = None
    
    def parse(self):
        from nebula.parser import NebulaCompile
        logging.info("Parsing SubNode: %s" % (self.path))        
        parser = NebulaCompile()
        parser.compile(self.path)
        self.task_map = parser.target_map
        
    def sub_targets(self):
        if self.task_map is None:
            self.parse()        
        return self.task_map