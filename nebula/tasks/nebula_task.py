
import os
import logging

from nebula.dag import TaskNode
from nebula.exceptions import CompileException

class NebulaTask(TaskNode):
    def __init__(self, path, inputs=None):
        super(NebulaTask,self).__init__("nebula:/" + path, None, None)
        self.path = os.path.abspath(path)
        # these are input references, the actual usage of the inputs will be
        # tracked at the task level
        self.input_refs = inputs
        self.task_map = None
        self.input_map = None

    def parse(self):
        from nebula.parser import NebulaCompile
        logging.info("Parsing SubNode: %s" % (self.path))
        parser = NebulaCompile()
        if parser.compile(self.path, self.input_refs):
            raise CompileException("Child Compile Failure")

        self.task_map = parser.target_map
        self.outputs = parser.output_map
        for o in self.outputs.values():
            o.parent_task = self
        for v in self.task_map.values():
            v.src_path = self.path

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
