
import os

class Workflow(object):
    def __init__(self, desc):
        self.desc = desc

class ToolBox(object):
    def __init__(self):
        self.config_files = {}
        self.tools = {}


class Tool(object):
    def __init__(self, config_file):
        self.config_file = os.path.abspath(config_file)
