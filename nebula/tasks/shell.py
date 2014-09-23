
import logging
import subprocess

from nebula.dag import TaskNode
from nebula.service import Docker

class Shell(TaskNode):
    def __init__(self, task_id, script, inputs, outputs, docker=None):
        print "Shell outputs", outputs
        super(Shell,self).__init__(task_id, inputs, outputs)
        self.script = script
        if docker is None:
            self.docker = Docker('debian')
        else:
            self.docker = docker

    def get_task_data(self, workrepo):
        sha1 = workrepo.get_dockerimage_sha1(self.docker.name)
        if sha1 is None:
            logging.info("Missing Docker Image: " + self.docker.name)
            if self.docker.path is not None:
                logging.info("Running Docker Build")
                cmd = "docker build -t %s %s" % (self.docker.name, self.docker.path)
                subprocess.check_call(cmd, shell=True)
                logging.info("Saving Docker Image: " + self.docker.name)
                cmd = "docker save %s > %s" % (self.docker.name, workrepo.get_dockerimage_path(self.docker.name))
                subprocess.check_call(cmd, shell=True)
        return {
            'task_id' : self.task_id,
            'task_type' : 'shell',
            'script' : self.script,
            'inputs' : self.get_input_data(),
            'outputs' : self.get_output_data(),
            'docker' : self.docker.name
        }
