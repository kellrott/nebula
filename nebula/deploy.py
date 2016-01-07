
import os
import json
import tempfile
import subprocess
import logging

def which(file):
    for path in os.environ["PATH"].split(":"):
        p = os.path.join(path, file)
        if os.path.exists(p):
            return p

class Deployer(object):
    
    def __init__(self, workdir="./workdir"):
        self.workdir = workdir


class CmdLineDeploy(Deployer):
    
    def __init__(self):
        super(CmdLineDeploy, self).__init__()
    
    def run(self, service, task):
        
        workdir = os.path.abspath(tempfile.mkdtemp(dir=self.workdir, prefix="nebula_"))
        
        service_path = os.path.join(workdir, "service")
        with open(service_path, "w") as handle:
            handle.write(json.dumps(service.to_dict()))
        task_path = os.path.join(workdir, "task")
        with open(task_path, "w") as handle:
            handle.write(json.dumps(task.to_dict()))

        docker_cmd = [which("docker"), "run"]
        docker_cmd.extend( ["--rm", "-v", "%s:%s" % (workdir,"/nebula") ])
        docker_cmd.extend( ["-p", "8080:8080"] )
        docker_cmd.extend( [ "-v", "/var/run/docker.sock:/var/run/docker.sock"])
        docker_cmd.append( service.get_docker_image() )
        docker_cmd.extend( service.get_wrapper_command() )
        docker_cmd.extend( ["--docstore", service.docstore.get_url()] )
        docker_cmd.append( "/nebula/service" )
        docker_cmd.append( "/nebula/task" )
        logging.info("Running: %s" % " ".join(docker_cmd))
        print("Running: %s" % " ".join(docker_cmd))
        
        proc = subprocess.Popen( docker_cmd )
        proc.communicate()
        