
import os
import json
import uuid
import tempfile
import subprocess
import logging

try:
    import pyagro
    from pyagro import agro_pb2
except ImportError:
    pyagro = None
    agro_pb2 = None
    
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


class AgroDeploy(Deployer):
    
    def __init__(self, agro_url):
        super(AgroDeploy, self).__init__()
        self.agro_url = agro_url
    
    def run(self, service, task):
        
        client = pyagro.AgroClient(self.agro_url)
        workdir = os.path.abspath(tempfile.mkdtemp(dir=self.workdir, prefix="nebula_"))
        
        sched = client.scheduler()
        files = client.filestore()
        
        job_cmd = []
        job_cmd.extend( service.get_wrapper_command() )
        job_cmd.extend( ["--docstore", service.docstore.get_url()] )
        job_cmd.append( "/nebula/service" )
        job_cmd.append( "/nebula/task" )
        
        
        agro_task = agro_pb2.Task()
        task_id = str(uuid.uuid4())
        agro_task.id = task_id
        agro_task.command = job_cmd[0]
        agro_task.container = service.get_docker_image()
        
        agro_task.args.add( arg="--docstore" )
        agro_task.args.add( arg=service.docstore.get_url() )

        service_file_id = str(uuid.uuid4())
        pyagro.upload_file_str(files, service_file_id, json.dumps(service.to_dict()), "service")
        agro_task.args.add( file_arg=agro_pb2.FileArgument(
            id=service_file_id, 
            input=True, 
            silent=False,
            type=agro_pb2.FileArgument.PATH),
        )
        
        task_file_id = str(uuid.uuid4())
        pyagro.upload_file_str(files, task_file_id, json.dumps(task.to_dict()), "task")
        agro_task.args.add( file_arg=agro_pb2.FileArgument(
            id=task_file_id, 
            input=True, 
            silent=False,
            type=agro_pb2.FileArgument.PATH),
        )
        
        agro_task.tags.extend( ['testing'] )
        print "Adding task"
        sched.AddTask(agro_task)

        e = pyagro.wait(sched, task_id)
        assert(e == 0)
        print "Result", e
        
        sched = None
        files = None
        
        