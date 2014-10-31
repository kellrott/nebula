
import logging
import json
from nebula.service import GalaxyService, TaskJob, ServiceFactory

logging.basicConfig(level=logging.DEBUG)

with open("examples/simple_galaxy/SimpleWorkflow.ga") as handle:
    workflow = json.loads(handle.read())

task = TaskJob({
    'service' : 'galaxy',
    'task_id' : 'test_task',
    'task_type' : 'galaxy_workflow',
    'workflow' : workflow,
    'inputs' : { 'input_file' : {'uuid' : 'c39ded10-6073-11e4-9803-0800200c9a66', "path" : "examples/simple_galaxy/P04637.fasta"} },
    'docker' : 'galaxy'
})

service = ServiceFactory(service_name=task.service_name, lib_data=['examples/simple_galaxy/'], docker_tag="galaxy")

service.start()
service.submit(task)
