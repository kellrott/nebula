#!/usr/bin/env python

import json
import logging
from argparse import ArgumentParser
from nebula.dag import Target
from nebula.tasks import GalaxyWorkflow
from nebula.service import GalaxyService, TaskJob, ServiceFactory

logging.basicConfig(level=logging.DEBUG)

def run_workflow(args):

    #this side happens on the master node
    tasks = []
    for i, input_file in enumerate(args.inputs):
        with open(input_file) as handle:
            meta = json.loads(handle.read())
        inputs = {}
        for k, v in meta.items():
            if isinstance(v,dict):
                inputs[k] = Target(v['uuid'])
        task = GalaxyWorkflow('task_%s' % (i), args.workflow, inputs=inputs, docker=args.galaxy, tool_dir=args.tools)
        task_data = task.get_task_data()
        tasks.append(task_data)

    #this side happens on the worker node
    service = ServiceFactory('galaxy', lib_data=[args.data], tool_dir=args.tools, docker_tag="galaxy")
    service.start()
    job_ids = []
    for task_data in tasks:
        task = TaskJob(task_data)
        i = service.submit(task)
        job_ids.append(i)

    while True:
        waiting = False
        for i in job_ids:
            if service.status(i) not in ['DONE', 'ERROR']:
                waiting = True
        if not waiting:
            break
        time.sleep(1)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-g", "--galaxy", help="Galaxy Runner Image", default="galaxy")
    parser.add_argument("-w", "--workflow", help="Galaxy Workflow File", required=True)
    parser.add_argument("-d", "--data", help="Data directory (metadata as .json files)", required=True)
    parser.add_argument("-t", "--tools", help="Tool Directory", required=True)
    parser.add_argument("inputs", nargs="+", default=[])

    args = parser.parse_args()
    run_workflow(args)
