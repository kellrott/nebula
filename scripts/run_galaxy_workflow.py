#!/usr/bin/env python

import os
import re
import json
import time
import logging
from glob import glob
from argparse import ArgumentParser
from nebula.dag import Target
from nebula.tasks import GalaxyWorkflow
from nebula.service import GalaxyService, TaskJob, ServiceFactory
from nebula.objectstore import DiskObjectStore, DiskObjectStoreConfig

logging.basicConfig(level=logging.DEBUG)

def run_workflow(args):
    data_map = {}
    for meta_path in glob(os.path.join(args.data, "*.json")):
        data_path = re.sub(r'.json$', "", meta_path)
        if os.path.exists(data_path):
            try:
                with open(meta_path) as handle:
                    meta = json.loads(handle.read())
                    if 'uuid' in meta:
                        data_map[meta['uuid']] = data_path
            except:
                pass
                
    print data_map
    obj = DiskObjectStore(DiskObjectStoreConfig(job_work=args.local_store, new_file_path=args.local_store), file_path=args.object_store)

    #this side happens on the master node
    tasks = []
    for i, input_file in enumerate(args.inputs):
        with open(input_file) as handle:
            meta = json.loads(handle.read())
        inputs = {}
        for k, v in meta.items():
            if isinstance(v,dict):
                t = Target(v['uuid'])
                if not obj.exists(t):
                    if t.uuid not in data_map:
                        raise Exception("Can't find input data: %s" % (t.uuid))
                    obj.update_from_file(t, data_map[t.uuid], create=True)
                inputs[k] = t
        task = GalaxyWorkflow('task_%s' % (i), args.workflow, inputs=inputs, docker=args.galaxy, tool_dir=args.tools)
        task_data = task.get_task_data()
        tasks.append(task_data)

    #this side happens on the worker node
    service = ServiceFactory('galaxy', objectstore=obj, lib_data=[args.object_store], tool_dir=args.tools, docker_tag=args.galaxy)
    service.start()
    job_ids = []
    for task_data in tasks:
        task = TaskJob(task_data)
        i = service.submit(task)
        job_ids.append(i)

    while True:
        waiting = False
        for i in job_ids:
            if service.status(i) not in ['ok', 'error']:
                waiting = True
        if not waiting:
            break
        time.sleep(1)
    print "Done"
    service.stop()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-g", "--galaxy", help="Galaxy Runner Image", default="galaxy")
    parser.add_argument("-w", "--workflow", help="Galaxy Workflow File", required=True)
    parser.add_argument("-d", "--data", help="Data directory (metadata as .json files)", required=True)
    parser.add_argument("-t", "--tools", help="Tool Directory", required=True)
    parser.add_argument("-s", "--object-store", default="./nebula_data")
    parser.add_argument("-l", "--local-store", default="./nebula_work")
    
    parser.add_argument("inputs", nargs="+", default=[])

    args = parser.parse_args()
    run_workflow(args)
