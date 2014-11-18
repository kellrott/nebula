#!/usr/bin/env python

"""

To test:
./scripts/run_galaxy_workflow.py -d examples/simple_galaxy \
-t examples/simple_galaxy \
-w examples/simple_galaxy/SimpleWorkflow.ga \
-l examples/simple_galaxy/ examples/simple_galaxy/input.json

"""


import os
import re
import json
import time
import logging
from urlparse import urlparse
from glob import glob
from argparse import ArgumentParser
from nebula.dag import Target
from nebula.tasks import GalaxyWorkflow
from nebula.service import GalaxyService, TaskJob, ServiceFactory
from nebula.objectstore import DiskObjectStore, DiskObjectStoreConfig
from nebula.docstore import FileDocStore

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

    o_url = urlparse(args.object_store)
    if o_url.scheme == '':
        obj = DiskObjectStore(DiskObjectStoreConfig(job_work=args.local_store, new_file_path=args.local_store), file_path=o_url.path)
    else:
        raise Exception("Object Store type not supported: %s" % (o_url.scheme))

    d_url = urlparse(args.doc_store)
    if d_url.scheme == '':
        doc = FileDocStore(file_path=d_url.path)
    else:
        raise Exception("Object Store type not supported: %s" % (o_url.scheme))


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
                    doc.put(t.uuid, t.to_dict())
                inputs[k] = t
        task = GalaxyWorkflow('task_%s' % (i), args.workflow, inputs=inputs, docker=args.galaxy, tool_dir=args.tools)
        task_data = task.get_task_data()
        tasks.append(task_data)

    #this side happens on the worker node
    service = ServiceFactory('galaxy', objectstore=obj,
        lib_data=[args.object_store], tool_dir=args.tools,
        docker_tag=args.galaxy, work_dir=args.warpdrive_dir, sudo=args.sudo,
        tool_docker=True)
    service.start()
    job_ids = []
    for task_data in tasks:
        task = TaskJob(task_data)
        i = service.submit(task)
        job_ids.append(i)

    while True:
        waiting = False
        for i in job_ids:
            status = service.status(i)
            print "Status", status
            if status not in ['ok', 'error']:
                waiting = True
        if not waiting:
            break
        time.sleep(1)

    #move the output data into the datastore
    for i in job_ids:
        job = service.get_job(i)
        for a in job.get_outputs():
            service.store_data(a, obj)
            service.store_meta(a, doc)

    print "Done"
    service.stop()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-g", "--galaxy", help="Galaxy Runner Image", default="galaxy")
    parser.add_argument("-c", "--warpdrive-dir", help="Directory For Warpdrive config mounting", default="/tmp")
    parser.add_argument("-w", "--workflow", help="Galaxy Workflow File", required=True)
    parser.add_argument("-d", "--data", help="Data directory (metadata as .json files)", required=True)
    parser.add_argument("-t", "--tools", help="Tool Directory", required=True)
    parser.add_argument("-s", "--object-store", default="./nebula_data")
    parser.add_argument("-b", "--doc-store", default="./nebula_docs")
    parser.add_argument("-l", "--local-store", default="./nebula_work")
    parser.add_argument("--sudo", action="store_true", default=False)


    parser.add_argument("inputs", nargs="+", default=[])

    args = parser.parse_args()
    run_workflow(args)
