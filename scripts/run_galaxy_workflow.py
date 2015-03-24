#!/usr/bin/env python

"""

To test:
./scripts/run_galaxy_workflow.py \
-t examples/simple_galaxy \
-w examples/simple_galaxy/SimpleWorkflow.ga \
-l examples/simple_galaxy/ examples/simple_galaxy/input.json

"""


import os
import re
import json
import time
import logging
import yaml
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
    for meta_path in glob(os.path.join(args['lib_data'], "*.json")):
        data_path = re.sub(r'.json$', "", meta_path)
        if os.path.exists(data_path):
            try:
                with open(meta_path) as handle:
                    meta = json.loads(handle.read())
                    if 'uuid' in meta:
                        data_map[meta['uuid']] = data_path
            except:
                pass

    d_url = urlparse(args['doc_store'])
    if d_url.scheme == '':
        doc = FileDocStore(file_path=d_url.path)
    else:
        raise Exception("Object Store type not supported: %s" % (o_url.scheme))


    #this side happens on the master node
    tasks = {}
    task_request = {}
    input_uuids = {}
    for i, input_file in enumerate(args['inputs']):
        with open(input_file) as handle:
            meta = json.loads(handle.read())
        inputs = {}
        for k, v in meta.get('ds_map').items():
            input_uuids[v['uuid']] = True
            t = Target(v['uuid'])
            if not doc.exists(t):
                if t.uuid not in data_map:
                    raise Exception("Can't find input data: %s" % (t.uuid))
                doc.update_from_file(t, data_map[t.uuid], create=True)
                doc.put(t.uuid, t.to_dict())
            inputs[k] = t
        params = meta.get("parameters", {})
        task_name = 'task_%s' % (i)
        if args['workflow'] is not None:
            task = GalaxyWorkflow(task_name, args['workflow'],
                inputs=inputs, parameters=params, tags=meta.get("tags", None),
                galaxy=args['galaxy'], tool_dir=args['tool_dir'], tool_data=args['tool_data'])
        else:
            with open(args['yaml_workflow']) as handle:
                yaml_text = handle.read()
            task = GalaxyWorkflow(task_name, yaml=yaml_text, inputs=inputs, parameters=params, tags=meta.get("tags", None), docker=args['galaxy'], tool_dir=args['tools'], tool_data=args['tool_data'])
        task_request[task_name] = meta
        task_data = task.get_task_data()
        tasks[task_name] = task_data

    #this side happens on the worker node
    service = ServiceFactory('galaxy', objectstore=doc,
        lib_data=[doc.file_path], tool_dir=args['tool_dir'], tool_data=args['tool_data'],
        galaxy=args['galaxy'], config_dir=args['config_dir'], sudo=args['sudo'], force=True,
        tool_docker=True, smp=args['smp'], cpus=args['cpus'])
    service.start()
    task_job_ids = {}
    for task_name, task_data in tasks.items():
        task = TaskJob(task_data)
        i = service.submit(task)
        task_job_ids[task_name] = i

    sleep_time = 1
    while True:
        waiting = False
        for i in task_job_ids.values():
            status = service.status(i)
            logging.info("Status check %s %s" % (status, i))
            if status not in ['ok', 'error']:
                waiting = True
        if not waiting:
            break
        time.sleep(sleep_time)
        if sleep_time < 60:
            sleep_time += 1

    #move the output data into the datastore
    for task_name, i in task_job_ids.items():
        job = service.get_job(i)
        if job.error is None:
            for a in job.get_outputs():
                meta = service.get_meta(a)
                #if 'tags' in task_request[task_name]:
                #    meta["tags"] = task_request[task_name]["tags"]
                #print "meta!!!", json.dumps(meta, indent=4)
                doc.put(meta['uuid'], meta)
                if meta.get('visible', True):
                    if meta['state'] == "ok":
                        if meta['uuid'] not in input_uuids:
                            logging.info("Downloading: %s" % (meta['uuid']))
                            service.store_data(a, doc)
                        else:
                            logging.info("Skipping input file %s" % (a))
                    else:
                        logging.info("Skipping non-ok file: %s" % (meta['state']))
                else:
                    logging.info("Skipping Download %s (not visible)" % (a))

    logging.info("Done")
    if not args['hold']:
        service.stop()


if __name__ == "__main__":
    parser = ArgumentParser()
    #this block of command should be the same as the warpdrive program
    parser.add_argument("-g",  "--galaxy", help="Galaxy Runner Image", default="bgruening/galaxy-stable:dev")
    parser.add_argument("-t",  "--tool-dir", help="Tool Directory", required=True)
    parser.add_argument("-ti", "--tool-images", default=None)
    parser.add_argument("-td", "--tool-data", help="Tool Directory", default=None)
    parser.add_argument("-l",  "--lib-data", help="Data directory (metadata as .json files)", required=True)
    parser.add_argument("-c", "--config", default=None)
    parser.add_argument("--cpus", type=int, default=None)
    parser.add_argument("--config-dir", help="Directory For Warpdrive config mounting", default=None)
    parser.add_argument("--smp", action="append", nargs=2, default=[])
    parser.add_argument("--sudo", action="store_true", default=False)

    parser.add_argument("-b", "--doc-store", default="./nebula_data")
    parser.add_argument("-w", "--workflow", help="Galaxy Workflow File")
    parser.add_argument("-y", "--yaml-workflow", help="Galaxy YAML Workflow File")
    parser.add_argument("--hold", action="store_true", default=False)
    parser.add_argument("inputs", nargs="+", default=[])

    args = parser.parse_args()
    if args.yaml_workflow is None and args.workflow is None:
        sys.stderr.write("Must define workflow with -w or -y flags")
        sys.exit(1)
    vargs = vars(args)
    if args.config is not None:
        with open(args.config) as handle:
            txt = handle.read()
        nargs = yaml.load(txt)
        for k,v in nargs.items():
            vargs[k] = v

    run_workflow(vargs)
