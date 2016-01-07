# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import os
import sys
import time
import json
import argparse
import logging
import socket
import logging
import subprocess
import traceback
import tempfile
import shutil
from uuid import uuid4

import nebula.docstore
from nebula import Target, TaskGroup, TargetFile
from nebula.galaxy import GalaxyWorkflow, GalaxyService, GalaxyWorkflowTask
from nebula import warpdrive

def action_run(config, request, docstore,
    galaxy_start, galaxy_stop, tool_dir=None, hold=False,
    tool_tar=None, hold_error=False, workdir="outputs", inputs=[]):

    try:
        if tool_dir is not None:
            warpdrive.config_tool_dir(tool_dir, os.environ)
        warpdrive.config_jobs({}, os.environ, 
            parent_name=os.environ.get('HOSTNAME', None), 
            plugin="local",
            handler="main")
        
        config_doc = {}
        if config is not None:
            with open(config) as handle:
                config_doc = json.loads(handle.read())
        ds = nebula.docstore.from_url(docstore, file_path="/export/datastore")
        for i in config_doc['config']['resources']['images']:
            image_file = ds.get_filename( Target(i['id']) )
            warpdrive.call_docker_load(image_file)
        if not os.path.exists("/export/tools"):
            os.mkdir("/export/tools")
        for i in config_doc['config']['resources']['tools']:
            tool_file = ds.get_filename( Target(i['id']) )
            subprocess.check_call(["/bin/tar", "xzf", tool_file], cwd="/export/tools")
        ds = None
        env = dict(os.environ)
        warpdrive.config_tool_dir("/export/tools", env)
        smp = {}
        warpdrive.config_jobs(smp=smp, env=env, 
            parent_name=os.environ['HOSTNAME'], 
            job_conf_file="/etc/galaxy/jobs.xml", 
            default_container=config_doc['config']['galaxy'], plugin="local", handler="main")
        print "Starting Galaxy"
        subprocess.check_call(galaxy_start, shell=True, env=env)
        e = run_workflow(request=request, docstore=docstore, workdir=workdir)
    except Exception, e:
        traceback.print_exc()
        sys.stderr.write("%s\n" % (e.message))
        e = 1
    #subprocess.check_call(galaxy_stop, shell=True, env=env)
    logging.info("Exiting")
    time.sleep(5) # I don't know why this works, but it stops hanging
    """
    ##some code to scan the threads and figure out why things are hanging
    for threadId, stack in sys._current_frames().items():
        print("\n# ThreadID: %s" % threadId)
        for filename, lineno, name, line in traceback.extract_stack(stack):
            print('File: "%s", line %d, in %s' % (filename, lineno, name))
            if line:
                print("  %s" % (line.strip()))
    """
    #ds.close()
    
def run_workflow(request, docstore, workdir, hold=False, hold_error=False):
            
    request_doc = {}
    if request is not None:
        with open(request) as handle:
            request_doc = json.loads(handle.read())
    inputs = {}
    request_dirs = []

    ds = nebula.docstore.from_url(docstore, file_path="/export/datastore")
    request_dirs.append(os.path.abspath(ds.file_path))
    common_dirs = [ os.path.commonprefix( request_dirs ) ]

    inputs = request_doc['inputs']
    
    if request_doc is not None:
        task = GalaxyWorkflowTask.from_dict(request_doc)
    else:
        if 'workflow' in request_doc:
            workflow = GalaxyWorkflow(workflow=request_doc['workflow'])
        if args.workflow is not None:
            workflow = GalaxyWorkflow(ga_file=args.workflow)
        task = GalaxyWorkflowTask("workflow_test",
            workflow,
            inputs=inputs,
            parameters = request_doc.get('parameters', {})
        )
    
    service = GalaxyService(
        docstore=ds,
        url="http://localhost:8080",
        api_key=os.environ['GALAXY_DEFAULT_ADMIN_KEY'],
        common_dirs=common_dirs
    )

    error = 0
    try:
        logging.info("Starting Service")
        service.start()
        logging.info("Starting Task")
        job = service.submit(task)
        service.wait([job])

        if job.get_status() not in ['ok']:
            sys.stderr.write("---ERROR---\n")
            sys.stderr.write( job.error_msg.encode('utf-8') + "\n")
            sys.stderr.write("---ERROR---\n")
            error = 1
    finally:
        logging.info("Done")
        if not hold and (not hold_error or not error):
            logging.info("Stopping Galaxy Service")
            service.stop()
    #ds.close()
    
    return error

def action_pack(tooldir, docstore, host=None, sudo=False):
    image_dir = tempfile.mkdtemp(dir="./", prefix="nebula_pack_")
    if not os.path.exists(image_dir):
        os.mkdir(image_dir)

    images = []
    tools = []
    ds = nebula.docstore.from_url(docstore, file_path="/export/nebula_data")
    for tool_id, tool_conf, docker_tag in warpdrive.tool_dir_scan(tooldir):
        print tool_id, tool_conf, docker_tag

        dockerfile = os.path.join(os.path.dirname(tool_conf), "Dockerfile")
        if os.path.exists(dockerfile):
            warpdrive.call_docker_build(
                host = host,
                sudo = sudo,
                no_cache=no_cache,
                tag=docker_tag,
                dir=os.path.dirname(tool_conf)
            )
                
        image_file = os.path.join(image_dir, "docker_" + docker_tag.split(":")[0] + ".tar")
        warpdrive.call_docker_save(
            host=host,
            sudo=sudo,
            tag=docker_tag,
            output=image_file
        )
        t = Target(str(uuid4()))
        ds.update_from_file(t, image_file, create=True)
        ds.put(t.id, {
            'name' : docker_tag,
            "type" : "docker_image"
        })
        images.append( {
            "id" : t.id,
            'name' : docker_tag,
            "type" : "docker_image"
        } )
        archive_dir = os.path.dirname(tool_conf)        
        archive_name = os.path.basename(os.path.dirname(tool_conf))        
        archive_tar = os.path.join(image_dir, "%s.tar.gz" % (archive_name))
        pack_cmd = "tar -C %s -cvzf %s %s" % (os.path.dirname(archive_dir), archive_tar, archive_name)
        print "Calling", pack_cmd
        subprocess.check_call(pack_cmd, shell=True)
                
        t = Target(str(uuid4()))
        ds.update_from_file(t, archive_tar, create=True)
        ds.put(t.id, {
            'name' : archive_name,
            'type' : "galaxy_tool_archive"
        })
        tools.append({
            "id" : t.id,
            'name' : archive_name,
            'type' : "galaxy_tool_archive"
        })        
    shutil.rmtree(image_dir)
    return {"tools" : tools, "images" : images}

def add_nebula_run_commands(subparsers):
    parser_run = subparsers.add_parser('run')
    parser_run.add_argument("-v", action="store_true", default=False)
    parser_run.add_argument("-vv", action="store_true", default=False)
    parser_run.add_argument("--hold", action="store_true", default=False)
    parser_run.add_argument("--hold-error", action="store_true", default=False)
    parser_run.add_argument("--docstore", default=None)
    parser_run.add_argument("--workdir", default="output")
    parser_run.add_argument("--tool-tar", action="append", default=[])
    parser_run.add_argument("--galaxy-start", default="startup_lite -j")
    parser_run.add_argument("--galaxy-stop", default="galaxy_shutdown")
    parser_run.add_argument("--tool-dir", default=None)
    parser_run.add_argument("config")
    parser_run.add_argument("request")
    parser_run.set_defaults(func=action_run)

def add_nebula_build_commands(subparsers):
    parser_pack = subparsers.add_parser('pack')
    parser_pack.add_argument("-v", action="store_true", default=False)
    parser_pack.add_argument("-vv", action="store_true", default=False)
    parser_pack.add_argument("--host", default=None)
    parser_pack.add_argument("--sudo", action="store_true", default=False)
    parser_pack.add_argument("tooldir")
    parser_pack.add_argument("docstore")
    parser_pack.set_defaults(func=action_pack)

