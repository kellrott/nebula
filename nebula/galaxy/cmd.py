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
import logging
import subprocess
import traceback
import tempfile
import shutil
from uuid import uuid4

import nebula.docstore
from nebula import Target
from nebula.galaxy import GalaxyWorkflow,\
GalaxyWorkflowTask, GalaxyEngine, GalaxyResources
from nebula import warpdrive

def action_run(
    task, docstore,
    galaxy_start, galaxy_stop, tool_dir=None, hold=False,
    tool_tar=None, hold_error=False, work_volume=None):
    """

    """
    try:

        with open(task) as handle:
            task_doc = json.loads(handle.read())
        if 'engine' in task_doc and 'config' in task_doc['engine'] and 'hold' in task_doc['engine']['config']:
            hold = task_doc['engine']['config']['hold']
        child_network = task_doc['engine']['config'].get("child_network", "bridge")
        work_volume = task_doc['engine']['config'].get("work_volume", None)
        ds = nebula.docstore.from_url(docstore, cache_path="/export/datastore")
        print json.dumps(task_doc, indent=4)
        for i in task_doc['engine']['resources']['images']:
            print "loading image %s" % (i)
            image_file = ds.get_filename( Target(i['id']) )
            warpdrive.call_docker_load(image_file)
        if not os.path.exists("/export/tools"):
            os.mkdir("/export/tools")
        for i in task_doc['engine']['resources']['tools']:
            tool_file = ds.get_filename(Target(i['id']))
            subprocess.check_call(["/bin/tar", "xzf", tool_file], cwd="/export/tools")
        env = dict(os.environ)
        warpdrive.config_tool_dir("/export/tools", env)
        smp = {}
        docker_volumes_from = None
        docker_volumes = None
        
        #need to mount /export in somehow
        if work_volume is None:
            docker_volumes_from = os.environ['HOSTNAME']
        else:
            docker_volumes = "%s:/export" % (work_volume)
        warpdrive.config_jobs(
            smp=smp, env=env,
            docker_volumes_from=docker_volumes_from,
            docker_volumes=docker_volumes,
            job_conf_file="/etc/galaxy/jobs.xml", 
            network=child_network,
            default_container=task_doc['engine']['config']['galaxy'], 
            plugin="local", handler="main")
        print "Starting Galaxy"
        subprocess.check_call(galaxy_start, shell=True, env=env)
        
        #create an instance of the galaxy engine wrapper that links
        #to the galaxy instance we just started
        engine = GalaxyEngine(
            docstore=ds,
            launch_docker=False,
            resources=GalaxyResources(),
            url="http://localhost:8080",
            api_key=os.environ['GALAXY_DEFAULT_ADMIN_KEY'],
            #common_dirs=common_dirs
        )
        e = run_workflow(
            task=task, engine=engine,
            hold=hold)
        ds = None
    except Exception, e:
        traceback.print_exc()
        sys.stderr.write("%s\n" % (e.message))
        e = 1
        if  hold or hold_error:
            while True:
                time.sleep(10)

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
    
def run_workflow(task, engine, hold=False, hold_error=False):
            
    task_doc = {}
    if task is not None:
        with open(task) as handle:
            task_doc = json.loads(handle.read())
    
    task = GalaxyWorkflowTask.from_dict(task_doc, engine=engine)
    error = 0
    try:
        logging.info("Starting Service")
        engine.start()
        logging.info("Starting Task")
        job = engine.submit(task)
        engine.wait([job])

        if job.get_status() not in ['ok']:
            sys.stderr.write("job: %s " % job.get_status() )
            sys.stderr.write("---ERROR---\n")
            if job.error_msg is not None:
                sys.stderr.write( job.error_msg.encode('utf-8') + "\n")
            sys.stderr.write("---ERROR---\n")
            error = 1
    finally:
        logging.info("Done")
        if not hold and (not hold_error or not error):
            logging.info("Stopping Galaxy Service")
            engine.stop()
        else:
            while True:
                time.sleep(10)
    #ds.close()
    return error

def action_pack(tooldir, docstore, host=None, sudo=False, no_cache=False):
    image_dir = tempfile.mkdtemp(dir="./", prefix="nebula_pack_")
    if not os.path.exists(image_dir):
        os.mkdir(image_dir)

    images = []
    tools = []
    ds = nebula.docstore.from_url(docstore)
    for tool_id, tool_conf, docker_tag in warpdrive.tool_dir_scan(tooldir):
        print tool_id, tool_conf, docker_tag

        dockerfile = os.path.join(os.path.dirname(tool_conf), "Dockerfile")
        if os.path.exists(dockerfile):
            warpdrive.call_docker_build(
                host=host,
                sudo=sudo,
                no_cache=no_cache,
                tag=docker_tag,
                dir=os.path.dirname(tool_conf)
            )
                
        image_file = os.path.join(image_dir, "docker_" + docker_tag.split(":")[0].replace("/", "_") + ".tar")
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
        images.append({
            "id" : t.id,
            'name' : docker_tag,
            "type" : "docker_image"
        })
        archive_dir = os.path.dirname(tool_conf)        
        archive_name = os.path.basename(os.path.dirname(tool_conf))        
        archive_tar = os.path.join(image_dir, "%s.tar.gz" % (archive_name))
        pack_cmd = "tar -C %s -cvzf %s %s" % (
                                              os.path.dirname(archive_dir),
                                              archive_tar, archive_name)
        print "Calling", pack_cmd
        subprocess.check_call(pack_cmd, shell=True)
                
        target = Target(str(uuid4()))
        ds.update_from_file(t, archive_tar, create=True)
        ds.put(target.id, {
            'name' : archive_name,
            'type' : "galaxy_tool_archive"
        })
        tools.append({
            "id" : target.id,
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
    parser_run.add_argument("--work-volume", default=None)
    parser_run.add_argument("--tool-tar", action="append", default=[])
    parser_run.add_argument("--galaxy-start", default="startup_lite -j")
    parser_run.add_argument("--galaxy-stop", default="galaxy_shutdown")
    parser_run.add_argument("--tool-dir", default=None)
    parser_run.add_argument("task")
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

