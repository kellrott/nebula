#!/usr/bin/env python

import re
import sys
import os
import time
import urlparse
import argparse
import subprocess
import logging
import requests
import tempfile
import string
import json
import shutil

try:
    import yaml
except ImportError:
    yaml = None

from xml.dom.minidom import parse as parseXML
from glob import glob
from socket import gethostname

if 'WARPDRIVE_CONFIG_DIR' in os.environ:
    DEFAULT_CONFIG=os.path.join(os.path.abspath(os.environ["WARPDRIVE_CONFIG_DIR"]), gethostname())
else:
    DEFAULT_CONFIG=os.path.join(os.environ["HOME"], ".warpdrive", gethostname())
if not os.path.exists(DEFAULT_CONFIG):
    os.makedirs(DEFAULT_CONFIG)

class RequestException(Exception):
    def __init__(self, message):
        self.message = message

def which(file):
    for path in os.environ["PATH"].split(":"):
        p = os.path.join(path, file)
        if os.path.exists(p):
            return p

def get_docker_path():
    docker_path = which('docker')
    if docker_path is None:
        raise Exception("Cannot find docker")
    return docker_path

def call_docker_run(
    galaxy, ports={},
    args=[], host=None, sudo=False,
    env={},
    set_user=False,
    mounts={},
    privledged=False,
    name=None):

    docker_path = get_docker_path()

    cmd = [
        docker_path, "run"
    ]

    if set_user:
        cmd.extend( ["-u", str(os.geteuid())] )
    for k, v in ports.items():
        cmd.extend( ["-p", "%s:%s" % (k,v) ] )
    for k, v in env.items():
        cmd.extend( ["-e", "%s=%s" % (k,v)] )
    if name is not None:
        cmd.extend( ["--name", name])
    for k, v in mounts.items():
        cmd.extend( ["-v", "%s:%s" % (k, v)])
    if privledged:
        cmd.append("--privileged")
    cmd.append("-d")
    cmd.extend( [galaxy] )
    cmd.extend(args)

    sys_env = dict(os.environ)
    if host is not None:
        sys_env['DOCKER_HOST'] = host
    if sudo:
        cmd = ['sudo'] + cmd
    logging.info("executing: " + " ".join(cmd))
    proc = subprocess.Popen(cmd, close_fds=True, env=sys_env, stdout=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    if proc.returncode != 0:
        raise Exception("Call Failed: %s" % (cmd))


def call_docker_attach(
    host=None, sudo=False,
    name=None):

    docker_path = get_docker_path()
    cmd = [
        docker_path, "attach", name
    ]
    sys_env = dict(os.environ)
    if host is not None:
        sys_env['DOCKER_HOST'] = host
    if sudo:
        cmd = ['sudo'] + cmd
    logging.info("executing: " + " ".join(cmd))
    proc = subprocess.Popen(cmd, close_fds=True, env=sys_env)
    stdout, stderr = proc.communicate()
    if proc.returncode != 0:
        raise Exception("Call Failed: %s" % (cmd))


def call_docker_copy(
    src,
    dst,
    host = None,
    sudo = False):

    docker_path = get_docker_path()

    cmd = [
        docker_path, "cp", src, dst
    ]
    sys_env = dict(os.environ)
    if host is not None:
        sys_env['DOCKER_HOST'] = host
    if sudo:
        cmd = ['sudo'] + cmd
    logging.info("executing: " + " ".join(cmd))
    subprocess.check_call(cmd, close_fds=True, env=sys_env, stdout=subprocess.PIPE)


def call_docker_kill(
    name,
    host=None, sudo=False
    ):

    docker_path = get_docker_path()

    cmd = [
        docker_path, "kill", name
    ]
    sys_env = dict(os.environ)
    if host is not None:
        sys_env['DOCKER_HOST'] = host
    if sudo:
        cmd = ['sudo'] + cmd
    logging.info("executing: " + " ".join(cmd))
    subprocess.check_call(cmd, close_fds=True, env=sys_env, stdout=subprocess.PIPE)

def call_docker_rm(
    name=None, volume_delete=False,
    host=None, sudo=False
    ):

    docker_path = get_docker_path()

    cmd = [
        docker_path, "rm"
    ]
    if volume_delete:
        cmd.append("-v")
    cmd.append(name)

    sys_env = dict(os.environ)
    if host is not None:
        sys_env['DOCKER_HOST'] = host
    if sudo:
        cmd = ['sudo'] + cmd
    logging.info("executing: " + " ".join(cmd))
    proc = subprocess.Popen(cmd, close_fds=True, env=sys_env, stdout=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    if proc.returncode != 0:
        raise Exception("Call Failed: %s" % (cmd))



def call_docker_ps(
    host=None, sudo=False
    ):

    docker_path = get_docker_path()

    cmd = [
        docker_path, "ps", "-a", "--no-trunc", "-s"
    ]

    sys_env = dict(os.environ)
    if host is not None:
        sys_env['DOCKER_HOST'] = host
    if sudo:
        cmd = ['sudo'] + cmd
    logging.info("executing: " + " ".join(cmd))
    proc = subprocess.Popen(cmd, close_fds=True, env=sys_env, stdout=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    if proc.returncode != 0:
        raise Exception("Call Failed: %s" % (cmd))
    return stdout


def call_docker_build(
    dir,
    host = None,
    sudo = False,
    no_cache=False,
    tag=None
    ):

    docker_path = get_docker_path()

    cmd = [
        docker_path, "build"
    ]
    if no_cache:
        cmd.append("--no-cache")
    if tag is not None:
        cmd.extend( ['-t', tag] )
    cmd.append(dir)

    sys_env = dict(os.environ)
    if host is not None:
        sys_env['DOCKER_HOST'] = host
    if sudo:
        cmd = ['sudo'] + cmd
    logging.info("executing: " + " ".join(cmd))
    proc = subprocess.Popen(cmd, close_fds=True, env=sys_env)
    stdout, stderr = proc.communicate()
    if proc.returncode != 0:
        raise Exception("Call Failed: %s" % (cmd))

def call_docker_save(
    tag,
    output,
    host=None,
    sudo=False,
    ):


    docker_path = get_docker_path()

    cmd = [
        docker_path, "save", "-o", output, tag
    ]
    sys_env = dict(os.environ)
    if host is not None:
        sys_env['DOCKER_HOST'] = host
    if sudo:
        cmd = ['sudo'] + cmd
    logging.info("executing: " + " ".join(cmd))
    proc = subprocess.Popen(cmd, close_fds=True, env=sys_env)
    stdout, stderr = proc.communicate()
    if proc.returncode != 0:
        raise Exception("Call Failed: %s" % (cmd))

def scan_directory(lpath, metadata_suffix=None):
    data_load = []
    meta_data = {}
    for a in glob(os.path.join(lpath, "*")):
        if metadata_suffix is None or not a.endswith(metadata_suffix):
            if os.path.isfile(a):
                data_load.append( a )
            elif os.path.isdir(a):
                d, m = scan_directory(a, metadata_suffix)
                for i in d:
                    data_load.append(i)
                for k,v in m.items():
                    meta_data[k] = v
        elif metadata_suffix is not None:
            file = a[:-len(metadata_suffix)]
            if os.path.exists(file):
                try:
                    with open(a) as handle:
                        txt = handle.read()
                        md = json.loads(txt)
                        meta_data[ os.path.join(dpath, os.path.relpath(file, lpath) ) ] = md
                        logging.debug("Found metadata for %s " % (file))
                except:
                    pass
    return data_load, meta_data


def run_up(name="galaxy", galaxy="bgruening/galaxy-stable", port=8080, host=None,
    sudo=False, lib_data=[], auto_add=False, tool_data=None, metadata_suffix=None,
    tool_dir=None, config_dir=DEFAULT_CONFIG, work_dir=None, tool_docker=False, force=False,
    tool_images=None, smp=[], cpus=None, timeout=60,
    hold=False, key="HSNiugRFvgT574F43jZ7N9F3"):

    if config_dir is None:
        config_dir = DEFAULT_CONFIG

    if force and run_status(name=name,host=host, sudo=sudo):
        run_down(name=name, host=host, rm=True, config_dir=config_dir, sudo=sudo)

    env = {
        "GALAXY_CONFIG_CHECK_MIGRATE_TOOLS" : "False",
        "GALAXY_CONFIG_MASTER_API_KEY" : key
    }

    mounts = {}
    privledged = False

    if tool_data is not None:
        mounts[os.path.abspath(tool_data)] = "/tool_data"
        env['GALAXY_CONFIG_TOOL_DATA_PATH'] = "/tool_data"

    if work_dir is not None:
        if not os.path.exists(work_dir):
            os.mkdir(work_dir)
            os.chmod(work_dir, 0777)
        files_path = os.path.join(os.path.abspath(work_dir), "files")
        job_working_dir = os.path.join(os.path.abspath(work_dir), "job_working_directory")
        if not os.path.exists(files_path):
            os.mkdir(files_path)
            os.chmod(files_path, 0777)
        if not os.path.exists(job_working_dir):
            os.mkdir(job_working_dir)
            os.chmod(job_working_dir, 0777)
        env['GALAXY_CONFIG_FILE_PATH'] = "/parent/database_files"
        mounts[files_path] = "/parent/database_files"
        env['GALAXY_CONFIG_JOB_WORKING_DIRECTORY'] = "/parent/job_working_directory"
        mounts[job_working_dir] = "/parent/job_working_directory"

    config_dir = os.path.abspath(os.path.join(config_dir, "warpdrive_%s" % (name)))
    if not os.path.exists(config_dir):
        os.mkdir(config_dir)

    if tool_dir is not None:
        mounts[os.path.abspath(tool_dir)] = "/tools_import"
        mounts[config_dir] = "/config"
        with open( os.path.join(config_dir, "import_tool_conf.xml"), "w" ) as handle:
            handle.write(TOOL_IMPORT_CONF)
        env['GALAXY_CONFIG_TOOL_CONFIG_FILE'] = "/config/import_tool_conf.xml,config/tool_conf.xml.main"

    if tool_images is not None:
        mounts[os.path.abspath(tool_images)] = "/images"


    data_load = []
    meta_data = {}
    lib_mapping = {}
    for i, ld in enumerate(lib_data):
        env['GALAXY_CONFIG_ALLOW_LIBRARY_PATH_PASTE'] = "True"
        lpath = os.path.abspath(ld)
        dpath = "/parent/lib_data_%s" % (i)
        mounts[lpath] = dpath
        lib_mapping[lpath] = dpath
        if auto_add:
            d, m = scan_directory(lpath, metadata_suffix)
            for i in d:
                data_load.append(i)
            for k,v in m:
                meta_data[k] = v

    if tool_docker:
        common_volumes = ",".join( "%s:%s:ro" % (k,v) for k,v in lib_mapping.items() )

        #for every different count of SMPs, create a different destination
        smp_destinations = []
        for count in set( a[1] for a in smp ):
            smp_destinations.append( string.Template(SMP_DEST_CONF).substitute(
                DEST_NAME="docker_cluster_smp%s" % (count),
                TAG=galaxy,
                NAME=name,
                NCPUS=count,
                COMMON_VOLUMES=common_volumes)
            )

        smp_tools = []
        for tool, count in smp:
            smp_tools.append( string.Template(SMP_TOOL_CONF).substitute(
                    DEST_NAME="docker_cluster_smp%s" % (count),
                    TOOL_ID=tool
                )
            )

        mounts[config_dir] = "/config"
        job_conf = string.Template(JOB_CHILD_CONF).substitute(
            TAG=galaxy,
            NAME=name,
            COMMON_VOLUMES=common_volumes,
            SMP_DESTINATIONS="\n".join(smp_destinations),
            SMP_TOOLS="\n".join(smp_tools)
        )
        with open( os.path.join(config_dir, "job_conf.xml"), "w" ) as handle:
            handle.write(job_conf)
        env["GALAXY_CONFIG_JOB_CONFIG_FILE"] = "/config/job_conf.xml"
        #env['GALAXY_CONFIG_OUTPUTS_TO_WORKING_DIRECTORY'] = "True"
        env['DOCKER_PARENT'] = "True"
        privledged=True
        mounts['/var/run/docker.sock'] = '/var/run/docker.sock'

    if cpus is not None:
        env['SLURM_CPUS'] = cpus

    call_docker_run(
        galaxy,
        ports={str(port) : "80"},
        host=host,
        sudo=sudo,
        name=name,
        mounts=mounts,
        privledged=privledged,
        env=env
    )

    web_host="localhost"
    if 'DOCKER_HOST' in os.environ:
        u = urlparse.urlparse(os.environ['DOCKER_HOST'])
        web_host = u.netloc.split(":")[0]

    timeout_time = time.time() + timeout
    while True:
        if time.time() > timeout_time:
            raise Exception("Startup Timed out")
        time.sleep(3)
        try:
            url = "http://%s:%s/api/tools?key=%s" % (web_host, port, key)
            logging.debug("Pinging: %s" % (url))
            res = requests.get(url, timeout=3)
            if res.status_code / 100 == 5:
                continue
            if res.status_code in [404, 403]:
                continue
            break
        except requests.exceptions.ConnectionError:
            pass
        except requests.exceptions.Timeout:
            pass

    rg = RemoteGalaxy("http://%s:%s"  % (web_host, port), 'admin', path_mapping=lib_mapping)
    library_id = rg.create_library("Imported")
    folder_id = rg.library_find_contents(library_id, "/")['id']
    for data in data_load:
        logging.info("Loading: %s" % (data))
        md = {}
        if data in meta_data:
            md = meta_data[data]
        rg.library_paste_file(library_id, folder_id, os.path.basename(data), data, uuid=md.get('uuid', None))

    with open(os.path.join(config_dir, "config.json"), "w") as handle:
        handle.write(json.dumps({
            'galaxy' : galaxy,
            'port' : port,
            'lib_data' : list(os.path.abspath(a) for a in lib_data),
            'host' : web_host,
            'tool_dir' : os.path.abspath(tool_dir) if tool_dir is not None else None,
            'tool_data' : os.path.abspath(tool_data) if tool_data is not None else None,
            'metadata_suffix' : metadata_suffix,
            'tool_docker' : tool_docker,
            'key' : key,
            'lib_mapping' : lib_mapping
        }))

    if hold:
        call_docker_attach(
            host=host,
            sudo=sudo,
            name=name
        )

    return rg

class RemoteGalaxy(object):

    def __init__(self, url, api_key, path_mapping={}):
        self.url = url
        self.api_key = api_key
        self.path_mapping = path_mapping

    def get(self, path, params = {}):
        c_url = self.url + path
        params['key'] = self.api_key
        req = requests.get(c_url, params=params)
        return req.json()

    def post(self, path, payload, params={}):
        c_url = self.url + path
        params['key'] = self.api_key
        logging.debug("POSTING: %s %s" % (c_url, json.dumps(payload)))
        req = requests.post(c_url, data=json.dumps(payload), params=params, headers = {'Content-Type': 'application/json'} )
        print req.text
        return req.json()

    def post_text(self, path, payload, params=None):
        c_url = self.url + path
        if params is None:
            params = {}
        params['key'] = self.api_key
        logging.debug("POSTING: %s %s" % (c_url, json.dumps(payload)))
        req = requests.post(c_url, data=json.dumps(payload), params=params, headers = {'Content-Type': 'application/json'} )
        return req.text

    def download_handle(self, path):
        url = self.url + path
        logging.info("Downloading: %s" % (url))
        params = {}
        params['key'] = self.api_key
        r = requests.get(url, params=params, stream=True)
        return r

    def download(self, path, dst):
        r = self.download_handle(path)
        dsize = 0L
        if hasattr(dst, 'write'):
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    dst.write(chunk)
                    dsize += len(chunk)
        else:
            with open(dst, "wb") as handle:
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:
                        handle.write(chunk)
                        handle.flush()
                        dsize += len(chunk)
        logging.info("Downloaded: %s bytes" % (dsize))

    def create_library(self, name):
        lib_create_data = {'name' : name}
        library = self.post('/api/libraries', lib_create_data)
        library_id = library['id']
        return library_id

    def library_find(self, name):
        for d in self.library_list():
            if d['name'] == name:
                return d
        return None

    def library_list(self):
        return self.get("/api/libraries")

    def library_list_contents(self, library_id):
        return self.get("/api/libraries/%s/contents" % library_id)

    def library_find_contents(self, library_id, name):
        for a in self.library_list_contents(library_id):
            if a['name'] == name:
                return a
        return None

    def library_get_contents(self, library_id, ldda_id):
        return self.get("/api/libraries/%s/contents/%s" % (library_id, ldda_id))

    def get_hda(self, history, hda):
        return self.get("/api/histories/%s/contents/%s" % (history, hda))

    def get_dataset(self, id, src='hda' ):
        return self.get("/api/datasets/%s?hda_ldda=%s" % (id, src))

    def download_hda(self, history, hda, dst):
        meta = self.get_hda(history, hda)
        self.download(meta['download_url'], dst)

    def history_list(self):
        return self.get("/api/histories")

    def get_history(self, history):
        return self.get("/api/histories/%s" % (history))

    def get_provenance(self, history, hda, follow=False):
        if follow:
            return self.get("/api/histories/%s/contents/%s/provenance" % (history, hda), {"follow" : True})
        else:
            return self.get("/api/histories/%s/contents/%s/provenance" % (history, hda))

    def add_workflow(self, wf):
        self.post("/api/workflows/upload", { 'workflow' : wf } )

    def get_workflow(self, wid):
        return self.get("/api/workflows/%s" % (wid))

    def call_workflow(self, request):
        return self.post("/api/workflows", request, params={'step_details' : True} )

    def get_job(self, jid):
        return self.get("/api/jobs/%s" % (jid), {'full' : True} )

    def library_paste_file(self, library_id, library_folder_id, name, datapath, uuid=None, metadata=None):
        datapath = os.path.abspath(datapath)
        found = False
        for ppath, dpath in self.path_mapping.items():
            if datapath.startswith(ppath):
                datapath = os.path.join(dpath, os.path.relpath(datapath, ppath))
                found = True
                break
        if not found:
            raise Exception("Path not in mounted lib_data directories: %s" % (datapath))
        data = {}
        data['folder_id'] = library_folder_id
        data['file_type'] = 'auto'
        data['name'] = name
        if uuid is not None:
            data['uuid'] = uuid
        data['dbkey'] = ''
        data['upload_option'] = 'upload_paths'
        data['create_type'] = 'file'
        data['link_data_only'] = 'link_to_files'
        if metadata is not None:
            data['extended_metadata'] = metadata
        data['filesystem_paths'] = datapath
        libset = self.post("/api/libraries/%s/contents" % library_id, data)
        print libset
        return libset[0]



def run_down(name="galaxy", host=None, rm=False, config_dir=DEFAULT_CONFIG, sudo=False):
    if config_dir is None:
        config_dir = DEFAULT_CONFIG
    config_dir = os.path.join(config_dir, "warpdrive_%s" % (name))
    try:
        call_docker_kill(
            name, host=host, sudo=sudo
        )
    except subprocess.CalledProcessError:
        pass
    if rm:
        call_docker_rm(
            name, host=host, sudo=sudo, volume_delete=True
        )
        if os.path.exists(config_dir):
            shutil.rmtree(config_dir)


def run_status(name="galaxy", host=None, sudo=False):
    txt = call_docker_ps(
        host=host, sudo=sudo
    )

    lines = txt.split("\n")

    containerIndex = lines[0].index("CONTAINER ID")
    imageIndex = lines[0].index("IMAGE")
    commandIndex = lines[0].index("COMMAND")
    portsIndex = lines[0].index("PORTS")
    statusIndex = lines[0].index("STATUS")
    namesIndex = lines[0].index("NAMES")
    sizeIndex = lines[0].index("SIZE")

    found = False
    for line in lines[1:]:
        if len(line):
            cur_name = line[namesIndex:sizeIndex].split()[0]
            tmp = line[statusIndex:portsIndex].split()
            status = "NotFound"
            if len(tmp):
                status = tmp[0]
            if cur_name == name:
                print status
                found = True
    if not found:
        print "NotFound"
    return found


def run_add(name="galaxy", config_dir=DEFAULT_CONFIG, files=[]):
    if config_dir is None:
        config_dir = DEFAULT_CONFIG
    config_dir = os.path.join(config_dir, "warpdrive_%s" % (name))
    if not os.path.exists(config_dir):
        print "Config not found"
        return

    with open(os.path.join(config_dir, "config.json")) as handle:
        txt = handle.read()
        config = json.loads(txt)

    rg = RemoteGalaxy("http://%s:%s" % (config['host'], config['port']), 'admin', path_mapping=config['lib_mapping'])
    library_id = rg.library_find("Imported")['id']
    folder_id = rg.library_find_contents(library_id, "/")['id']

    for d in data_load:
        md = {}
        if config['metadata_suffix'] is not None:
            if os.path.exists(d + config['metadata_suffix']):
                with open(d+config['metadata_suffix']) as handle:
                    txt = handle.read()
                    md = json.loads(txt)
        logging.info("Adding %s" % (d))
        rg.library_paste_file(library_id, folder_id, os.path.basename(name), d, uuid=md.get('uuid', None))


def run_copy(name="galaxy", src=None, dst=None, host=None, sudo=False):
    if src is None or dst is None:
        return
    call_docker_copy(
        host = host,
        sudo = sudo,
        src= "%s:%s" % (name, src),
        dst=dst
    )


"""
Code for dealing with XML
"""

def getText(nodelist):
    rc = []
    for node in nodelist:
        if node.nodeType == node.TEXT_NODE:
            rc.append(node.data)
    return ''.join(rc)


def dom_scan(node, query):
    stack = query.split("/")
    if node.localName == stack[0]:
        return dom_scan_iter(node, stack[1:], [stack[0]])

def dom_scan_iter(node, stack, prefix):
    if len(stack):
        for child in node.childNodes:
            if child.nodeType == child.ELEMENT_NODE:
                if child.localName == stack[0]:
                    for out in dom_scan_iter(child, stack[1:], prefix + [stack[0]]):
                        yield out
                elif '*' == stack[0]:
                    for out in dom_scan_iter(child, stack[1:], prefix + [child.localName]):
                        yield out
    else:
        if node.nodeType == node.ELEMENT_NODE:
            yield node, prefix, dict(node.attributes.items()), getText( node.childNodes )
        elif node.nodeType == node.TEXT_NODE:
            yield node, prefix, None, getText( node.childNodes )


def run_build(tool_dir, host=None, sudo=False, tool=None, no_cache=False, image_dir=None):
    for tool_conf in glob(os.path.join(tool_dir, "*", "*.xml")):
        logging.info("Scanning: " + tool_conf)
        dom = parseXML(tool_conf)
        s = dom_scan(dom.childNodes[0], "tool")
        if s is not None:
            if tool is None or list(s)[0][2]['id'] in tool:
                scan = dom_scan(dom.childNodes[0], "tool/requirements/container")
                if scan is not None:
                    for node, prefix, attrs, text in scan:
                        if 'type' in attrs and attrs['type'] == 'docker':
                            tag = text
                            dockerfile = os.path.join(os.path.dirname(tool_conf), "Dockerfile")
                            if os.path.exists(dockerfile):
                                call_docker_build(
                                    host = host,
                                    sudo = sudo,
                                    no_cache=no_cache,
                                    tag=tag,
                                    dir=os.path.dirname(tool_conf)
                                )

                                if image_dir is not None:
                                    if not os.path.exists(image_dir):
                                        os.mkdir(image_dir)
                                    image_file = os.path.join(image_dir, "docker_" + tag.split(":")[0] + ".tar")
                                    call_docker_save(
                                        host=host,
                                        sudo=sudo,
                                        tag=tag,
                                        output=image_file
                                    )



TOOL_IMPORT_CONF = """<?xml version='1.0' encoding='utf-8'?>
<toolbox>
  <section id="imported" name="Imported Tools">
    <tool_dir dir="/tools_import"/>
  </section>
</toolbox>"""


SMP_DEST_CONF = """<destination id="${DEST_NAME}" runner="slurm">
            <param id="docker_enabled">true</param>
            <param id="docker_sudo">true</param>
            <param id="docker_net">bridge</param>
            <param id="docker_default_container_id">${TAG}</param>
            <param id="docker_volumes">${COMMON_VOLUMES}</param>
            <param id="docker_volumes_from">${NAME}</param>
            <param id="docker_container_image_cache_path">/images</param>
            <param id="nativeSpecification">--ntasks=${NCPUS}</param>
        </destination>"""

SMP_TOOL_CONF = """<tool id="${TOOL_ID}" handler="handlers" destination="${DEST_NAME}"></tool>"""

JOB_CHILD_CONF = """<?xml version="1.0"?>
<job_conf>
    <plugins workers="2">
        <plugin id="slurm" type="runner" load="galaxy.jobs.runners.slurm:SlurmJobRunner">
            <param id="drmaa_library_path">/usr/lib/slurm-drmaa/lib/libdrmaa.so</param>
        </plugin>
    </plugins>
    <handlers default="handlers">
        <handler id="handler0" tags="handlers"/>
        <handler id="handler1" tags="handlers"/>
    </handlers>
    <destinations default="cluster_docker">
        <destination id="cluster_docker" runner="slurm">
            <param id="docker_enabled">true</param>
            <param id="docker_sudo">true</param>
            <param id="docker_net">bridge</param>
            <param id="docker_default_container_id">${TAG}</param>
            <param id="docker_volumes">${COMMON_VOLUMES}</param>
            <param id="docker_volumes_from">${NAME}</param>
            <param id="docker_container_image_cache_path">/images</param>
        </destination>
        ${SMP_DESTINATIONS}
        <destination id="cluster" runner="slurm">
        </destination>
    </destinations>
    <tools>
        <tool id="upload1" handler="handlers" destination="cluster"></tool>
        ${SMP_TOOLS}
    </tools>
</job_conf>
"""


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(title="subcommand")

    parser_up = subparsers.add_parser('up')
    parser_up.add_argument("-g", "--galaxy", dest="galaxy", default="bgruening/galaxy-stable:dev")
    parser_up.add_argument("-t", "--tool-dir", default=None)
    parser_up.add_argument("-ti", "--tool-images", default=None)
    parser_up.add_argument("-td", "--tool-data", default=None)
    parser_up.add_argument("-l", "--lib-data", action="append", default=[])
    parser_up.add_argument("-c", "--config", default=None)
    parser_up.add_argument("--config-dir", default=DEFAULT_CONFIG)
    parser_up.add_argument("--work-dir", default=None)
    parser_up.add_argument("--smp", action="append", nargs=2, default=[])
    parser_up.add_argument("--cpus", type=int, default=None)
    parser_up.add_argument("-d", "--docker", dest="tool_docker", action="store_true", help="Launch jobs in child containers", default=False)

    #parser_up.add_argument("-s", "--file-store", default=None)
    parser_up.add_argument("-v", action="store_true", default=False)
    parser_up.add_argument("-vv", action="store_true", default=False)
    parser_up.add_argument("-f", "--force", action="store_true", default=False)
    parser_up.add_argument("-p", "--port", default="8080")
    parser_up.add_argument("-m", "--metadata", dest="metadata_suffix", default=None)
    parser_up.add_argument("-n", "--name", default="galaxy")
    parser_up.add_argument("-a", "--auto-add", action="store_true", default=False)
    parser_up.add_argument("--key", default="HSNiugRFvgT574F43jZ7N9F3")
    parser_up.add_argument("--host", default=None)
    parser_up.add_argument("--sudo", action="store_true", default=False)
    parser_up.add_argument("--hold", action="store_true", default=False)

    parser_up.set_defaults(func=run_up)

    parser_down = subparsers.add_parser('down')
    parser_down.add_argument("-n", "--name", default="galaxy")
    parser_down.add_argument("--rm", action="store_true", default=False)
    parser_down.add_argument("--host", default=None)
    parser_down.add_argument("--config-dir", default=DEFAULT_CONFIG)
    parser_down.add_argument("--sudo", action="store_true", default=False)
    parser_down.add_argument("-v", action="store_true", default=False)
    parser_down.add_argument("-vv", action="store_true", default=False)
    parser_down.set_defaults(func=run_down)

    parser_status = subparsers.add_parser('status')
    parser_status.add_argument("-n", "--name", default="galaxy")
    parser_status.add_argument("--host", default=None)
    parser_status.add_argument("--sudo", action="store_true", default=False)
    parser_status.add_argument("-v", action="store_true", default=False)
    parser_status.add_argument("-vv", action="store_true", default=False)
    parser_status.set_defaults(func=run_status)

    parser_add = subparsers.add_parser('add')
    parser_add.add_argument("-n", "--name", default="galaxy")
    parser_add.add_argument("--config-dir", default=DEFAULT_CONFIG)
    parser_add.add_argument("-v", action="store_true", default=False)
    parser_add.add_argument("-vv", action="store_true", default=False)
    parser_add.add_argument("files", nargs="+")
    parser_add.set_defaults(func=run_add)

    parser_build = subparsers.add_parser('build')
    parser_build.add_argument("--host", default=None)
    parser_build.add_argument("--sudo", action="store_true", default=False)
    parser_build.add_argument("--no-cache", action="store_true", default=False)
    parser_build.add_argument("-t", "--tool", action="append", default=None)
    parser_build.add_argument("-o", "--image-dir", default=None)
    parser_build.add_argument("-v", action="store_true", default=False)
    parser_build.add_argument("-vv", action="store_true", default=False)

    parser_build.add_argument("tool_dir")
    parser_build.set_defaults(func=run_build)

    args = parser.parse_args()

    if args.v:
        logging.basicConfig(level=logging.INFO)
    if args.vv:
        logging.basicConfig(level=logging.DEBUG)

    func = args.func
    kwds=vars(args)
    del kwds['v']
    del kwds['vv']
    del kwds['func']

    if 'config' in kwds:
        if kwds['config'] is not None:
            if yaml is None:
                raise Exception("Can't find YAML parsing library")
            with open(kwds['config'] ) as handle:
                txt = handle.read()
            nargs = yaml.load(txt)
            for k,v in nargs.items():
                kwds[k] = v
        del kwds['config']

    try:
        func(**kwds)
        sys.exit(0)
    except RequestException, e:
        sys.stderr.write("%s\n" % (e.message))
        sys.exit(1)
