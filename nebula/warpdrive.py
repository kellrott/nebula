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

from glob import glob

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
    docker_tag, ports={},
    args=[], host=None,
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
        cmd.extend( ['-v', '/var/run/docker.sock:/var/run/docker.sock'] )
    cmd.append("-d")
    cmd.extend( [docker_tag] )
    cmd.extend(args)

    sys_env = dict(os.environ)
    if host is not None:
        sys_env['DOCKER_HOST'] = host

    logging.info("executing: " + " ".join(cmd))
    proc = subprocess.Popen(cmd, close_fds=True, env=sys_env, stdout=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    if proc.returncode != 0:
        raise Exception("Call Failed: %s" % (cmd))


def call_docker_kill(
    name,
    host=None,
    ):

    docker_path = get_docker_path()

    cmd = [
        docker_path, "kill", name
    ]

    sys_env = dict(os.environ)
    if host is not None:
        sys_env['DOCKER_HOST'] = host

    logging.info("executing: " + " ".join(cmd))
    subprocess.check_call(cmd, close_fds=True, env=sys_env, stdout=subprocess.PIPE)

def call_docker_rm(
    name=None,
    host=None
    ):

    docker_path = get_docker_path()

    cmd = [
        docker_path, "rm", name
    ]

    sys_env = dict(os.environ)
    if host is not None:
        sys_env['DOCKER_HOST'] = host

    logging.info("executing: " + " ".join(cmd))
    proc = subprocess.Popen(cmd, close_fds=True, env=sys_env, stdout=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    if proc.returncode != 0:
        raise Exception("Call Failed: %s" % (cmd))



def call_docker_ps(
    host=None
    ):

    docker_path = get_docker_path()

    cmd = [
        docker_path, "ps", "-a", "--no-trunc", "-s"
    ]

    sys_env = dict(os.environ)
    if host is not None:
        sys_env['DOCKER_HOST'] = host

    logging.info("executing: " + " ".join(cmd))
    proc = subprocess.Popen(cmd, close_fds=True, env=sys_env, stdout=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    if proc.returncode != 0:
        raise Exception("Call Failed: %s" % (cmd))
    return stdout


def run_up(name="galaxy", docker_tag="bgruening/galaxy-stable", port=8080, host=None,
    lib_data=[], auto_add=False, tool_data=None, metadata_suffix=None, tool_dir=None,
    work_dir="/tmp", tool_docker=False,
    key="HSNiugRFvgT574F43jZ7N9F3"):

    env = {
        "GALAXY_CONFIG_CHECK_MIGRATE_TOOLS" : "False",
        "GALAXY_CONFIG_MASTER_API_KEY" : key
    }

    mounts = {}
    privledged = False

    if tool_data is not None:
        mounts[os.path.abspath(tool_data)] = "/tool_data"
        env['GALAXY_CONFIG_TOOL_DATA_PATH'] = "/tool_data"

    config_dir = os.path.abspath(os.path.join(work_dir, "warpdrive_%s" % (name)))
    if not os.path.exists(config_dir):
        os.mkdir(config_dir)

    if tool_dir is not None:
        mounts[os.path.abspath(tool_dir)] = "/tools_import"
        mounts[config_dir] = "/config"
        with open( os.path.join(config_dir, "import_tool_conf.xml"), "w" ) as handle:
            handle.write(TOOL_IMPORT_CONF)
        env['GALAXY_CONFIG_TOOL_CONFIG_FILE'] = "/config/import_tool_conf.xml,config/tool_conf.xml.main"

    data_load = []
    meta_data = {}
    lib_mapping = {}
    for i, ld in enumerate(lib_data):
        env['GALAXY_CONFIG_ALLOW_LIBRARY_PATH_PASTE'] = "True"
        lpath = os.path.abspath(ld)
        dpath = "/export/lib_data_%s" % (i)
        mounts[lpath] = dpath
        lib_mapping[lpath] = dpath
        if auto_add:
            for a in glob(os.path.join(lpath, "*")):
                if metadata_suffix is None or not a.endswith(metadata_suffix):
                    if os.path.isfile(a):
                        data_load.append( a )
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

    if tool_docker:
        mounts[config_dir] = "/config"
        with open( os.path.join(config_dir, "job_conf.xml"), "w" ) as handle:
            handle.write(string.Template(JOB_CHILD_CONF).substitute(TAG=docker_tag, NAME=name))
        env["GALAXY_CONFIG_JOB_CONFIG_FILE"] = "/config/job_conf.xml"
        env['GALAXY_CONFIG_OUTPUTS_TO_WORKING_DIRECTORY'] = "True"
        privledged=True

    call_docker_run(
        docker_tag,
        ports={str(port) : "80"},
        host=host,
        name=name,
        mounts=mounts,
        privledged=privledged,
        env=env
    )

    host="localhost"
    if 'DOCKER_HOST' in os.environ:
        u = urlparse.urlparse(os.environ['DOCKER_HOST'])
        host = u.netloc.split(":")[0]

    while True:
        time.sleep(3)
        try:
            url = "http://%s:%s/api/tools?key=%s" % (host, port, key)
            logging.debug("Pinging: %s" % (url))
            res = requests.get(url, timeout=3)
            if res.status_code / 100 == 5:
                continue
            break
        except requests.exceptions.ConnectionError:
            pass
        except requests.exceptions.Timeout:
            pass

    rg = RemoteGalaxy("http://%s:%s"  % (host, port), 'admin', path_mapping=lib_mapping)
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
            'docker_tag' : docker_tag,
            'port' : port,
            'lib_data' : list(os.path.abspath(a) for a in lib_data),
            'host' : host,
            'tool_dir' : os.path.abspath(tool_dir) if tool_dir is not None else None,
            'tool_data' : os.path.abspath(tool_data) if tool_dir is not None else None,
            'metadata_suffix' : metadata_suffix,
            'tool_docker' : tool_docker,
            'key' : key,
            'lib_mapping' : lib_mapping
        }))
    return rg

class RemoteGalaxy(object):

    def __init__(self, url, api_key, path_mapping={}):
        self.url = url
        self.api_key = api_key
        self.path_mapping = path_mapping

    def get(self, path):
        c_url = self.url + path
        params = {}
        params['key'] = self.api_key
        req = requests.get(c_url, params=params)
        return req.json()

    def post(self, path, payload):
        c_url = self.url + path
        params = {}
        params['key'] = self.api_key
        logging.debug("POSTING: %s %s" % (c_url, json.dumps(payload)))
        req = requests.post(c_url, data=json.dumps(payload), params=params, headers = {'Content-Type': 'application/json'} )
        return req.json()

    def post_text(self, path, payload, params=None):
        c_url = self.url + path
        if params is None:
            params = {}
        params['key'] = self.api_key
        logging.debug("POSTING: %s %s" % (c_url, json.dumps(payload)))
        req = requests.post(c_url, data=json.dumps(payload), params=params, headers = {'Content-Type': 'application/json'} )
        return req.text

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
    
    def add_workflow(self, wf):
        self.post("/api/workflows/upload", { 'workflow' : wf } )

    def library_paste_file(self, library_id, library_folder_id, name, datapath, uuid=None, metadata=None):
        datapath = os.path.abspath(datapath)
        for ppath, dpath in self.path_mapping.items():
            if datapath.startswith(ppath):
                datapath = os.path.join(dpath, os.path.relpath(datapath, ppath))
                break
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
        return libset



def run_down(name, host=None, rm=False, work_dir="/tmp"):
    config_dir = os.path.join(work_dir, "warpdrive_%s" % (name))
    if not os.path.exists(config_dir):
        print "Config not found"
        return
    try:
        call_docker_kill(
            name, host=host
            )
    except subprocess.CalledProcessError:
        return
    if rm:
        call_docker_rm(
            name, host=host
        )
        shutil.rmtree(config_dir)


def run_status(name="galaxy", host=None):
    txt = call_docker_ps(
        host=host
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


def run_add(name="galaxy", work_dir="/tmp", files=[]):
    config_dir = os.path.join(work_dir, "warpdrive_%s" % (name))
    if not os.path.exists(config_dir):
        print "Config not found"
        return
    
    with open(os.path.join(config_dir, "config.json")) as handle:
        txt = handle.read()
        config = json.loads(txt)

    data_load = []
    for f in files:
        fpath = os.path.abspath(f)
        if os.path.isfile(fpath):
            mapping = None
            for ppath, dpath in config['lib_mapping'].items():
                if mapping is None and fpath.startswith(ppath):
                    mapping = os.path.join(dpath, os.path.relpath(fpath, ppath))
            if mapping is None:
                raise RequestException("File %s not in mounted lib directories" % (fpath))
            data_load.append(mapping)

    rg = RemoteGalaxy("http://%s:%s" % (config['host'], config['port']), 'admin')
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



TOOL_IMPORT_CONF = """<?xml version='1.0' encoding='utf-8'?>
<toolbox>
  <section id="imported" name="Imported Tools">
    <tool_dir dir="/tools_import"/>
  </section>
</toolbox>
"""

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
    <destinations default="cluster">
        <destination id="cluster" runner="slurm">
            <param id="docker_enabled">true</param>
            <param id="docker_sudo">false</param>
            <param id="docker_net">bridge</param>
            <param id="docker_default_container_id">${TAG}</param>
            <param id="docker_volumes"></param>
            <param id="docker_volumes_from">${NAME}</param>
        </destination>
    </destinations>
</job_conf>
"""



if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-v", action="store_true", default=False)
    parser.add_argument("-vv", action="store_true", default=False)

    subparsers = parser.add_subparsers(title="subcommand")

    parser_up = subparsers.add_parser('up')
    parser_up.add_argument("-t", "--tag", dest="docker_tag", default="bgruening/galaxy-stable")
    parser_up.add_argument("-x", "--tool-dir", default=None)
    parser_up.add_argument("-d", "--tool-data", default=None)
    parser_up.add_argument("-w", "--work-dir", default="/tmp")
    parser_up.add_argument("-p", "--port", default="8080")
    parser_up.add_argument("-m", "--metadata", dest="metadata_suffix", default=None)
    parser_up.add_argument("-n", "--name", default="galaxy")
    parser_up.add_argument("-l", "--lib-data", action="append", default=[])
    parser_up.add_argument("-a", "--auto-add", action="store_true", default=False)
    parser_up.add_argument("-c", "--child", dest="tool_docker", action="store_true", help="Launch jobs in child containers", default=False)
    parser_up.add_argument("--key", default="HSNiugRFvgT574F43jZ7N9F3")
    parser_up.add_argument("--host", default=None)
    parser_up.set_defaults(func=run_up)

    parser_down = subparsers.add_parser('down')
    parser_down.add_argument("-n", "--name", default="galaxy")
    parser_down.add_argument("--rm", action="store_true", default=False)
    parser_down.add_argument("--host", default=None)
    parser_down.add_argument("-w", "--work-dir", default="/tmp")
    parser_down.set_defaults(func=run_down)

    parser_status = subparsers.add_parser('status')
    parser_status.add_argument("-n", "--name", default="galaxy")
    parser_status.add_argument("--host", default=None)
    parser_status.set_defaults(func=run_status)

    parser_add = subparsers.add_parser('add')
    parser_add.add_argument("-n", "--name", default="galaxy")
    parser_add.add_argument("-w", "--work-dir", default="/tmp")
    parser_add.add_argument("files", nargs="+")
    parser_add.set_defaults(func=run_add)

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

    try:
        func(**kwds)
        sys.exit(0)
    except RequestException, e:
        sys.stderr.write("%s\n" % (e.message))
        sys.exit(1)
