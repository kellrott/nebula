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
    proc = subprocess.Popen(cmd, close_fds=True, env=sys_env, stdout=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    if proc.returncode != 0:
        raise Exception("Call Failed: %s" % (cmd))

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


def run_up(args):
    env = {
        "GALAXY_CONFIG_CHECK_MIGRATE_TOOLS" : "False",
        "GALAXY_CONFIG_MASTER_API_KEY" : args.key
    }

    mounts = {}
    privledged = False

    if args.tool_data is not None:
        mounts[os.path.abspath(args.tool_data)] = "/tool_data"
        env['GALAXY_CONFIG_TOOL_DATA_PATH'] = "/tool_data"

    config_dir = None

    if args.tool_dir is not None:
        mounts[os.path.abspath(args.tool_dir)] = "/tools_import"
        config_dir = os.path.abspath(tempfile.mkdtemp(dir=args.work_dir, prefix="galaxy_warpconfig_"))
        mounts[config_dir] = "/config"
        with open( os.path.join(config_dir, "import_tool_conf.xml"), "w" ) as handle:
            handle.write(TOOL_IMPORT_CONF)
        env['GALAXY_CONFIG_TOOL_CONFIG_FILE'] = "/config/import_tool_conf.xml,config/tool_conf.xml.main"

    if args.child:
        if config_dir is None:
            config_dir = os.path.abspath(tempfile.mkdtemp(dir=args.work_dir, prefix="galaxy_warpconfig_"))
            mounts[config_dir] = "/config"
        with open( os.path.join(config_dir, "job_conf.xml"), "w" ) as handle:
            handle.write(string.Template(JOB_CHILD_CONF).substitute(TAG=args.tag, NAME=args.name))
        env["GALAXY_CONFIG_JOB_CONFIG_FILE"] = "/config/job_conf.xml"
        env['GALAXY_CONFIG_OUTPUTS_TO_WORKING_DIRECTORY'] = "True"
        privledged=True

    call_docker_run(
        args.tag,
        ports={args.port : "80"},
        host=args.host,
        name=args.name,
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
            requests.get("http://%s:%s/api/tools?key=%s" % (host, args.port, args.key), timeout=3)
            return 0
        except requests.exceptions.ConnectionError:
            pass
        except requests.exceptions.Timeout:
            pass


def run_down(args):
    call_docker_kill(
        args.name, host=args.host
    )

    if args.rm:
        call_docker_rm(
            args.name, host=args.host
        )


def run_status(args):
    txt = call_docker_ps(
        host=args.host
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
            name = line[namesIndex:sizeIndex].split()[0]
            tmp = line[statusIndex:portsIndex].split()
            status = "NotFound"
            if len(tmp):
                status = tmp[0]
            if name == args.name:
                print status
                found = True
    if not found:
        print "NotFound"


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

    subparsers = parser.add_subparsers(title="subcommand")

    parser_up = subparsers.add_parser('up')
    parser_up.add_argument("-t", "--tag", default="bgruening/galaxy-stable")
    parser_up.add_argument("-x", "--tool-dir", default=None)
    parser_up.add_argument("-d", "--tool-data", default=None)
    parser_up.add_argument("-w", "--work-dir", default="/tmp")
    parser_up.add_argument("-p", "--port", default="8080")
    parser_up.add_argument("-n", "--name", default="galaxy")
    parser_up.add_argument("-c", "--child", action="store_true", help="Launch jobs in child containers", default=False)
    parser_up.add_argument("--key", default="HSNiugRFvgT574F43jZ7N9F3")
    parser_up.add_argument("--host", default=None)
    parser_up.set_defaults(func=run_up)

    parser_down = subparsers.add_parser('down')
    parser_down.add_argument("-n", "--name", default="galaxy")
    parser_down.add_argument("--rm", action="store_true", default=False)
    parser_down.add_argument("--host", default=None)
    parser_down.set_defaults(func=run_down)

    parser_status = subparsers.add_parser('status')
    parser_status.add_argument("-n", "--name", default="galaxy")
    parser_status.add_argument("--host", default=None)
    parser_status.set_defaults(func=run_status)

    args = parser.parse_args()

    if args.v:
        logging.basicConfig(level=logging.INFO)
    sys.exit(args.func(args))
