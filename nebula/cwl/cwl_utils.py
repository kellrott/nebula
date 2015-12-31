
import os
from glob import glob
import yaml

def run_build(tool_dir, tool=None, host=None, no_cache=False, image_dir=None, sudo=False):
    for path in glob(os.path.join(tool_dir, "*", "*.yaml")):
        print "Parsing:", path
        with open(path) as handle:
            data = yaml.load(handle.read())
            
        for r in data.get('requirements', []):
            if r.get('class', None) == 'DockerRequirement':
                print r.get('dockerImageId')

def add_cwl_build_command(subparsers):
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
