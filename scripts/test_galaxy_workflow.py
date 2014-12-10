#!/usr/bin/env python


import os
import re
import sys
import json
import time
import logging
from urlparse import urlparse
from glob import glob
from argparse import ArgumentParser

from nebula.galaxy import Workflow
from nebula.galaxy import ToolBox
from nebula.galaxy import Tool
from nebula.galaxy.yaml_to_workflow import yaml_to_workflow

logging.basicConfig(level=logging.DEBUG)

def test_workflow(args):
    if args.yaml:
        with open(args.workflow) as handle:
            wdesc = yaml_to_workflow(handle.read())
    else:
        with open(args.workflow) as handle:
            wdesc = json.loads(handle.read())
    if args.verbose:
        print json.dumps(wdesc, indent=4)
    workflow = Workflow(wdesc)

    toolbox = ToolBox()
    toolbox.scan_dir(args.tools)

    for req_file in args.inputs:
        with open(req_file) as handle:
            req = json.loads(handle.read())
        req = workflow.adjust_input(req,  label_translate=True, ds_translate=False)
        print "Checking", req
        workflow.validate_input(req, toolbox)


if __name__ == "__main__":
    parser = ArgumentParser()    
    subparsers = parser.add_subparsers(title="subcommand")

    parser_test = subparsers.add_parser('test')

    parser_test.add_argument("-w", "--workflow", help="Galaxy Workflow File", required=True)
    parser_test.add_argument("-t", "--tools", help="Tool Directory", required=True)
    parser_test.add_argument("-y", "--yaml", action="store_true", default=False)
    parser_test.add_argument("-v", "--verbose", action="store_true", default=False)
    parser_test.add_argument("inputs", nargs="+", default=[])
    parser_test.set_defaults(func=test_workflow)

    args = parser.parse_args()
    sys.exit(args.func(args))

