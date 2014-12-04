#!/usr/bin/env python


import os
import re
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
    with open(args.workflow) as handle:
        wdesc = yaml_to_workflow(handle.read())
    print json.dumps(wdesc, indent=4)
    workflow = Workflow(wdesc)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-w", "--workflow", help="Galaxy Workflow File", required=True)
    parser.add_argument("-t", "--tools", help="Tool Directory", required=True)
    parser.add_argument("inputs", nargs="+", default=[])

    args = parser.parse_args()
    test_workflow(args)
