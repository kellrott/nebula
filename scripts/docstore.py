#!/usr/bin/env python

import os
import re
import json
import argparse
import subprocess
import shutil
from nebula.target import Target, TargetFile
from nebula.docstore import FileDocStore

def run_copy(docstore, out_docstore):
    doc = FileDocStore(file_path=docstore)

    out_doc = FileDocStore(file_path=out_docstore)

    for id, entry in doc.filter():
        if out_doc.get(id) is None:
            print "copy", id
            out_doc.put(id, entry)
            if doc.exists(entry):
                src_path = doc.get_filename(entry)
                out_doc.create(entry)
                dst_path = out_doc.get_filename(entry)
                shutil.copy(src_path, dst_path)
                out_doc.update_from_file(entry)
        else:
            #print "skip", id, doc.size(entry), out_doc.size(entry)
            if doc.size(entry) != out_doc.size(entry):
                print "mismatch", id


def run_errors(docstore):
    doc = FileDocStore(file_path=docstore)

    for id, entry in doc.filter():
        if entry.get('state', '') == 'error':
            print "Dataset", id, entry.get("tags", "")
            if 'provenance' in entry:
                print "tool:", entry['provenance']['tool_id']
                print "-=-=-=-=-=-=-"
            print entry['job']['stdout']
            print "-------------"
            print entry['job']['stderr']
            print "-=-=-=-=-=-=-"


def run_ls(docstore, size=False):
    doc = FileDocStore(file_path=docstore)

    for id, entry in doc.filter():
        #if doc.size(entry) > 0:
            if size:
                print id, entry.get('name', id), doc.size(entry)
            else:
                print id, entry.get('name', id)

def run_query(docstore, fields, size, filters):
    doc = FileDocStore(file_path=docstore)

    filter = {}
    for k in filters:
        tmp=k.split("=")
        filter[tmp[0]] = tmp[1]

    for id, entry in doc.filter(**filter):

        if fields is None or len(fields) == 0:
            line = entry
        else:
            line = dict( (i, entry.get(i, "")) for i in fields )

        if size:
            size_value = doc.size(Target(uuid=entry['uuid']))
        else:
            size_value = ""

        print size_value, json.dumps(line)

def run_get(docstore, uuid, outpath):
    doc = FileDocStore(file_path=docstore)
    print doc.get_filename(Target(uuid=uuid))


def run_timing(docstore):
    doc = FileDocStore(file_path=docstore)
    for id, entry in doc.filter():
        if 'job' in entry and 'job_metrics' in entry['job']:
            timing = None
            for met in entry['job']['job_metrics']:
                if met['name'] == 'runtime_seconds':
                    timing = met['raw_value']
            if timing is not None:
                print id, entry["name"], timing

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--docstore", help="DocStore", required=True)
    subparsers = parser.add_subparsers(title="subcommand")

    parser_query = subparsers.add_parser('copy')
    parser_query.set_defaults(func=run_copy)
    parser_query.add_argument("out_docstore", help="DocStore")

    parser_query = subparsers.add_parser('errors')
    parser_query.set_defaults(func=run_errors)

    parser_ls = subparsers.add_parser('ls')
    parser_ls.add_argument("-s", "--size", action="store_true", default=False)
    parser_ls.set_defaults(func=run_ls)

    parser_query = subparsers.add_parser('query')
    parser_query.set_defaults(func=run_query)
    parser_query.add_argument("--size", action="store_true", default=False)
    parser_query.add_argument("-f", "--filter", dest="filters", action="append", default=[])
    parser_query.add_argument("fields", nargs="*", default=None)

    parser_timing = subparsers.add_parser('timing')
    parser_timing.set_defaults(func=run_timing)

    args = parser.parse_args()
    func = args.func

    vargs = vars(args)
    del vargs['func']

    func(**vargs)
