#!/usr/bin/env python

import json
import argparse
from nebula.dag import Target, TargetFile
from nebula.docstore import FileDocStore

def run_query(docstore, fields, size):
    doc = FileDocStore(file_path=docstore)

    for id, entry in doc.filter():

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

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--docstore", help="DocStore", required=True)
    subparsers = parser.add_subparsers(title="subcommand")

    parser_query = subparsers.add_parser('query')
    parser_query.set_defaults(func=run_query)
    parser_query.add_argument("--size", action="store_true", default=False)
    parser_query.add_argument("fields", nargs="*", default=None)

    parser_get = subparsers.add_parser('get')
    parser_get.set_defaults(func=run_get)
    parser_get.add_argument("uuid")
    parser_get.add_argument("outpath")


    args = parser.parse_args()
    func = args.func

    vargs = vars(args)
    del vargs['func']

    func(**vargs)
