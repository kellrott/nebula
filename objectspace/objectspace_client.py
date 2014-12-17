#!/usr/bin/env python

import sys

from argparse import ArgumentParser
from nebula.dag import Target, TargetFile
from nebula.objectstore import init_objectstore_url
from nebula.docstore import init_docstore_url


def run_size(args):
    store = GNOSStore(None, file_path=args.cache)
    print store.size(Target(args.obj))

def run_cache(args):
    store = GNOSStore(None, file_path=args.cache, docker_config={'image' : 'gtdownload'})
    print store.get_filename(Target(args.obj))

def run_ls(args):
    doc = init_docstore_url(args.doc_server)
    for a in doc.filter():
        print "%s\t%s" % (a['model_class'], a['_id'])

def run_put(args):
    obj_store = init_objectstore_url(args.obj_server)
    doc_store = init_docstore_url(args.doc_server)
    obj_store.create(TargetFile(args.file))
    path = obj_store.get_filename(hda)
    shutil.copy(args.file, path)
    obj_store.update_from_file(hda)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-d", "--doc-server", default="objectspace://localhost:9000")
    parser.add_argument("-o", "--obj-server", default=None)
    parser.add_argument("--cache", default="/tmp/nebula-cache")

    subparsers = parser.add_subparsers(title="subcommand")

    parser_size = subparsers.add_parser('size')
    parser_size.add_argument("obj")
    parser_size.set_defaults(func=run_size)

    parser_cache = subparsers.add_parser('cache')
    parser_cache.add_argument("obj")
    parser_cache.set_defaults(func=run_cache)

    parser_ls = subparsers.add_parser('ls')
    parser_ls.set_defaults(func=run_ls)

    parser_put = subparsers.add_parser('put')
    parser_put.add_argument("file")
    parser_put.add_argument("metadata")
    parser_put.set_defaults(func=run_put)


    args = parser.parse_args()

    if args.obj_server is None:
        args.obj_server = args.doc_server

    sys.exit(args.func(args))
