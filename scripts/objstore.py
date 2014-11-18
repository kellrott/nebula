#!/usr/bin/env python

import sys

from argparse import ArgumentParser
from nebula.dag import Target
from nebula.objectstore.gnos import GNOSStore
from nebula.docstore import FileDocStore

def run_size(args):    
    store = GNOSStore(None, file_path=args.cache)    
    print store.size(Target(args.obj))


def run_cache(args):
    store = GNOSStore(None, file_path=args.cache, docker_config={'image' : 'gtdownload'})
    print store.get_filename(Target(args.obj))
    
def run_ls(args):
    doc = FileDocStore(file_path=args.doc)
    for a in doc.filter():
        print "%s\t%s" % (a['model_class'], a['uuid'])

if __name__ == "__main__":
    parser = ArgumentParser()    
    subparsers = parser.add_subparsers(title="subcommand")
    
    parser_size = subparsers.add_parser('size')
    parser_size.add_argument("obj")    
    parser_size.add_argument("--cache", default="/tmp/gnos-cache")
    parser_size.set_defaults(func=run_size)

    parser_cache = subparsers.add_parser('cache')
    parser_cache.add_argument("obj")    
    parser_cache.add_argument("--cache", default="/tmp/gnos-cache")
    parser_cache.set_defaults(func=run_cache)
    
    parser_ls = subparsers.add_parser('ls')
    parser_ls.add_argument("--doc", default="./nebula_docs")
    parser_ls.set_defaults(func=run_ls)

    args = parser.parse_args()
    sys.exit(args.func(args))

    