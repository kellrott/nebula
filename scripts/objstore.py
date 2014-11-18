#!/usr/bin/env python

import sys

from argparse import ArgumentParser
from nebula.dag import Target
from nebula.objectstore.gnos import GNOSStore

def run_size(args):    
    store = GNOSStore(None, file_path=args.cache)    
    print store.size(Target(args.obj))


def run_cache(args):
    store = GNOSStore(None, file_path=args.cache, docker_config={'image' : 'gtdownload'})
    print store.get_filename(Target(args.obj))
    

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


    args = parser.parse_args()
    sys.exit(args.func(args))

    