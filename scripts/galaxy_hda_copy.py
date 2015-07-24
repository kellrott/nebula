#!/usr/bin/env python

import sys
import re
import os
import argparse
import nebula.warpdrive
from nebula.docstore import from_url
from nebula.target import Target


def action_ls(args):
    rg = nebula.warpdrive.RemoteGalaxy(args.url, args.api_key)
    for h in rg.history_list():
        print "%s\t%s" % (h['id'], h['name'])

def action_cp(args):
    rg = nebula.warpdrive.RemoteGalaxy(args.url, args.api_key)
    
    if not args.dir:
        docstore = from_url(args.dst)
    else:
        if not os.path.exists(args.dst):
            os.mkdir(args.dst)

    for hda in rg.get_history_contents(args.src):
        if hda['visible']:
            if args.filter is None or re.search(args.filter, hda['name']):
                if hda['name'] not in args.exclude:
                    print hda['name']
                    meta = rg.get_dataset(hda['id'], 'hda')
                    if args.dir:
                        dst_path = os.path.join(args.dst, hda['name'])
                        rg.download(meta['download_url'], dst_path)
                    else:
                        meta['id'] = meta['uuid'] #use the glocal id
                        hda = Target(uuid=meta['uuid'])
                        docstore.create(hda)
                        path = docstore.get_filename(hda)
                        rg.download(meta['download_url'], path)
                        docstore.update_from_file(hda)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title="subcommand")

    parser_ls = subparsers.add_parser('ls')
    parser_ls.add_argument("-u", "--url", default="http://localhost:8080")
    parser_ls.add_argument("-k", "--api-key", default="admin")
    parser_ls.set_defaults(func=action_ls)

    parser_cp = subparsers.add_parser('cp')
    parser_cp.add_argument("-u", "--url", default="http://localhost:8080")
    parser_cp.add_argument("-k", "--api-key", default="admin")
    parser_cp.add_argument("-f", "--filter", default=None)
    parser_cp.add_argument("-e", "--exclude", action="append", default=[])
    parser_cp.add_argument("-d", "--dir", action="store_true", default=False)
    
    
    parser_cp.add_argument("src")
    parser_cp.add_argument("dst")
    
    parser_cp.set_defaults(func=action_cp)

    args = parser.parse_args()
    func = args.func
    e = func(args)
    sys.exit(e)
