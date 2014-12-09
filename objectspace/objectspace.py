#!/usr/bin/env python

import os
import json
import uuid
import argparse
import tornado.ioloop
import tornado.web
import pymongo

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("test")


class DBBase(tornado.web.RequestHandler):
    def initialize(self, server, port, database, fsbase):
        self.conn = pymongo.MongoClient( server, port )
        self.db = self.conn[database]
        self.collection = self.db['objects']
        self.fsbase = os.path.abspath(fsbase)
    
    def report_error(self, message):
        self.write(json.dumps({"error" : message}))

    def check_id(self, id):
        try:
            uid = str(uuid.UUID(id))
        except ValueError:
            self.report_error("ID not UUID")
            return False
        return True
    
    def id2path(self, id):
        dirname = os.path.join(self.fsbase, id[0], id[:2])
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        path = os.path.join( dirname, id )
        return "file://" + path
        

class ListHandler(DBBase):
    def get(self):
        for doc in self.collection.find():
            self.write(json.dumps(doc))


class DocHandler(DBBase):
    
    def get(self, id):
        if not self.check_id(id):
            return
        self.write(json.dumps(self.collection.find_one(id)))

    def post(self, id):
        if not self.check_id(id):
            return
        try:
            meta = json.loads(self.request.body)
        except ValueError:
            self.report_error("Bad input")
            return
        if 'model_class' not in meta:
            self.report_error("Missing Model class")
            return
        meta["_id"] = id
        out = self.collection.insert(meta)
        self.write(out)

    def put(self, id):
        self.post(id)


class PathHandler(DBBase):
    def get(self, id):
        if not self.check_id(id):
            return
        self.write(json.dumps([self.id2path(id)]))


def run_main(args):
    
    config = dict(
        server=args.mongo.split(":")[0],
        port=int(args.mongo.split(":")[1]), database=args.db,
        fsbase=args.fsbase
    )
    
    application = tornado.web.Application([
        (r"/", MainHandler),
        (r"/api/objects", ListHandler, config),
        (r"/api/objects/(.*)", DocHandler, config),
        (r"/api/locs/(.*)", PathHandler, config),

    ])

    application.listen(args.port)
    tornado.ioloop.IOLoop.instance().start()



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mongo", default="localhost:27017")
    parser.add_argument("--fsbase", default="files")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--db", default="objectspace")
    
    args = parser.parse_args()
    args.fsbase = os.path.abspath(args.fsbase)
    run_main(args)
