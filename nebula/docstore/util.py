
import os
import re
import json
from urlparse import urlparse
from glob import glob
import logging
from nebula import Target

def scan_doc_dir(path):
    data_map = {}
    for meta_path in glob(os.path.join(path, "*.json")):
        data_path = re.sub(r'.json$', "", meta_path)
        if os.path.exists(data_path):
            try:
                with open(meta_path) as handle:
                    meta = json.loads(handle.read())
                    if 'uuid' in meta:
                        data_map[meta['uuid']] = data_path
            except:
                pass
    return data_map


def sync_doc_dir(path, docstore, uuid_set=None, filter=None):
    data_map = scan_doc_dir(path)
    #print "Scanned", path, data_map
    for uuid, path in data_map.items():
        t = Target(uuid)
        if not docstore.exists(t):
            copy = True
            if uuid_set is not None and uuid not in uuid_set:
                copy = False
            if filter is not None:
                with open(path + ".json") as handle:
                    meta = json.loads(handle.read())
                if not filter(meta):
                    copy = False
            if copy:
                logging.info("Adding file: %s" % (path))
                docstore.update_from_file(t, path, create=True)
                with open(path + ".json") as handle:
                    meta = json.loads(handle.read())
                docstore.put(t.id, meta)

"""
        #move the output data into the datastore
        for task_name, i in task_job_ids.items():
            job = service.get_job(i)
            if job.error is None:
                for a in job.get_outputs():
                    meta = service.get_meta(a)
                    #if 'tags' in task_request[task_name]:
                    #    meta["tags"] = task_request[task_name]["tags"]
                    #print "meta!!!", json.dumps(meta, indent=4)
                    doc.put(meta['uuid'], meta)
                    if meta.get('visible', True):
                        if meta['state'] == "ok":
                            if meta['uuid'] not in input_uuids:
                                logging.info("Downloading: %s" % (meta['uuid']))
                                service.store_data(a, doc)
                            else:
                                logging.info("Skipping input file %s" % (a))
                        else:
                            logging.info("Skipping non-ok file: %s" % (meta['state']))
                    else:
                        logging.info("Skipping Download %s (not visible)" % (a))

"""
