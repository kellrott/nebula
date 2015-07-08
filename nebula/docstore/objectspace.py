
import json
from urllib2 import urlopen, Request
from urlparse import urljoin
from nebula.docstore import DocStore
import requests

class ObjectSpace(DocStore):
    """
    Cheap and simple file based doc store, not recommended for large systems
    """

    def __init__(self, server_url, **kwds):
        super(ObjectSpace, self).__init__(None)
        self.server_url = server_url

    def get(self, id):
        return None

    def put(self, id, doc):
        req = Request(urljoin(self.server_url, "/api/docs"), json.dumps(doc))
        response = urlopen(req)
        return response.read()

    def filter(self, **kwds):
        params = "&".join( "%s=%s" % (k,v) for k,v in kwds.items() )
        req_url = urljoin(self.server_url, "/api/docs?%s" % (params))
        results = urlopen( req_url )
        for line in results:
            for k, v in json.loads(line).items():
                yield k, v

    def exists(self, obj, **kwds):
        req_url = urljoin(self.server_url, "/api/files/%s" % (obj.id))
        results = urlopen( req_url )
        meta = json.loads(results.read())
        if 'error' in meta or 'id' not in meta:
            return False
        return meta['id'] == obj.id

    def file_ready(self, obj, **kwds):
        raise Exception("Not Implemented")

    def create(self, obj, **kwds):
        raise Exception("Not Implemented")

    def empty(self, obj, **kwds):
        raise Exception("Not Implemented")

    def size(self, obj, **kwds):
        raise Exception("Not Implemented")

    def delete(self, obj, **kwds):
        raise Exception("Not Implemented")

    def get_data(self, obj):
        raise Exception("Not Implemented")

    def get_filename(self, obj, **kwds):
        raise Exception("Not Implemented")

    def update_from_file(self, obj, file_name=None, create=False, **kwds):
        with open(file_name) as handle:
            req_url = urljoin(self.server_url, "/api/files/%s" % (obj.id))
            rdst = requests.post(req_url, files={'file': handle})

    def get_object_url(self, obj, **kwds):
        raise Exception("Not Implemented")

    def get_store_usage_percent(self):
        raise Exception("Not Implemented")

    def local_cache_base(self):
        raise Exception("Not Implemented")

    def get_url(self):
        raise Exception("Not Implemented")
