
import nebula.docstore
from nebula.service import Service, ServiceConfig


class MD5Service(Service):
    def __init__(self, docstore, **kwds):
        super(MD5Service, self).__init__('md5')
        self.docstore = docstore

    @staticmethod
    def from_dict(data):
        meta = dict(data)
        doc_store = nebula.docstore.from_url(data['docstore_url'], **data.get('docstore_config', {}))
        return MD5Service( doc_store, **data['config'] )
