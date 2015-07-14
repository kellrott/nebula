
import nebula.docstore
from nebula.service import Service, ServiceConfig


class MD5Service(Service):
    def __init__(self, docstore, **kwds):
        super(MD5Service, self).__init__('md5')
        self.docstore = docstore

    def to_dict(self):
        return {
            'service_type' : 'MD5',
            'config' : {},
            'docstore_url' : self.docstore.get_url()
        }

    @staticmethod
    def from_dict(data):
        meta = dict(data)
        doc_store = nebula.docstore.from_url(data['docstore_url'], **data.get('docstore_config', {}))
        return MD5Service( doc_store, **data['config'] )

    def runService(self):
        print "I should do something here"
