
import os
import uuid
import json

class DocStore(object):
    """
    DocStore abstract interface
    """
    def __init__(self, config, **kwargs):
        self.running = True
        self.extra_dirs = {}

    def shutdown(self):
        self.running = False

    def get(self, id):
        raise NotImplementedError()

    def put(self, id, doc):
        raise NotImplementedError()

    def loaddoc(self, data):
        return json.loads(data)

    def dumpdoc(self, doc):
        return json.dumps(doc)

    def cleanid(self, id):
        return str(uuid.UUID(id))


class FileDocStore(DocStore):
    """
    Cheap and simple file based doc store, not recommended for large systems
    """

    def __init__(self, file_path, **kwds):
        super(FileDocStore, self).__init__(kwds)
        self.file_path = file_path
        if not os.path.exists(self.file_path):
            os.mkdir(self.file_path)

    def get(self, id):
        id = self.cleanid(id)
        path = os.path.join(self.file_path, id[:2], id + ".json")
        if not os.path.exists(path):
            return None
        with open(path) as handle:
            data = handle.read()
        return self.loaddoc(data)

    def put(self, id, doc):
        id = self.cleanid(id)
        dir = os.path.join(self.file_path, id[:2])
        if not os.path.exists(dir):
            os.mkdir(dir)
        path = os.path.join(self.file_path, id[:2], id + ".json")
        with open(path, "w") as handle:
            handle.write(self.dumpdoc(doc))